#!/usr/bin/env python3
"""
dht_ipc_writer.py — the single WRITER process for the DHT collector (Phase 2).

Collector processes (multi-core, unchanged) send peer-row batches over a Unix
domain socket; THIS process owns the only write connection to hashes_v2.db,
batches upserts, and runs TRUNCATE checkpoints. Keeps multi-core collection
(separate collector processes) while concentrating writes into one connection
so WAL truncation always reclaims and there is zero write contention / loss.

Run:  python dht_ipc_writer.py --serve --db /data/db/hashes_v2.db --sock /data/run/dht_writer.sock
"""
from __future__ import annotations
import os, sys, socket, struct, pickle, queue, threading, time, sqlite3, argparse, signal

_HDR = struct.Struct(">I")
UPSERT = ("INSERT INTO peers (hash, ip, country, first_seen, last_seen) VALUES (?,?,?,?,?) "
          "ON CONFLICT(hash, ip) DO UPDATE SET country=excluded.country, last_seen=excluded.last_seen")


def _recv_exact(conn, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return bytes(buf)


def _recv_msg(conn):
    hdr = _recv_exact(conn, 4)
    if hdr is None:
        return None
    (ln,) = _HDR.unpack(hdr)
    payload = _recv_exact(conn, ln)
    if payload is None:
        return None
    return pickle.loads(payload)


# ----------------------------- writer (server) -----------------------------
class WriterServer:
    def __init__(self, db_path, sock_path, batch=5000, flush_sec=2.0, ckpt_every=25, qmax=200000):
        self.db_path, self.sock_path = db_path, sock_path
        self.batch, self.flush_sec, self.ckpt_every = batch, flush_sec, ckpt_every
        self.q: queue.Queue = queue.Queue(maxsize=qmax)
        self.stats = dict(rows=0, batches=0, checkpoints=0, lock_errors=0,
                          clients=0, active_clients=0, max_wal_mb=0.0)
        self._stop = threading.Event()

    def _ts(self):
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    def _stats_loop(self):
        """Periodic observability so a live cutover is VISIBLE in dht_writer.log."""
        last_rows = 0
        while not self._stop.wait(30):
            try:
                wal = os.path.getsize(self.db_path + "-wal") / 1048576
            except FileNotFoundError:
                wal = 0.0
            d = self.stats["rows"] - last_rows
            last_rows = self.stats["rows"]
            print("%s STATS active_clients=%d total_clients=%d rows=%d (+%d/30s) "
                  "batches=%d checkpoints=%d lock_errors=%d qdepth=%d wal=%.1fMB"
                  % (self._ts(), self.stats["active_clients"], self.stats["clients"],
                     self.stats["rows"], d, self.stats["batches"], self.stats["checkpoints"],
                     self.stats["lock_errors"], self.q.qsize(), wal), flush=True)

    def _db_loop(self):
        conn = sqlite3.connect(self.db_path, timeout=60)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=60000")
        conn.execute("PRAGMA synchronous=NORMAL")
        buf, last = [], time.time()

        def flush():
            if not buf:
                return
            try:
                conn.execute("BEGIN"); conn.executemany(UPSERT, buf); conn.commit()
                self.stats["rows"] += len(buf); self.stats["batches"] += 1
            except sqlite3.OperationalError as e:
                if "lock" in str(e).lower():
                    self.stats["lock_errors"] += 1
                    try: conn.rollback()
                    except Exception: pass
                else:
                    raise
            buf.clear()
            if self.stats["batches"] and self.stats["batches"] % self.ckpt_every == 0:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                self.stats["checkpoints"] += 1
            try:
                self.stats["max_wal_mb"] = max(self.stats["max_wal_mb"],
                                               os.path.getsize(self.db_path + "-wal") / 1048576)
            except FileNotFoundError:
                pass

        while not (self._stop.is_set() and self.q.empty()):
            try:
                rows = self.q.get(timeout=self.flush_sec)
            except queue.Empty:
                flush(); last = time.time(); continue
            buf.extend(rows)
            if len(buf) >= self.batch or (time.time() - last) >= self.flush_sec:
                flush(); last = time.time()
        flush()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)"); self.stats["checkpoints"] += 1
        conn.close()

    def _client(self, conn):
        self.stats["clients"] += 1
        self.stats["active_clients"] += 1
        print("%s client connected (active=%d total=%d)"
              % (self._ts(), self.stats["active_clients"], self.stats["clients"]), flush=True)
        try:
            while True:
                rows = _recv_msg(conn)
                if rows is None:
                    break
                self.q.put(rows)            # bounded -> backpressure to collector
        except Exception as e:
            print("%s client error: %r" % (self._ts(), e), flush=True)
        finally:
            self.stats["active_clients"] -= 1
            print("%s client disconnected (active=%d)"
                  % (self._ts(), self.stats["active_clients"]), flush=True)
            try: conn.close()
            except Exception: pass

    def serve(self):
        d = os.path.dirname(self.sock_path)
        if d and not os.path.isdir(d):
            os.makedirs(d, exist_ok=True)
        if os.path.exists(self.sock_path):
            os.remove(self.sock_path)
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        srv.bind(self.sock_path); srv.listen(128)
        try: os.chmod(self.sock_path, 0o660)
        except OSError: pass
        dbt = threading.Thread(target=self._db_loop, name="db-writer", daemon=True)
        dbt.start()
        threading.Thread(target=self._stats_loop, name="stats", daemon=True).start()

        def _sig(*_a):
            self._stop.set()
            try: srv.close()
            except Exception: pass
        signal.signal(signal.SIGTERM, _sig)
        signal.signal(signal.SIGINT, _sig)
        try:
            while not self._stop.is_set():
                try:
                    c, _ = srv.accept()
                except OSError:
                    break
                threading.Thread(target=self._client, args=(c,), daemon=True).start()
        finally:
            self._stop.set()
            dbt.join(60)
            try: os.remove(self.sock_path)
            except FileNotFoundError: pass


# ----------------------------- client (collector) -----------------------------
class IPCClient:
    """Persistent per-process connection to the writer. Thread-safe (executor threads)."""
    def __init__(self, sock_path):
        self.sock_path = sock_path
        self._s = None
        self._lock = threading.Lock()

    def _connect(self):
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect(self.sock_path)
        return s

    def send(self, rows):
        if not rows:
            return
        data = pickle.dumps(rows, protocol=pickle.HIGHEST_PROTOCOL)
        frame = _HDR.pack(len(data)) + data
        with self._lock:
            if self._s is None:
                self._s = self._connect()
            try:
                self._s.sendall(frame)
            except OSError:
                try: self._s.close()
                except Exception: pass
                self._s = self._connect()      # one reconnect attempt
                self._s.sendall(frame)

    def close(self):
        with self._lock:
            if self._s is not None:
                try: self._s.close()
                except Exception: pass
                self._s = None


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--serve", action="store_true")
    ap.add_argument("--db", required=True)
    ap.add_argument("--sock", required=True)
    ap.add_argument("--batch", type=int, default=5000)
    a = ap.parse_args()
    if a.serve:
        print(f"dht_ipc_writer serving: db={a.db} sock={a.sock}", flush=True)
        WriterServer(a.db, a.sock, batch=a.batch).serve()
