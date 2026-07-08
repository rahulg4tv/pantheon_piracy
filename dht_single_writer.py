"""
dht_single_writer.py — optional single-writer queue for dht_peer_count.py.

Flag-gated by env DHT_SINGLE_WRITER=1 (OFF by default → zero behavior change).
When ON, collectors enqueue peer rows instead of writing through per-thread
connections; ONE writer thread batches them through a single connection.

Why: replaces N executor-thread DB connections per process with exactly ONE
writer connection — eliminating intra-process write contention and dropped-row
loss, and (once the 8 units are consolidated to 1 process) letting
wal_checkpoint(TRUNCATE) always reclaim because no other connection pins a
snapshot.

Upsert semantics are preserved exactly:
  * peers present  -> one row per (hash, ip, country); update country+last_seen
  * peers empty    -> sentinel row ip='_queried_', country='XX' (marks hash done)
"""
from __future__ import annotations
import os, queue, threading, time, sqlite3, atexit

ENABLED     = os.environ.get("DHT_SINGLE_WRITER", "0") == "1"
# Phase 2: if DHT_WRITER_SOCK is set, route writes to the external single-writer
# PROCESS over that Unix socket (keeps collectors multi-core). If unset, use the
# in-process PeerWriter thread (Phase 1).
WRITER_SOCK = os.environ.get("DHT_WRITER_SOCK", "")
BATCH       = int(os.environ.get("DHT_WRITER_BATCH", "4000"))
FLUSH_SEC   = float(os.environ.get("DHT_WRITER_FLUSH_SEC", "2"))
CKPT_EVERY  = int(os.environ.get("DHT_WRITER_CKPT_EVERY", "25"))   # batches between TRUNCATE
QMAX        = int(os.environ.get("DHT_WRITER_QMAX", "100000"))

_UPSERT = ("INSERT INTO peers (hash, ip, country, first_seen, last_seen) "
           "VALUES (?, ?, ?, ?, ?) "
           "ON CONFLICT(hash, ip) DO UPDATE SET "
           "country = excluded.country, last_seen = excluded.last_seen")

_STOP = object()


class PeerWriter(threading.Thread):
    def __init__(self, db_path: str):
        super().__init__(name="dht-peer-writer", daemon=True)
        self.db_path = db_path
        self.q: queue.Queue = queue.Queue(maxsize=QMAX)
        self.stats = dict(rows=0, batches=0, checkpoints=0, lock_errors=0,
                          dropped=0, max_qdepth=0, max_wal_mb=0.0)

    def run(self):
        conn = sqlite3.connect(self.db_path, timeout=60)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=60000")
        conn.execute("PRAGMA synchronous=NORMAL")
        buf: list[tuple] = []
        last = time.time()

        def flush():
            if not buf:
                return
            for attempt in range(6):                     # safety net; single writer rarely locks
                try:
                    conn.execute("BEGIN")
                    conn.executemany(_UPSERT, buf)
                    conn.commit()
                    self.stats["rows"] += len(buf)
                    self.stats["batches"] += 1
                    break
                except sqlite3.OperationalError as e:
                    if "locked" not in str(e).lower():
                        raise
                    try: conn.rollback()
                    except Exception: pass
                    if attempt == 5:
                        self.stats["lock_errors"] += 1
                        self.stats["dropped"] += len(buf)
                        break
                    time.sleep(0.5 * (attempt + 1))
            buf.clear()
            if self.stats["batches"] and self.stats["batches"] % CKPT_EVERY == 0:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                self.stats["checkpoints"] += 1
            try:
                w = os.path.getsize(self.db_path + "-wal") / 1048576
                self.stats["max_wal_mb"] = max(self.stats["max_wal_mb"], w)
            except FileNotFoundError:
                pass

        while True:
            try:
                item = self.q.get(timeout=FLUSH_SEC)
            except queue.Empty:
                flush(); last = time.time(); continue
            if item is _STOP:
                flush(); break
            self.stats["max_qdepth"] = max(self.stats["max_qdepth"], self.q.qsize())
            buf.append(item)
            if len(buf) >= BATCH or (time.time() - last) >= FLUSH_SEC:
                flush(); last = time.time()
        # final drain + reclaim
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)"); self.stats["checkpoints"] += 1
        conn.close()


_writer: PeerWriter | None = None


_start_lock = threading.Lock()


def start_writer(db_path: str) -> None:
    """Idempotent: start the single writer thread for this process."""
    global _writer
    if _writer is None:
        _writer = PeerWriter(db_path)
        _writer.start()


_ipc_client = None


def ensure_writer(db_path: str) -> None:
    """Lazy, thread-safe, idempotent start on first enqueue. In IPC mode
    (DHT_WRITER_SOCK set) connect a client to the external writer process;
    otherwise start the in-process PeerWriter + atexit drain."""
    global _writer, _ipc_client
    if WRITER_SOCK:
        if _ipc_client is None:
            with _start_lock:
                if _ipc_client is None:
                    import dht_ipc_writer
                    _ipc_client = dht_ipc_writer.IPCClient(WRITER_SOCK)
        return
    if _writer is None:
        with _start_lock:
            if _writer is None:
                start_writer(db_path)
                atexit.register(stop_writer)


def enqueue(hash_val: str, ip_country: dict, today: str) -> None:
    """Drop-in for _upsert_peers_threadsafe's effect, but routed to the writer.
    Blocks if the queue is full (backpressure) — by design."""
    if WRITER_SOCK:                     # Phase 2: send to external writer process
        if ip_country:
            rows = [(hash_val, ip, c, today, today) for ip, c in ip_country.items()]
        else:
            rows = [(hash_val, "_queried_", "XX", today, today)]
        _ipc_client.send(rows)
        return
    w = _writer
    if w is None:                       # safety: not started -> caller falls back
        raise RuntimeError("PeerWriter not started")
    if ip_country:
        for ip, country in ip_country.items():
            w.q.put((hash_val, ip, country, today, today))
    else:
        w.q.put((hash_val, "_queried_", "XX", today, today))


def stop_writer(timeout: float = 120.0) -> dict:
    """Signal end-of-input, drain, and return stats. Idempotent."""
    global _writer
    if _writer is None:
        return {}
    _writer.q.put(_STOP)
    _writer.join(timeout)
    st = dict(_writer.stats)
    _writer = None
    return st
