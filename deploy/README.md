# deploy/ — systemd units & config

- **pantheon-web.service** — Flask/gunicorn dashboard service
- **pex-harvest.service** — PEX collector service
- **logrotate.conf** — log rotation

Note: production runs everything from a **flat** directory, so `ExecStart` paths in
these units are flat (e.g. `/home/ec2-user/hash_trackerv2/pantheon_web.py`), not
the folder layout of this repo. Adjust paths for your deployment.
