# Setting up the backend (native Ubuntu)

The backend runs on one dedicated Linux desktop. These steps rebuild it from a fresh Ubuntu install.

| | |
| --- | --- |
| Machine | MSI Aegis R2, hostname `tower2` |
| CPU | Intel Core Ultra 7 265 (20 threads) |
| RAM | 32 GB |
| GPU | NVIDIA RTX 5060 Ti, 16 GB (Blackwell, `sm_120`) |
| Disk | 2 TB NVMe (TeamGroup T-Force), one LVM volume |
| Network | Realtek RTL8125 2.5GbE wired, Realtek RTL8852CE Wi-Fi 6E (`rtw89` driver) |
| OS | Ubuntu 26.04 LTS, kernel 7.0 |

Windows is no longer part of the setup.

## 1. System packages and drivers

```bash
sudo apt update && sudo apt full-upgrade
sudo apt install -y python3 python3-venv python3-pip git build-essential
sudo ubuntu-drivers install            # installs the recommended NVIDIA driver
sudo reboot
```

Check the drivers after rebooting:

```bash
nvidia-smi                              # RTX 5060 Ti, 16311 MiB, driver 595 or newer
iw dev wlp130s0 link                    # Wi-Fi: rtw89_8852ce, a few hundred Mbit/s
sudo fwupdmgr refresh && sudo fwupdmgr get-updates   # BIOS/SSD firmware via LVFS
```

- **NVIDIA:** use the `-open` kernel modules (`nvidia-driver-595-open` at setup time). Blackwell GPUs require them.
- **Wi-Fi:** the RTL8852CE is supported in-kernel. Its firmware comes from `linux-firmware-realtek`, which `apt full-upgrade` keeps current. No out-of-tree driver is needed.
- **Wired networking:** prefer Ethernet for the long-running pipeline. A 72 h soak shouldn't depend on Wi-Fi.
- **MSI BIOS updates:** MSI doesn't publish them to LVFS, so they come from MSI's site via a USB stick. Only update the BIOS if there's a specific fix you need.

## 2. Python and the project

```bash
cd ~/source
git clone https://github.com/DawsonGeist/news_rag.git
cd news_rag
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest
```

Python 3.12 or newer is supported. Ubuntu 26.04 ships 3.14.

`feeds/source/feeds_world.yaml` isn't in git. You only need it to regenerate `feeds/candidates_us.yaml`, so copy it over by hand if that ever comes up.

## 3. Data directory

The archive lives outside the repo at `~/newsrag-data` by default. Set `$NEWSRAG_DATA_DIR` or `data_dir` in `config/newsrag.toml` to use another path. Keep it on a local disk, never on a network mount.

```bash
newsrag db init                         # creates ~/newsrag-data/state/crawl.sqlite3
newsrag storage verify                  # checks storage invariants (add --deep to re-hash files)
newsrag test kill                       # Gate 1 kill test, about 20 s
```

## 4. GPU stack (Phase 2)

The RTX 5060 Ti is a Blackwell card (`sm_120`). It needs PyTorch wheels built for CUDA 12.8 or newer. Older `cu121` and `cu124` wheels import fine but fail at the first kernel launch. The 595 driver supports the CUDA 13.2 runtime, so we use the `cu130` wheels.

PyTorch comes from its own package index (pip can't read that from `pyproject.toml`), and then the `ml` extra:

```bash
pip install "torch==2.14.1" --index-url https://download.pytorch.org/whl/cu130
pip install -e ".[dev,ml]"
python -c "import torch; print(torch.cuda.get_device_name(), torch.cuda.get_device_capability())"
# NVIDIA GeForce RTX 5060 Ti (12, 0)
```

## 5. Docker: Neo4j (entity graph) and Qdrant (vector-store prototype)

Install Docker Engine from Docker's own apt repository (see docs.docker.com, "Install Docker Engine on Ubuntu"), then `sudo usermod -aG docker "$USER"`, then **log in again or reboot**. Group changes only reach new sessions, and Ubuntu 26.04 ships neither `newgrp` nor `sg`. As a stop-gap until the next login, `sudo setfacl -m u:$USER:rw /var/run/docker.sock` grants access immediately; it resets when Docker restarts.

Services are defined in `deploy/compose.yml`, with pinned versions and ports bound to `127.0.0.1` only. Data lives under `~/newsrag-data/services/`.

```bash
deploy/init-env.sh                                   # once: deploy/.env with a generated Neo4j password
docker compose -f deploy/compose.yml --env-file deploy/.env up -d
docker compose -f deploy/compose.yml --env-file deploy/.env ps
```

- **Neo4j:** browser at http://127.0.0.1:7474 (user `neo4j`, password in `deploy/.env`), bolt on 7687.
- **Qdrant:** http://127.0.0.1:6333/dashboard.
- **Restarts:** both restart with Docker (`unless-stopped`).
- **Losing them is not fatal:** both are derived stores, rebuildable from the archive.

## 6. Running the pipeline

The pipeline runs as a systemd **user** service, so it survives closing the terminal and logging out:

```bash
loginctl enable-linger "$USER"           # once: user services run without a login session
deploy/install-service.sh --start        # install, enable and start (re-run after moving the repo)

systemctl --user status newsrag          # running? recent log lines
journalctl --user -u newsrag -f          # supervisor output
tail -f ~/newsrag-data/logs/{supervisor,poller,fetcher,writer}.log
systemctl --user stop newsrag            # graceful: the writer flushes before exit
```

Health checks while it runs:

```bash
newsrag report lag --since 24h           # G1.3 lag percentiles, queue depth
newsrag report disk                      # G1.7 sizes and 12-month forecast
newsrag storage verify                   # storage invariants (add --deep to re-hash)
```

The service unit is `deploy/systemd/newsrag.service.in`, and the install script fills in this checkout's path. To run in the foreground instead, use `newsrag run all`.

## Moving the archive

To move the archive, stop the pipeline, then copy the data directory with `rsync -a`. It is self-contained (SQLite plus files). Only copy it while nothing is running.
