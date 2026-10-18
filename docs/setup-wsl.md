# Setting up the backend on WSL2 (Windows 10)

The backend runs on Linux, which is WSL2 Ubuntu now and a dedicated Linux desktop later. These steps give you the same environment on both, so moving to the desktop is just `git clone` plus these steps.

**Why not run it natively on Windows?**

- Windows can't fsync a directory.
- `os.replace` fails while another process has the file open.
- Docker's Linux containers run inside WSL2 anyway.

The code still runs on Windows for quick tests, but the long-running pipeline should run in WSL.

## 1. Install WSL2 and Ubuntu

In **PowerShell as Administrator**:

```powershell
wsl --install -d Ubuntu-24.04
```

Reboot when it asks. On first launch, Ubuntu asks you to create a Linux username and password.

Check that it's WSL **2**:

```powershell
wsl -l -v      # VERSION column must say 2
```

### Limit WSL's memory

WSL2 can otherwise take up to half your RAM. Create `C:\Users\Dawson\.wslconfig`:

```ini
[wsl2]
memory=10GB
swap=8GB
```

Then run `wsl --shutdown` in PowerShell and reopen Ubuntu.

## 2. Python and the project, inside Ubuntu

```bash
sudo apt update && sudo apt install -y python3 python3-venv python3-pip git build-essential
python3 --version        # Ubuntu 24.04 ships 3.12, which is supported

# Keep the repo and data on the Linux filesystem (~), NOT under /mnt/c.
# /mnt/c is slow and its fsync/rename semantics are Windows ones.
cd ~
git clone https://github.com/DawsonGeist/news_rag.git
cd news_rag
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest
```

`feeds/source/feeds_world.yaml` isn't in git. To regenerate candidates inside WSL, copy it over from Windows once:

```bash
cp /mnt/c/Users/Dawson/source/news_rag/feeds/source/feeds_world.yaml feeds/source/
```

### Editing from VS Code

Install the **WSL** extension. Then, in Ubuntu, run `code .` from `~/news_rag`. VS Code reopens connected to WSL, and Claude Code works there too.

From now on, treat the WSL copy as the main working copy. The Windows copy at `C:\Users\Dawson\source\news_rag` can be deleted once you've pushed everything from it.

## 3. GPU check (needed from Phase 2)

The Windows NVIDIA driver already exposes the GPU to WSL2. **Don't install a Linux NVIDIA driver inside WSL.**

```bash
nvidia-smi    # should list the RTX 2070 with 8192 MiB
```

PyTorch with CUDA gets installed in Phase 2.

## 4. Docker (needed only if Phase 2 picks Qdrant)

Phase 1 doesn't use Docker. When it's needed:

1. Install **Docker Desktop for Windows** from docker.com.
2. Turn on **Settings → General → Use the WSL 2 based engine**.
3. Turn on **Settings → Resources → WSL integration → Ubuntu-24.04**.
4. Check it from Ubuntu:

   ```bash
   docker run --rm hello-world
   ```

We'll write a `docker-compose.yml` at that point. You'll only need to run `docker compose up -d` and `docker compose down`.

## 5. Running Phase 1 (once implemented)

```bash
source .venv/bin/activate
export NEWSRAG_DATA_DIR=~/newsrag-data     # optional; defaults to ./data
newsrag db init
newsrag run all                            # poller + fetcher + writer, supervised
newsrag report lag --since 24h
```

## Moving to the Linux desktop later

1. Push from WSL.
2. On the desktop, run steps 2–4 with native Ubuntu (install the NVIDIA driver natively there).
3. To move the archive, stop the pipeline, then copy `data/` with `rsync -a`. The data directory is self-contained (SQLite plus files). Copy it only while nothing is running.
