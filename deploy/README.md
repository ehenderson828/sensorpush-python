# Running as a systemd service

Replaces the old `nohup python sensorpush_python.py &` approach. As a systemd
service the sensor reader starts automatically on boot, restarts if it
crashes, and logs to the journal.

## Install

On the target machine (e.g. the Raspberry Pi), from the repo root:

```sh
sudo ./deploy/install.sh
```

This renders `deploy/sensorpush.service` with the current user, repo path, and
python interpreter (a repo-local `venv/` or `.venv/` is preferred if present),
installs it to `/etc/systemd/system/sensorpush.service`, and enables + starts
it. Pass a different service user as the first argument if needed:
`sudo ./deploy/install.sh pi`.

Prerequisite: a `.env` file in the repo root with `SUPABASE_URL` and
`SUPABASE_KEY`.

## Day-to-day

```sh
systemctl status sensorpush        # is it running?
journalctl -u sensorpush -f        # follow logs
sudo systemctl restart sensorpush  # restart after pulling new code
sudo systemctl disable --now sensorpush  # stop and disable
```

The script also still writes `sensor.log` in the repo root.
