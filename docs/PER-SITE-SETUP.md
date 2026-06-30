# Per-site outbound setup (bringing up reporting at a new site)

This is the operator playbook for getting a sigmond site's data flowing to the
upstream networks via `hs-uploader`. It was written from the worked bring-up of
**sigma (AC0G / EM38ww)** on 2026-06-29 and is meant to be reproducible at any
site by substituting identity + credentials.

> TL;DR of what is actually *per site*: a **callsign + grid**, a **unique
> reporter id per radiod/antenna**, and (for SFTP destinations) **one ed25519
> keypair** that self-registers. Everything else is code/defaults.

---

## 1. The four outbound paths and what each needs

| Path | Transport | Per-site inputs | Secret / provisioning |
|---|---|---|---|
| **pskreporter** (FT8/FT4, MSK144) | `PskReporterTcp` (in psk-/meteor-recorder) | callsign, grid | none (open TCP 4739) |
| **wsprnet.org** | `WsprNet` (HTTP MEPT) | callsign, grid | none (HTTP form) |
| **wsprdaemon.org** | `WsprdaemonTarSftp` (+`WsprdaemonTarFtp` bootstrap) | reporter id (rx_call), gateway hostnames | ed25519 key, **self-provisioned** via FTP bootstrap |
| **PSWS** (GRAPE Digital RF datasets, magnetometer zips) | `PswsDatasetSftp` (file *or* directory payload; `PswsMagnetometerSftp` is an alias) | PSWS `station_id` (S000NNN), `instrument_id` | ed25519/RSA key **manually** registered on the PSWS portal (one key serves all of a host's station ids) |

The two HTTP/TCP paths (pskreporter, wsprnet) need **only identity** — no keys,
no registration. They "just work" once the recorder runs with a callsign+grid.

---

## 2. Identity (callsign + grid)

Canonical source today is `coordination.toml`:

```toml
[host]
call = "AC0G"
grid = "EM38ww"
```

Per-recorder env/TOML can override (this is a known non-uniformity — see
§6). Callsign **rendering differs by destination** and matters:

- **pskreporter** wants the bare callsign (`AC0G`). On pskreporter.info you look
  up your receptions by setting the **receiver/monitor** callsign to `AC0G`
  ("signals received by AC0G") — not as a sender. (At sigma this caused a
  "reports aren't showing" scare; they were there all along under `AC0G`.)
- **wsprnet / wsprdaemon** use the suffixed **reporter id** (`AC0G/S`,
  `AC0G/B4`, …) so each radiod+antenna is distinct. This rides per-record as
  `rx_call`; the uploader's `WD_RECEIVER_CALL` is the fallback identity.

### Reporter id = one per radiod server + antenna

A site with multiple receivers gives each a unique reporter id, e.g. the AC0G
fleet uses `AC0G/B4`, `AC0G/B1`, `AC0G/B6`; sigma's single RX-888 is `AC0G/S`.
For wspr this is `WD_RECEIVER_CALL` (single-instance) or `WD_RX_CALL`
(decode-only instances feeding a shared uploader). **Do not** put a `/` reporter
id into `WD_UPLOAD_ID` — that's a tar *filename* prefix and a slash breaks it.

---

## 3. SSH key (only for SFTP destinations)

`hs-uploader` keys live at **`/etc/hs-uploader/keys/id_ed25519`** by default
(override with `HS_UPLOADER_SSH_KEY_FILE`). The directory does **not** exist
after a fresh install — create it group-writable so the service user (e.g.
`wsprrec`, who is in the `sigmond` group) can use it:

```bash
sudo install -d -o root -g sigmond -m 2775 /etc/hs-uploader/keys
```

> **Important gap (as of 2026-06-29):** nothing in the wsprdaemon transport
> calls `StationIdentity.ensure_ssh_key()`, and `public_key()` only *reads* the
> `.pub` (returns empty if absent). So the key is **not** auto-generated on first
> use, and the FTP bootstrap will ship an **empty** `ssh_public_key=` (the
> gateway then can never provision SFTP). **Generate the key explicitly** as the
> service user until that gap is fixed:

```bash
sudo -u wsprrec ssh-keygen -q -t ed25519 \
  -f /etc/hs-uploader/keys/id_ed25519 -N "" -C "hs-uploader@<reporter-id>"
```

PSWS keys are separate and **manually registered on the PSWS portal**
(per `hf-timestd/docs/PSWS_SETUP_GUIDE.md`); they are not self-provisioning.

---

## 4. wsprdaemon.org — the self-provisioning ("key exchange") flow

You do **not** email anyone or pre-register a key. The transport bootstraps
itself:

1. SFTP to the gateways is tried first (`AC0G_S@gw1.wsprdaemon.org` — the login
   user is the reporter id with `/`→`_`). On a new site this fails with
   `Permission denied` because the gateway has no key yet.
2. The transport falls back to **FTP** (`WsprdaemonTarFtp`, default
   `gw2.wsprdaemon.org`, baked-in `noisegraphs` creds) and includes
   `client_upload_info.txt` = `reporter_id=<id>` + `ssh_public_key=<pubkey>`.
3. The gateway auto-provisions SFTP for that reporter; **subsequent cycles
   switch to SFTP automatically.** Provisioning is gateway-paced (can take a
   while); data keeps flowing via FTP until then.

### Enable it (single-instance site, in-process uploader)

Add to the recorder instance env (`/etc/wspr-recorder/env/<instance>.env`):

```ini
WD_SFTP_SERVERS=gw1.wsprdaemon.org,gw2.wsprdaemon.org
WD_UPLOAD_WSPRDAEMON_DIR=wsprdaemon
WD_FTP_SERVERS=gw2.wsprdaemon.org      # arms the first-run bootstrap (defaults to gw2 anyway)
# WD_RECEIVER_CALL=<reporter id, e.g. AC0G/S> already present
```

The pipeline only **builds** when `WD_SFTP_SERVERS` is non-empty; the FTP
fallback is on by default (`WD_FTP_FALLBACK=1`).

### systemd gotcha — `PrivateTmp=true` is required

`WsprdaemonTarSftp._upload_tar` stages the `.tbz` with
`tempfile.NamedTemporaryFile`, but the recorder unit runs
`ProtectSystem=strict`, making host `/tmp` read-only. Without a private tmp the
upload dies with *"No usable temporary directory found"*. The fix (matches
ac0g-b4's units):

```ini
# /etc/systemd/system/<unit>.d/10-private-tmp.conf
[Service]
PrivateTmp=true
```

Then `sudo systemctl daemon-reload && sudo systemctl restart <unit>`.

### Multi-receiver sites (reference architecture: ac0g-b4)

Larger sites split decode from upload: each receiver is a **decode-only**
`wspr-recorder@<inst>` (`WD_RX_CALL=AC0G/Bn`, no `WSPR_USE_HS_UPLOADER`), and a
single **`wspr-uploader.service`** (`ExecStart … wspr_recorder.cli uploader`,
env `/etc/wspr-recorder/uploader.env`) drains the shared sink and uploads
everything, with cross-RX merge gating (`WD_MERGE_REPORTERS`,
`WD_MERGE_BACKSTOP_SEC`). Sigma uses the simpler in-process model (one radiod).

---

## 5. Verification

```bash
# pskreporter: confirm reports land (look up the RECEIVER callsign)
curl -A 'site-diag' 'https://retrieve.pskreporter.info/query?receiverCallsign=<CALL>&flowStartSeconds=-3600'
#   → receptionReport entries with receiverCallsign=<CALL>  (rate-limited; don't poll)

# hs-uploader pipelines (acked, no dead_letter):
python3 - <<'PY'
import sqlite3; c=sqlite3.connect("file:/var/lib/hs-uploader/watermarks.db?mode=ro",uri=True)
for r in c.execute("select dest_id,outcome,count(*),max(ts) from attempts group by 1,2"): print(r)
PY

# wsprdaemon cycle ships:
sudo journalctl -u 'wspr-recorder@<inst>.service' | grep 'shipped wsprdaemon=[1-9]'

# GRAPE → PSWS: confirm datasets actually landed
sudo -u timestd hf-timestd grape status
sudo -u timestd sftp -i /home/timestd/.ssh/id_rsa_psws S000NNN@pswsnetwork.eng.ua.edu   # ls -la
```

> GRAPE caveat: `hf-timestd grape status` may report **"Verification failed"**
> even when data is present — `verify()` checks for the trigger directory
> (`c<dataset>_#<instr>_#<ts>`), which the PSWS server *consumes* after ingest.
> Confirm via the SFTP `ls` above (the `OBS*` dirs are the truth), not the local
> queue's error field. (Tracked code bug.)

---

## 6. Known non-uniformities & code gaps (Phase 2 backlog)

1. **Config surface differs per client** — psk uses `PSK_*` env + `[station]`
   TOML, wspr uses `WD_*` env, mag uses `[station]`/`[uploader]` TOML.
   (hf-timestd GRAPE was a separate PSWS uploader; **as of 2026-06-30 it is
   folded onto hs-uploader** — see below — so there is now ONE PSWS transport.)
2. **`ensure_ssh_key()` is never called** by the wsprdaemon transport → fresh
   sites ship an empty pubkey and never provision SFTP. Workaround in §3.
3. **`PrivateTmp` drift** — the deployed wspr-recorder unit shipped with
   `PrivateTmp=false`; tar uploads need `true`. Should be the unit default.
4. **GRAPE `verify()` trigger-dir false-negative** (see §5).
5. **Orphaned `pending_uploads` rows** — `WsprCycleSource` anchors at
   `start_at='now'`, so historical `wspr.noise`/spots predating enablement never
   drain through wsprdaemon; and `superdarn.detections` is queued with no
   transport at all. Needs a janitor / per-client egress decision.

---

## Appendix — sigma (AC0G/S) as the worked example, 2026-06-29

| Path | Result | Notes |
|---|---|---|
| psk → pskreporter | ✅ working | 691 reports/hr as receiver `AC0G` (was a lookup-direction confusion) |
| wspr → wsprnet | ✅ working | unchanged |
| GRAPE → PSWS | ✅ working | datasets present on PSWS through 06-28; verify false-negatives only |
| wspr → wsprdaemon | ✅ enabled | via FTP bootstrap; SFTP auto-upgrades on gateway provision |

Changes made on sigma: created `/etc/hs-uploader/keys` (root:sigmond 2775);
generated `wsprrec` ed25519 key; appended `WD_SFTP_SERVERS`/
`WD_UPLOAD_WSPRDAEMON_DIR`/`WD_FTP_SERVERS` to
`/etc/wspr-recorder/env/AC0G=S.env`; added `PrivateTmp=true` drop-in; restarted
`wspr-recorder@AC0G=S`.
