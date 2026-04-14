# wednesday-frog

`wednesday-frog` is a self-hosted web console that sends the checked-in frog image, or an uploaded replacement image, to one or more Slack, Teams, Mattermost, Discord, and Zoom destinations.

The authoritative implementation scope for this repo lives in [IMPLEMENTATION_PLAN.md](./IMPLEMENTATION_PLAN.md). Any change that affects scope, interfaces, storage, auth, delivery behavior, or deployment should update that plan first.

## Features

- Local admin setup and login
- Success confirmations that auto-dismiss after about 15 seconds, with 15-minute idle logout protection
- Schema-driven bundled plugin system for Slack, Teams, Mattermost, Discord, and Zoom
- Built-in web UI for settings, assets, destinations, channels, and encrypted secrets
- Local SQLite storage under `/data` by default, with PostgreSQL support for HA mode
- Encrypted secret storage with dual-key rotation support
- Manual send, scheduled send, fallback asset protection, and per-destination test sends
- Token-protected Prometheus-style `/metrics`
- Security-audit friendly dependency policy with `pip-audit` in the dev toolchain
- Docker-first deployment, plus an optional Redis + PostgreSQL HA compose profile

## Quickstart

1. Copy `.env.example` to `.env`.
2. Replace the bootstrap secrets with unique 32+ character values, or point the matching `_FILE` vars at secret files.
3. Start the app:

```bash
docker compose up --build
```

4. Open `http://localhost:8000`.
5. Use the setup token from `.env` to create the first admin account.
6. Visit `/settings` and `/destinations` to finish configuration.

`compose.yaml` persists the SQLite database, uploaded assets, and file-backed secrets in `./frog_data:/data`.

The checked-in Docker definitions use pinned image patch tags, and the HA Compose example expects PostgreSQL credentials to come from `.env` instead of inline placeholder secrets.

## Bootstrap Environment Variables

These are the app-owned runtime settings outside the database:

- `WEDNESDAY_FROG_MASTER_KEY` or `WEDNESDAY_FROG_MASTER_KEY_FILE`
- `WEDNESDAY_FROG_PREVIOUS_MASTER_KEY` or `WEDNESDAY_FROG_PREVIOUS_MASTER_KEY_FILE`
- `WEDNESDAY_FROG_SESSION_SECRET` or `WEDNESDAY_FROG_SESSION_SECRET_FILE`
- `WEDNESDAY_FROG_SETUP_TOKEN` or `WEDNESDAY_FROG_SETUP_TOKEN_FILE`
- `WEDNESDAY_FROG_DATABASE_URL` or `DATABASE_URL`
- `WEDNESDAY_FROG_REDIS_URL` or `REDIS_URL`
- `WEDNESDAY_FROG_METRICS_TOKEN` or `WEDNESDAY_FROG_METRICS_TOKEN_FILE`
- `WEDNESDAY_FROG_OUTBOUND_ALLOWLIST`
- `WEDNESDAY_FROG_DISABLE_SCHEDULER`
- `WEDNESDAY_FROG_SHUTDOWN_GRACE_SECONDS`
- `WEDNESDAY_FROG_SECURE_COOKIES`
- `TZ`
- `POSTGRES_USER`, `POSTGRES_PASSWORD`, and `POSTGRES_DB` when using `compose.ha.yaml`

The app refuses to start if the master key, session secret, or setup token are missing, still use the placeholder values from `.env.example`, or are shorter than 32 characters.

`WEDNESDAY_FROG_SECURE_COOKIES` defaults to secure-only session cookies. Set it to `false` only for local plain-HTTP development.

## Persistence And Backups

The application stores persistent state in `/data`:

- `/data/wednesday_frog.db` for SQLite
- `/data/assets/` for uploaded images
- `/data/logs/` for app logs if you add file logging later
- optional file-backed secrets such as `/data/master.key`

Back up the entire `/data` directory in single-node mode.

`compose.yaml` uses a bind mount so your data survives rebuilds:

```yaml
volumes:
  - ./frog_data:/data
```

The repo ignores local database files and persistence directories via `.gitignore`, including `*.db`, `*.sqlite*`, `data/`, and `frog_data/`.

## CI And Pull Requests

GitHub Actions runs both the `tests` and `Security Audit` workflows on pull requests and on pushes to `main`.

The `tests` workflow installs the project with dev dependencies and runs:

```bash
pytest -q
```

The `Security Audit` workflow audits the resolved dependency set with:

```bash
pip-audit
```

To make those checks block merges, configure GitHub branch protection or a ruleset for `main` and mark both the `tests` and `pip-audit` status checks as required.

## Default Schedule

On first startup the app seeds these defaults:

- Timezone: `UTC`
- Schedule enabled: `true`
- Cron: `0 12 * * wed`

That means Wednesday at `12:00 PM` in the configured timezone.

The settings page uses a full IANA timezone dropdown with `UTC` pinned first, shows the selected timezone and a human-readable schedule summary, and auto-saves when the timezone selection changes.

The weekly cadence is fixed to Wednesday. The UI no longer exposes raw cron editing. Instead, admins choose only the Wednesday delivery time by:

- selecting an hour and minute from dropdowns
- or typing a time manually in a common format such as `9:05 AM` or `21:05`

## Web Console Overview

- `/setup` creates the first local admin
- `/login` signs into the admin console
- `/settings` edits timezone, Wednesday delivery time, caption, scheduler state, and the active image
- `/destinations` creates plugin-backed destinations
- `/destinations/{id}` manages plugin config, encrypted secrets, and channels
- `/test` runs manual sends
- `/history` shows recent runs and per-attempt outcomes
- `/metrics` exposes Prometheus-style metrics only when a metrics token is configured and supplied

Outbound webhook/API traffic is validated against resolved IPs, pinned to the approved address for the actual connect, and does not inherit proxy settings from ambient environment variables.

If an uploaded asset is missing or unusable, the app falls back to the bundled `wednesday-frog.png` and badges the dashboard so the admin can see that fallback mode is active.

Uploaded assets are validated as real PNG or JPEG images based on the decoded file contents, not just the browser-reported MIME type.

Success flashes auto-dismiss after about 15 seconds. Authenticated sessions are logged out after 15 minutes of inactivity.

## CLI

Run the app locally without Docker:

```bash
wednesday-frog serve
wednesday-frog run-now
wednesday-frog validate-config
wednesday-frog check
wednesday-frog check --emit-plugin-env slack
wednesday-frog rekey-secrets
```

- `check` validates bundled plugin manifests, imports, and supported JSON Schema usage.
- `check --emit-plugin-env <plugin_id>` prints placeholder env and Compose hints for local plugin testing.
- `rekey-secrets` rewrites stored encrypted secrets with the current master key. During rotation you can keep the old key available through `WEDNESDAY_FROG_PREVIOUS_MASTER_KEY` until every node has restarted.

## Plugin Model

Each bundled plugin lives under `src/wednesday_frog/plugins/<plugin_id>/` and contains:

- `manifest.json`
- `plugin.py`

The app discovers these plugins at startup. A broken plugin does not crash the server; it is marked unavailable and reported in the dashboard, readiness checks, and metrics.

Plugins define JSON Schema for:

- destination config
- destination secrets
- channel config
- channel secrets

The admin UI renders forms from that schema instead of hardcoding service-specific fields.

## Service Setup

### Slack

Use a Slack bot token and channel IDs.

1. Create or reuse a Slack app with a bot user.
2. Grant at least `files:write` and `chat:write`.
3. Install the app to the workspace.
4. Invite the bot to each target channel.
5. In Wednesday Frog:
   - Create a `Slack` destination.
   - Save the bot token as the destination secret.
   - Add one channel row per Slack channel ID.

Wednesday Frog uses Slack's external upload flow.

### Teams

Use a per-channel Incoming Webhook URL.

1. In the target channel, create an Incoming Webhook connector.
2. Copy the webhook URL.
3. In Wednesday Frog:
   - Create a `Teams` destination.
   - Add one channel row per target channel.
   - Save the webhook URL on each channel row.

The app compresses the frog image for Teams' tighter webhook payload limits.

### Mattermost

Use your Mattermost server URL, an access token, and channel IDs.

1. Create a bot account or personal access token.
2. Confirm the token can upload files and create posts.
3. Find the target channel IDs.
4. If the Mattermost server is on a private network, add its host or CIDR to `WEDNESDAY_FROG_OUTBOUND_ALLOWLIST`.
5. In Wednesday Frog:
   - Create a `Mattermost` destination.
   - Save the base URL on the destination.
   - Save the token as the destination secret.
   - Add one channel row per Mattermost channel ID.

### Discord

Use a per-channel webhook URL.

1. Create a webhook in the target Discord channel.
2. Copy the webhook URL.
3. In Wednesday Frog:
   - Create a `Discord` destination.
   - Add one channel row per webhook target.
   - Save the webhook URL on each channel row.

### Zoom

Use Zoom Team Chat file sending with OAuth credentials, a sender user ID, and channel IDs.

1. Create a Zoom OAuth app that can send Team Chat files.
2. Collect:
   - account ID
   - client ID
   - client secret
   - sender user ID, or `me` for compatible user-level setups
   - optional bot JID if you want a follow-up caption message
3. In Wednesday Frog:
   - Create a `Zoom` destination.
   - Save the account ID, client ID, sender user ID, and optional bot JID on the destination.
   - Save the client secret as the destination secret.
   - Add one channel row per Zoom Team Chat channel ID.

## High Availability Mode

The repo includes `compose.ha.yaml` for a single-host Redis + PostgreSQL example:

```bash
docker compose -f compose.ha.yaml up --build
```

That profile:

- stores PostgreSQL data in a named Docker volume
- stores app data in a persistent `/data` volume
- reads PostgreSQL credentials from `.env` instead of inline placeholder values
- sets `stop_grace_period: 60s` so slow in-flight uploads have time to finish

HA mode is active only when both PostgreSQL and Redis are configured. The scheduler uses Redis locking plus a database uniqueness constraint on `(trigger_kind, scheduled_slot)` to prevent duplicate scheduled sends.

For true multi-host HA, every app node must share:

- the same database
- the same Redis instance
- the same session secret
- the same `/data/assets` and file-backed secret storage, or a future external asset/key store

## Local Development

Create a virtual environment and install the package in editable mode:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
```

Then run:

```bash
wednesday-frog serve
pytest
pip-audit
```

## Health And Validation

- `GET /health/live` reports process liveness.
- `GET /health/ready` validates bootstrap config, plugin load health, fallback asset state, and destination readiness, but only returns a redacted summary to anonymous callers.
- `GET /api/v1/config/validate` returns the current validation report from the authenticated admin session.
- `GET /metrics` returns Prometheus-style metrics only when the configured metrics token is supplied.

## Notes

- Secrets are encrypted at rest and never shown again in plaintext after save.
- Third-party service secrets are stored in the database, not in `.env`.
- Test sends are real API calls and do not contribute to the circuit-breaker auto-disable threshold.
[![ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/B0B615LXDL)
