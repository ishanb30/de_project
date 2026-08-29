# spotify_pipeline

An automated ELT pipeline that ingests personal Spotify listening history and models it into a
dimensional warehouse. Runs unattended on GitHub Actions every two hours.

**Stack:** Python (orchestration) · Spotify Web API (source) · Snowflake (warehouse) ·
dbt Core (transformation) · GitHub Actions (scheduling) · healthchecks.io (liveness)

No Airflow or Dagster — orchestration is a single Python entrypoint, deliberately.

---

## Architecture

```
Spotify Web API  ──►  run.py  ──►  Snowflake RAW  ──►  dbt  ──►  STAGING ──► INTERMEDIATE ──► MART
  /recently-played     fetch          (VARIANT)         build
                       load
```

One invocation of `run.py` performs the whole cycle:

1. Opens a single Snowflake connection and writes a `STARTED` row to `METADATA.PIPELINE_RUNS`.
2. Reads the refresh token from `AUTH.TOKENS`, exchanges it for an access token.
3. Reads the last watermark, requests `/me/player/recently-played` with `after` set to
   `watermark − 75 minutes`, and validates the response.
4. Inserts the raw JSON into `RAW.RECENTLY_PLAYED` and returns the new watermark.
5. Pings healthchecks.io.
6. Shells out to `dbt build`, which runs every model and every test.
7. Updates the run row to `COMPLETED` with the new watermark.

Any unhandled exception marks the run `FAILED`, which leaves the watermark unadvanced so the next
run re-ingests the same window. dbt is idempotent, so re-ingestion is safe.

### Why the lookback exists

Spotify stamps `played_at` server-side and delivers plays late or out of order, so a request for
"everything since the last watermark" misses rows. Each run therefore re-requests a 75-minute
overlap and relies on dbt to deduplicate. The window is a single source of truth in
`transform/dbt_project.yml` (`vars.lookback_window_mins`); Python reads the same value out of that
file rather than holding its own copy.

---

## Data model

| Layer | Schema | Models |
|---|---|---|
| Source | `RAW` | `RECENTLY_PLAYED` — API response as `VARIANT`, plus `LOADED_AT` and `RUN_ID` |
| Staging | `STAGING` | `stg_recently_played` — incremental, `merge`, deduplicated |
| Intermediate | `INTERMEDIATE` | `int_recently_played`, `int_bridge_track_artists`, `int_bridge_album_artists` |
| Mart | `MART` | `fct_play_events`, `dim_track`, `dim_album`, `dim_artist` |

Two further schemas hold state rather than analytics: `METADATA` (`PIPELINE_RUNS`) and `AUTH`
(`TOKENS`, which the pipeline role may only read).

### Grain

The grain of every play-event model is **`play_event_key`**, a surrogate hash of
`played_at` + `track_id`.

`played_at` alone is **not** a valid identifier. Spotify stamps plays server-side, and plays
buffered offline arrive as a batch sharing one timestamp — two collisions are present in `RAW`,
each carrying two entirely different tracks. Keying on `played_at` silently discarded one play per
collision. `played_at` and `track_id` are both retained as visible columns; the hash is the key,
not a replacement for readable ones.

One residual is accepted and unfixable, and it is narrow: two rows sharing **both** `played_at`
and `track_id`. Nothing in `RAW` distinguishes the same track genuinely played twice inside one
batch from the same single play fetched twice by the lookback, so the pair collapses to one row.
The same track played at a different `played_at` is a different key and is unaffected.

---

## Repository layout

```
run.py                     Orchestrator — the only module that knows the wiring
profiles.yml               dbt connection for CI (env_var placeholders only)
requirements.txt           Pinned runtime dependencies
requirements-dev.txt       Test dependencies (includes requirements.txt)
src/
  auth.py                  One-off OAuth authorisation code flow
  token_manager.py         Refresh-token → access-token exchange
  fetch.py                 API request, retry policy, response validation
  load.py                  Insert into RAW, watermark computation
  connector.py             Snowflake connection (key-pair auth)
utils/                     Logging, paths, shared helpers
infra/
  setup.sql                Database, schemas, tables
  permissions.sql          Role-level grants (idempotent, standalone)
  create_service_user.sql  One-time provisioning of SPOTIFY_PIPELINE_SVC
transform/                 dbt project — all dbt commands run from inside here
tests/                     pytest suite
.github/workflows/
  pipeline.yml             Scheduled pipeline
  tests.yml                pytest on every push
```

---

## Setup

Requires Python 3.12.5 and a Snowflake account.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
cd transform && dbt deps
```

Then, in order:

1. Run `infra/setup.sql`, `infra/permissions.sql`, `infra/create_service_user.sql` in Snowsight.
   Select the whole script before executing — Snowsight otherwise runs only the statement your
   cursor sits in.
2. Generate an RSA key pair and register the public key on the service user. `TYPE = SERVICE`
   users cannot hold a password on this account, so key-pair auth is mandatory.
3. Create a `.env` at the repo root with `CLIENT_ID`, `CLIENT_SECRET`, `ACCOUNT_IDENTIFIER`,
   `SNOWFLAKE_USER`, `ROLE`, `PRIVATE_KEY`, `DATABASE`, `DATA_WAREHOUSE`, `HEARTBEAT_URL`.
4. Run `src/auth.py` once to complete the OAuth flow, and seed the resulting refresh token into
   `AUTH.TOKENS`.

For CI, the same values are GitHub repository secrets, except `DATABASE`, `DATA_WAREHOUSE` and
`ROLE`, which stay plain text in the workflow file — a secret is write-only, so putting
configuration there forfeits diff, history and review.

---

## Running

**Locally, the full cycle:**

```bash
source .venv/bin/activate   # dbt must be on PATH; run.py shells out to it
python run.py
```

**In CI:** `pipeline.yml` runs on cron `17 */2 * * *` (UTC) and on `workflow_dispatch`. Steps are
checkout → setup-python → `pip install -r requirements.txt` → `dbt deps` → `python run.py`, with
secrets scoped to the final step only. `dbt deps` is a workflow step rather than part of
`run_dbt_build()` on purpose: a failed package download is a setup failure, and inside the
orchestrator's `try` it would write `RUN_STATUS = 'FAILED'` for a run that never started.

### Full refresh

`stg_recently_played` is incremental, so an ordinary run only merges new rows. Rebuilding it from
all of `RAW` — required whenever the model's grain or columns change — needs `--full-refresh`.

**Run it as the service user, not as yourself.** Snowflake ownership follows the role that created
the object, and `SPOTIFY_PIPELINE_ROLE` holds only `SELECT` and `CREATE TABLE` on the dbt schemas —
its write access to these tables comes from having created them. Rebuilding under a personal role
transfers ownership and breaks the next scheduled run on privileges.

From inside `transform/`:

```bash
export PRIVATE_KEY="$(cat ../rsa_key.p8)"
export SNOWFLAKE_USER=<service user from .env>
export ACCOUNT_IDENTIFIER=<value from .env>

dbt debug --profiles-dir .. --target dbt_subprocess     # confirm the user before continuing
dbt build --full-refresh --profiles-dir .. --target dbt_subprocess
```

`PRIVATE_KEY` is read from the key file rather than sourced from `.env`: the value there carries
literal `\n` escapes that `python-dotenv` expands and the shell does not. The exports live in that
shell session only.

---

## Testing

- **pytest** — 13 tests over the pure functions in `src/`, run on every push by `tests.yml`.
  No Snowflake or network access required.
- **dbt tests** — `unique` and `not_null` on the grain at the produced and consumed boundaries,
  `relationships` from `fct_play_events` to `dim_track` and `dim_album`, and a singular test
  asserting no play is stamped in the future. Run as part of `dbt build`, so a test failure fails
  the pipeline run.

Tests target what fails *silently*. A loud failure already has a detector, so covering it again
duplicates work — the value is where a wrong answer is indistinguishable from a right one.

---

## Monitoring

Three mechanisms, each covering a distinct failure, plus one in-band assertion:

| Mechanism | Detects |
|---|---|
| GitHub failure email | The process crashed, including a failed `dbt build` |
| healthchecks.io (cron mode, 100 min grace) | Nothing ran at all — the only detector that survives GitHub Actions itself being down |
| Zero-row assertion in `load()` | The API returned no plays, which is a defect rather than a state |
| `METADATA.PIPELINE_RUNS` | Post-hoc run history and watermark provenance |

Two things stay silent by construction and no additional monitor would change that: Spotify
dropping plays server-side, and an outage coinciding with a genuine quiet period.

---

## Known limitations

- **The endpoint retains only the last 50 plays.** More than 50 plays between runs is permanent
  loss; there is no pagination or catch-up that recovers an outage. The two-hour interval is sized
  against this, not against scheduler reliability.
- **GitHub Actions scheduling is materially unreliable** — measured 60–100 minute delays, silently
  skipped slots, and runner-acquisition failures that produce no logs. Delay does not compound.
- **The dimension models are neither SCD1 nor SCD2.** They are `select distinct` over every
  column rather than over the id, so an attribute changing upstream would leave two rows sharing
  one id. The `unique` test on `id` is what catches that, and it matters more than it looks: a
  duplicated id fans out on every join from `fct_play_events`, inflating play counts silently.
- **CI's write access rests on object ownership rather than grants.** See the full
  refresh procedure above.
- **The watchdog that reconciles abandoned `STARTED` rows is designed but not built.**
- **`track_id` is release-scoped, not song-level.** ISRC is the song-level grouping lever and is
  not yet modelled.
