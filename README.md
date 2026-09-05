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

`played_at` alone is **not** a valid identifier. `RAW` contains two timestamps that each carry two
entirely different tracks, so a `played_at` is not unique to a play. Keying on it silently discarded
one play per collision. The cause is unconfirmed — `played_at` is assigned server-side rather than
by the client, but what makes two plays share a value is not established, and it is clearly not
simply "offline plays", which are common while collisions are rare. `played_at` and `track_id` are both retained as visible columns; the hash is the key,
not a replacement for readable ones.

One residual is accepted and unfixable, and it is narrow: two rows sharing **both** `played_at`
and `track_id`. Nothing in `RAW` distinguishes the same track genuinely played twice inside one
batch from the same single play fetched twice by the lookback, so the pair collapses to one row.
The same track played at a different `played_at` is a different key and is unaffected.

### Dimension policy

The dimensions are **SCD1**: one row per natural key — `track_id`, `album_id`, and the artist id
flattened out of `track_artists` — holding
the most recently observed attributes, with earlier versions overwritten rather than versioned. Each
dim resolves this with a `QUALIFY row_number() ... order by played_at desc`. History is deliberately
not kept — nothing here needs to answer "what did this look like at the time", and SCD2 would cost
a validity range on every dimension to buy it.

The `unique` test on each dim's `id` is load-bearing rather than decorative: a duplicated id fans
out on every join from `fct_play_events` and inflates play counts silently.

---

## Repository layout

```
run.py                     Orchestrator — the only module that knows the wiring
profiles.yml               dbt connection for CI (env_var placeholders only)
requirements.in            Direct runtime dependencies — hand-written, 6 names
requirements.txt           Full resolved pin set — generated, never edited
requirements-dev.in        Direct test dependencies — hand-written
requirements-dev.txt       Full resolved pin set for tests — generated, never edited
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
uv venv --python 3.12.5
source .venv/bin/activate
uv pip install -r requirements-dev.txt
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

### Dependencies

Four files in two pairs. You edit the `.in` files; `uv` generates the `.txt` files.

| Edited by hand | Generated from it |
|---|---|
| `requirements.in` — 6 direct runtime packages | `requirements.txt` — 77 pins |
| `requirements-dev.in` — `pytest`, plus `-r requirements.in` | `requirements-dev.txt` — the same 77 plus 4 |

`pipeline.yml` installs `requirements.txt`; `tests.yml` installs `requirements-dev.txt`, which is a
complete standalone list rather than a layer on top of the other.

**Never edit a `.txt` by hand — the next compile discards it.** Add the package name to whichever
`.in` it belongs to (`requirements-dev.in` for test tooling, `requirements.in` for everything else),
then run all four commands:

```bash
uv pip compile requirements.in -o requirements.txt
uv pip compile requirements-dev.in -o requirements-dev.txt -c requirements.txt
uv pip install -r requirements-dev.txt
uv pip check
```

Both compiles run every time, regardless of which file you edited. `requirements-dev.txt` is a
complete list of all 81 pins rather than a layer on top of `requirements.txt`, and `tests.yml`
installs only that file — so skipping the second compile after a runtime change leaves the test job
running without the package you just added. `-c` constrains the dev resolution to the versions
already chosen for runtime, so the two files can never disagree about a shared package.

Two traps when regenerating `requirements.in` from scratch. **`dbt-snowflake` appears in no import**
— `run_dbt_build()` shells out to the `dbt` executable, so nothing an import scan can see will
reveal it. And a package's import name is often not its install name: `dotenv` is `python-dotenv`,
`yaml` is `PyYAML`, `snowflake` is `snowflake-connector-python`. `importlib.metadata.packages_distributions()`
maps one to the other.

**Why the split exists.** `pip freeze` *records* rather than *resolves* — it photographs whatever is
installed, so a venv that has drifted through months of ad-hoc installs gets written down as though
it were a decision. That is how five mutually incompatible pins were committed here and survived
undetected: a complete set of exact pins installs with a warning rather than a refusal, so CI stayed
green while the graph was unsatisfiable. **Green meant pip did as it was told, not that the versions
were compatible.** `uv pip compile` fails outright when no compatible set exists, and `uv pip check`
verifies the installed environment afterwards. Rebuilt clean 2026-09-04: 77 packages, no conflicts.

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

**Every dbt build — local or CI — must run under `SPOTIFY_PIPELINE_ROLE`. Any build, not just
a full refresh.** Snowflake records ownership against the **role** that created an object, not the
user, and `SPOTIFY_PIPELINE_ROLE` holds only `SELECT`, `CREATE TABLE` and `CREATE VIEW` on the dbt
schemas — its ability to *replace* these objects comes from having created them. The intermediate
views and mart tables are rebuilt with `CREATE OR REPLACE` on every run, so a single build under a
different role transfers ownership, kills the grants attached to the dropped object, and leaves the
next scheduled run unable to replace it. Nothing in a diff shows this.

The role, not the user, is the requirement. `~/.dbt/profiles.yml` authenticates as the personal user
`ishanb30` but pins `role: SPOTIFY_PIPELINE_ROLE` on both its targets, so an ordinary local build is
already safe. From inside `transform/`:

```bash
dbt debug                                # confirm the role before continuing
dbt build --full-refresh
```

Verified 2026-09-02 with `SHOW TABLES` / `SHOW VIEWS`: every dbt-built object is owned by
`SPOTIFY_PIPELINE_ROLE`. (`METADATA` and `AUTH` are owned by `ACCOUNTADMIN` — created once by
`setup.sql` and only ever read and written, never replaced. That is where `permissions.sql` does
real least-privilege work.)

Running as the service user via `--profiles-dir .. --target dbt_subprocess` also works and is what
CI does, but it is not required locally and costs a `PRIVATE_KEY` export — read from the key file
rather than `.env`, whose value carries literal `\n` escapes that `python-dotenv` expands and the
shell does not.

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
- **CI's write access rests on object ownership rather than grants**, so every dbt build must run
  under `SPOTIFY_PIPELINE_ROLE`. The pin that guarantees this locally lives in `~/.dbt/profiles.yml`
  — outside the repo, unversioned, and nothing fails loudly if it is removed. See Full refresh above.
- **The watchdog that reconciles abandoned `STARTED` rows is designed but not built.**
- **`track_id` is release-scoped, not song-level.** ISRC is the song-level grouping lever and is
  not yet modelled.
