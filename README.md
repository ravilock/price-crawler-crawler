# crawler: local development

This repository owns its Compose environment and local tooling. No sibling checkout is
required. The default dependency inventory is **rabbitmq, crawler-db**. The `app` profile
builds this checkout. Broker, database and Keycloak data use project-scoped named volumes.

## Prerequisites and settings

Install Docker Engine with Compose v2.24.4+ (including `!reset` support for acceptance),
Make and Python 3. Host Go commands require the version in `go.mod`; Node services require
Node 22 and their declared npm packages. Renderer workflows also require
`npx playwright install --with-deps chromium` on the host. Authenticate Docker to GHCR if
your selected packages are private; registry tokens stay in Docker's credential store.
Copy `.env.example` to `.env` and replace local password placeholders. Use URL-safe local
password characters or percent-encode credentials in connection URLs. Keep secrets out of
Git and logs. These local credentials are externally supplied and are not demo user accounts.

For each setting: Make command-line > exported shell > selected `ENV_FILE` > Compose default.
Empty dependency tags and owners are errors. `GHCR_OWNER` defaults in the example to
`ravilock`; project image references themselves require a nonempty owner. Override a sibling
with its `<SERVICE>_IMAGE_TAG`, such as `CRAWLER_IMAGE_TAG`. Normal `make config` prints
only service names and image references. Full `docker compose config` is a sensitive
operation and can print credentials. `COMPOSE_PROJECT_NAME` defaults to `price-crawler-local`;
use a distinct value and host ports for simultaneous environments.

## Run dependencies, then the checkout

```sh
cp .env.example .env
# Edit .env to supply local-only credentials and desired host ports.
make config
make deps.up
# Equivalently, after reviewing image access: docker compose up -d
make migrate.up
make migrate.version
make run
```

`make deps.up` pulls selected dependencies and stops on failure; it never substitutes a
cached image after a failed pull. The direct Compose command uses its normal pull policy;
explicitly pull first when freshness/availability verification is required.
Before host execution, apply this checkout's packaged migrations with `make migrate.up`
and inspect the clean result with `make migrate.version`. Then run `make run`.
For Node services, run `npm install` before host execution. `make run` maps database/broker
connections to the configured localhost ports, and binds the host process to `127.0.0.1`.

To build and run this checkout in a container:

```sh
make app.up
# Equivalent app-profile source build after dependency pull:
docker compose --profile app up --build -d
```

Container processes use Compose names and internal ports; the host process uses localhost.
Default application host port is `19080`, AMQP is `19072`, management is `19073`,
and owned PostgreSQL is `19032` when present. Keycloak uses `19081` when selected;
a sibling user API uses `19082`. Ports bind only to loopback. Configure `APP_PORT`,
`RABBITMQ_PORT`, `RABBITMQ_MANAGEMENT_PORT`, `POSTGRES_PORT`, `KEYCLOAK_PORT`, and
`USER_API_PORT` in `.env`; sibling database port overrides are named in Compose.

The browser-visible issuer is `OIDC_BASE_URL/realms/price-intelligence`. User API verifies
that same issuer and fetches keys through `http://keycloak:8080` inside Compose; host
`make run` maps this key URL to the published Keycloak port. If browser origins change,
update this repository's `local/keycloak/price-intelligence-realm.json` redirect/web origins
before importing a fresh realm, or update the retained realm through the local admin UI.
An existing realm import does not overwrite retained Keycloak settings.

## Optional workflows

Enable profiles only for the workflow being exercised, together with the checkout's `app`
profile for container execution. Their complete additional dependency closure is:

| Profile | Additional dependencies |
|---|---|
| collection | data-finder |
| rendered | data-finder, renderer-parser |
| api | user-api, keycloak |
| acceptance | controlled source, crawler, finder, renderer, user-api, keycloak |

```sh
# Example: select one profile listed above, without requiring sibling source.
make deps.up WORKFLOW_PROFILES="collection"
make app.up WORKFLOW_PROFILES="collection"
# Or docker compose --profile collection --profile app up --build -d
```

The controlled mock-source profile uses the public `v0.1.0-rc.1` image. Set a nonempty
`MOCK_CONTROL_TOKEN` in untracked local configuration; controls bind only to loopback.
Source origin inside Compose is `http://mock-store:8080`. Overrides persist in the
project-scoped `mock-store-data` volume, which normal down retains. Dependency-only
startup is distinct from a working collection, browser, monitoring or demo workflow.

## Image inventory and compatibility

Official providers: Docker Official Images for `postgres:16-alpine` (PostgreSQL) and
`rabbitmq:3.13-management-alpine` (RabbitMQ); Keycloak's official
`quay.io/keycloak/keycloak:26.1.4` when identity is selected. Only the dependencies present
in this Compose file are started. These explicit infrastructure tags were checked against
the registry on 2026-10-27. Project sibling defaults, also verified available on that date:

- `ghcr.io/ravilock/price-crawler-data-finder:2ea7fa5f575a837098e3ef0b39159fbf709777ee`
- `ghcr.io/ravilock/price-crawler-renderer-parser:ddaf34498116770221c311bc96df249c3d036beb`
- `ghcr.io/ravilock/price-crawler-user-api:06b665889d7087c14eca709497edb57c046f3a26`

These are published baseline commits using v1 messaging/configuration conventions. No
integrated readiness/business-workflow validation is claimed for this selection. `make`
startup warns that compatibility has not been established. An override is permitted, but
must remain a published explicit version or commit tag; changing a tag does not establish
content compatibility. Missing tags fail visibly. Review and record actual image content
and acceptance evidence before claiming compatibility.

## Source changes, verification and shutdown

For a reversible source-only check, change one startup log message in
`cmd/crawler/main.go`
(keep behavior unchanged).
Stop the host process with Ctrl-C and run `make run` again to observe the new message
in the terminal. For a container, run
`docker compose --profile app up --build -d app` and inspect `docker compose logs app`.
Revert the source edit and repeat.
Go/Node container assets rebuild in the Dockerfile; the frontend host dev server rebuilds
assets automatically. No migration is needed for this log/title edit.
Run the repository's `make test lint build` checks before publishing changes.

```sh
make down
# Or docker compose --profile '*' down
```

Normal shutdown retains volumes. Back up any retained data before an explicit reset,
verify the selected project with `docker compose ls`, then deliberately select
`docker compose --profile '*' down --volumes` only for that disposable project.
Never reset automatically after startup or migration errors. Dirty-version recovery must
stop the owner, inspect its version/error and actual schema, back up data, and repair or
restore the schema before an operator explicitly corrects the version and retries; no
automatic rollback or forced version is permitted.

## Delivery status

`/healthz` reports process liveness; `/readyz` requires an authenticated RabbitMQ connection, database SELECT 1, and a clean expected migration version. Startup waits up to `READINESS_TIMEOUT_SECONDS` (default 120); dependency failures degrade readiness and retry with exponential delays bounded by `RECONNECT_INITIAL_DELAY_SECONDS` (default 1) and `RECONNECT_MAX_DELAY_SECONDS` (default 30). The app-profile health check uses `/readyz`. Readiness does not establish business workflow or demo readiness.

Infrastructure dependencies are health-gated as applicable. The checked-out app's
health endpoints establish only the dependencies described above; they do not establish
business workflow or integrated demo readiness. After catalog, alerts, or broker
recovery, recurring group monitoring must wait for the synchronized demand watermark
before its next recovered scheduling decision. That later workflow synchronization is
not represented by process health and remains subject to its own acceptance checks.

## Verification — 2026-10-27

T085 checks passed with isolated RabbitMQ/PostgreSQL dependencies and the packaged
migration: `make run` started the crawler and `/healthz` returned alive; Ctrl-C stopped
the host process. Stopping the test broker made `make run` fail with a safe RabbitMQ
connection diagnostic. The isolated project was cleaned up after the check.

`make test lint build`, Compose resolution and the dependency-image inventory check
passed. The current test target contains no service behavior tests yet. Minimum
dependencies passed real `docker compose up --wait` in isolated project `t065-1d91f32568`,
with host ports removed; normal shutdown retained its named volumes. This verifies
dependency startup, not application, migration, demo or business-workflow readiness.

## Owned migrations — T073

The crawler image packages `/migrations`, the PostgreSQL-capable upstream
`migrate/migrate:v4.18.3` CLI at `/usr/local/bin/migrate`, and `/migration`, a bounded
administrative entry point that reads `DATABASE_URL` from environment configuration.
The upstream CLI image tag was verified published on 2026-10-27. `MIGRATION_TIMEOUT_SECONDS`
defaults to 120 and accepts 1–3600 seconds. Administrative migration jobs connect only to
the crawler's database. They never run another service's migrations.

```sh
make deps.up
make migrate.up
make migrate.version
make run
```

`make migrate.up` builds the checkout's image and runs an ephemeral `migrate` job after
the crawler database is healthy. `make migrate.version` uses the same image to report
`version=<number> dirty=<true|false>` or `version=none dirty=false`. Host Go does not need
a separately installed migration CLI. The app profile waits for successful completion
of the migration job before starting `/crawler`; it stays stopped on migration failure.
The integrated GHCR stack will use the matching packaged files after its separate T077
implementation and publication gates.

The historical SQL contents are retained as `001_initial.up.sql` and
`002_message_delivery.up.sql`. Their version ordering is unchanged. Version 1 owns the
initial collection tables and outbox; version 2 adds inbox deduplication and the pending
outbox index. Version 3 extends collection ownership and constraints; it preserves legacy
observations without a request ID while requiring one for new rows. Existing rows that
violate new constraints must be reviewed and corrected before upgrading. Re-running `up` on a clean current database is a no-op. Data and migration
metadata live in the crawler's retained database volume. An older image cannot migrate
a database whose tracked version is newer than its packaged release. Update the release
version guard in `cmd/migration/main.go` when adding a new owned migration.

### Adopt a database created before golang-migrate

An unversioned database containing any crawler-owned table is refused before applying
DDL or writing migration metadata. This avoids replaying schema creation over retained
data. Back up the database outside Git, stop the application, and compare the actual
schema with the ordered up files and a fresh disposable database first. Identify the
highest contiguous migration whose complete schema is already present. Do not infer a
baseline merely because one table exists, or adopt a partially applied schema.

Only after the schema is verified, an operator may explicitly record that baseline.
For a database confirmed to contain the complete versions 1 **and** 2, the command is:

```sh
docker compose --profile admin run --rm --entrypoint /bin/sh migrate -ec \
  'exec /usr/local/bin/migrate -path /migrations -database "$DATABASE_URL" force 2'
make migrate.version
make migrate.up
```

If only the complete version-1 schema is present, explicitly record version 1 instead;
`make migrate.up` then applies versions 2 and 3 normally. The database URL is expanded inside
the administrative container and is never echoed by the Make target. Baseline adoption
is a manual operation; no application or startup command forces a version automatically.

### Failed/dirty migrations and rollback

On failure, the migration command reports the version/dirty state and bounded diagnostics
with the configured URL/password redacted. The app remains stopped. Inspect the failed SQL,
the actual owned schema and a backup before repairing/restoring it. An operator may correct
version metadata only after identifying the schema that actually exists. Startup never
rolls back, clears dirty state, deletes data or forces a version. `make migrate.version`
remains available for diagnosis on a dirty database.

Down files are destructive, explicit administrative operations; they are never invoked
by the provided startup targets. Version 2 down removes inbox deduplication history and
its index while retaining the version-1 outbox. Version 1 down deletes the owned collection
tables and outbox. Back up retained data and review the SQL before any operator-directed
rollback. Ordinary `make down` preserves named volumes.

### Migration verification — 2026-10-27

`make test lint build` passed, including diagnostic redaction/bounds unit tests.
Real packaged-image acceptance passed fresh startup, retained restart, v1→v2 upgrade
with retained source data, refusal of unversioned owned tables, explicit verified
legacy adoption and an actual failed migration job blocking app startup while dirty
state stayed visible. Test projects `t073-fresh-7c89ea09bd`,
`t073-upgrade-b02346d2a8` and `t073-legacy-f564d9fc01` were shut down; their named
volumes were retained. Historical up SQL contents were preserved byte for byte.
This evidence covers owned migrations; application dependency-aware readiness remains
T085–T086.
