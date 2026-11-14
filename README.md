# Pgderive

Pgderive incrementally maintains PostgreSQL query results from committed source changes.

- [`engine/`](engine/) contains the Rust engine, worker, PostgreSQL harnesses, and engine documentation. Run engine Cargo commands from this directory.
- [`web/`](web/) contains the Vite and React landing page for pgderive.com.

Run `./scripts/check.sh` from the repository root to check the engine and control plane. See the [engine README](engine/README.md) for local setup and the [web README](web/README.md) for development and deployment.

The planned SaaS control plane will serve the REST API and reconcile declarative query assignments. Workers will poll for their desired state and run the Rust engine. ECS Fargate and RDS for PostgreSQL are the current infrastructure candidates.

## Local control plane

Run `docker compose up --build -d` from the repository root. Envoy is
available at `http://localhost:8080`. Set `PGDERIVE_HTTP_PORT` to use another
port. Use the `localhost` hostname for Better Auth's configured origin.

- `services/auth` runs Better Auth and applies its schema during Compose startup.
  Better Auth exposes account and organization endpoints under `/api/auth/`.
- `services/api` runs Axum. `GET /api/v1/organizations/{id}` returns the
  authenticated user's organization context after Envoy verifies membership.
- `deploy/envoy` contains routing and the external authorization configuration.
- `engine` contains the `pgderive` library and CLI. The separate
  `engine/worker` package imports that library and builds `pgderive_worker`.
  Both use the engine workspace and its qualification harness. The root Cargo
  workspace builds the API.

Run `python3 scripts/check_control_plane.py` with the stack running to create
two disposable users and an organization and verify the access boundary.
The Compose PostgreSQL data is in tmpfs. `docker compose down` removes the
local stack and its auth data. Restarting the database alone also discards its
data. Use `docker compose down` followed by `docker compose up --build -d`
to initialize a fresh database and rerun migrations. The credentials and listener are
for local use.
