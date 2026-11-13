# Pgderive

Pgderive incrementally maintains PostgreSQL query results from committed source changes.

- [`engine/`](engine/) contains the Rust engine, worker, PostgreSQL harnesses, and engine documentation. Run Cargo commands from this directory.
- [`web/`](web/) contains the Vite and React landing page for pgderive.com.

Run `./scripts/check.sh` from the repository root to check the engine. See the [engine README](engine/README.md) for local setup and the [web README](web/README.md) for development and deployment.

The planned SaaS control plane will serve the REST API and reconcile declarative query assignments. Workers will poll for their desired state and run the Rust engine. ECS Fargate and RDS for PostgreSQL are the current infrastructure candidates.
