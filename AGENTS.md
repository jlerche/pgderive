# Working on Pgderive

Pgderive is a PostgreSQL-only incremental view maintenance engine based on DBSP.
The same PostgreSQL instance supplies source CDC, authoritative metadata and
durable progress, and destination tables. Immutable arrangement data will live
in object storage. Build small, independently testable steps toward that design.

## Repository workflow

- Work directly on `main` and make small, coherent commits; this is authorized
  for the initial project work. Do not push or deploy unless requested.
- Read the current code and relevant tests before changing an interface.
- Run `./scripts/check.sh` before committing. It is the common local and CI gate.
- Integration harnesses that mutate shared PostgreSQL must run sequentially.
  Use isolated fixture schemas/databases, never reset application or PoC data.
- Preserve failure evidence when a runtime or durability check fails.
- Docker PostgreSQL is disposable: use tmpfs, never persistent Docker volumes
  or bind-mounted database storage. Do not recreate shared PoC containers.

## Rust quality rules

- Use the pinned toolchain and rustfmt configuration. All compiler warnings are
  errors; unsafe code is forbidden.
- Clippy `all`, `pedantic`, and `nursery` are denied. Selected restriction lints
  prohibit unchecked unwraps, expects, panics, debug macros, and placeholders.
  Cognitive complexity, function length, argument count, type complexity, and
  nesting are explicitly enforced through `clippy.toml`.
- Prefer small functions and modules, named types, early returns, and checked
  conversions. Do not add unused code/dependencies for future work.
- Fix lint findings instead of weakening the policy. If a narrow exception is
  justified, use `#[expect(specific_lint, reason = "concrete justification")]`;
  broad/module/crate allowances require a demonstrated need. Do not use an
  unreasoned `allow` or silence errors to make the gate pass.
- `cargo machete` must report no unused dependencies. Any necessary scanner
  exception must be documented next to its Cargo metadata entry.
- Each Rust source file has a default limit of **1,000 physical lines**, including
  comments and blank lines. Prefer splitting the file. An intentional exception
  must appear once within the first ten lines and include a reason:

  ```rust
  // pgderive: max-lines=1500 -- generated protocol definitions kept together
  ```

  The directive sets that file's exact limit. Malformed, late, or duplicate
  directives fail the custom checker. It runs locally and in CI.

## Semantic and durability boundaries

- Preserve full-tuple weighted identity; navigation prefixes do not imply
  primary-key or last-write-wins semantics. Keep source-specific PK validation
  separate from generic weighted operators.
- Assemble complete committed source transactions. Both join inputs can change
  in one transaction; the incremental join must include the cross term.
- Keep source LSNs distinct from DBSP logical time. Initial execution is acyclic
  and transaction ordered; recursion/frontiers are unsupported until implemented.
- Durable publication is object PUT, then one PG transaction publishing object
  membership/result deltas/source progress, then logical-slot acknowledgement.
  A diagnostic listener must not acknowledge merely received or printed rows.
- Keep fine-grained indexes object-local/resident; PG stores object membership
  and coarse metadata. Keep logical consolidation and physical GC separate.
- Source WAL retention is an unresolved operational concern, not a solved
  consequence of crash consistency.
- Use the independent memory oracle and recorded PoC traces when stateful
  computation is introduced. The prior evidence is under
  `/home/jlerche/programming_projects/workspace/try_incremental_pg_cache/object_backed_dataflow`.
  Do not modify its accepted artifacts or frozen binaries.
