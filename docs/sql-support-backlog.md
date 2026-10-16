# SQL support expansion backlog

This workstream is separate from the architecture milestones in the
[PostgreSQL fidelity roadmap](postgres-fidelity-roadmap.md). M2 establishes local
execution for the supported domain; it is not held open until every PostgreSQL
function is implemented. The closed registry in `src/compiler/capabilities.rs`
and the [capability inventory](postgres-capabilities.md) describe current support.

Each implementation task must name exact overloads, input/output domains, coercions,
NULL/strictness and error behavior, context dependencies, budgets and retraction
semantics. Qualify against PostgreSQL and an independent oracle, including CDC and
cold restart where applicable. Unsupported functionality must fail explicitly;
connected PostgreSQL computation and sink deferral are not fallback strategies.

| Family | Separate implementation tasks / qualification concerns |
| --- | --- |
| Numeric and integral | Additional arithmetic, rounding, numeric ABS and numeric casts; exact scale, special values, width/error boundaries |
| Text and encodings | Additional string operations and casts; UTF8, trailing spaces, byte versus character semantics and expansion budgets |
| Locale and collation | Case conversion and comparison; provider/version-specific behavior, normalization, deterministic versus nondeterministic equality |
| Calendar and timezones | Extraction, truncation, named zones, intervals and formatting; DST transitions, infinity, BC and external timezone data |
| Regex and formatting | PostgreSQL-compatible matching/replacement, capture/array behavior, format languages and resource limits |
| Arrays and structured values | Native codecs, subscripts, NULL elements, bounds, constructors and equality/representation |
| Floating point | Native codecs, scalar operations and aggregates; special values, rounding and order-dependent evaluation |
| Extensions and user functions | Explicit implementation/dependency/security design before admission; volatility declarations alone are insufficient |

Computed aggregate arguments, computed relational keys, joins/subqueries/windows
and dependency invalidation belong to M4. Supporting a function in the registry
does not automatically enable every relational position. Live-query delivery
remains outside the project scope.
