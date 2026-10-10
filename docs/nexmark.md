# PostgreSQL Nexmark portfolio contract

The target is q0–q22: 23 queries, following the
[upstream Nexmark portfolio](https://github.com/nexmark/nexmark).
This is a target contract, not a claim that the compiler executes these queries.
The local Feldera SQL templates are a comparison source; they omit q6 and q11
and contain syntax and temporal choices that need explicit PostgreSQL adaptations.
The compiler currently supports the narrower shapes in [sql-compiler.md](sql-compiler.md).

## Source and time contract

Use ordinary PostgreSQL person, auction and bid relations with stable source
primary keys and complete old/new tuple images. Preserve duplicate visible rows
as a weighted bag. A bid's source identity may break ranking ties, but must not
replace generic full-tuple identity or make projected output unique.

Event timestamps are explicit source data. Processing timestamps are separate
recorded source data, assigned once when an event is ingested; replay never calls
now() to regenerate them. Auction closure needs an explicit persisted clock
relation. Clock updates are ordinary source deltas, delivered with all other
changes from their complete committed transaction. Neither LSN nor logical tick
is a timestamp. PostgreSQL oracle sessions use a fixed UTC timezone and declared
collation. Timezone-dependent terminal functions require a separately bound
execution context before they can be supported.

Outputs remain revisable after late inserts, updates and deletes. There is no
watermark-based eviction or finalization policy in the current CDC contract.
Window membership is half-open [start,end). Session connectivity must state its
gap equality convention; the initial target joins consecutive events whose gap
is strictly less than ten seconds. A session starts at its first event and ends
ten seconds after its last event. NULL timestamps follow the SQL formulation: date_bin grouping retains a NULL
group unless WHERE excludes it. Expansion must likewise preserve the SQL rows,
including any NULL bucket values.

## PostgreSQL formulations for window targets

Every target must be expressible as PostgreSQL SQL before compiler support is
claimed. Flink-style TUMBLE/HOP/SESSION syntax is not part of the frontend.
PostgreSQL's OVER clause supports ranking and ordered window aggregates. Fixed
buckets use date_bin with an explicit origin; a ten-second hopping window advancing
every two seconds expands each non-NULL event over generate_series(0,4), with start
`date_bin('2 seconds', event_time, origin) - n * interval '2 seconds'`.

Sessions use lag(event_time) partitioned by bidder and ordered by event_time plus
stable source identity. A gap >= ten seconds starts a new session; a running SUM
of those boundaries with ROWS UNBOUNDED PRECEDING supplies the session group.
An outer grouping emits first event, last event + ten seconds, and count. This is
ordinary PostgreSQL relational SQL and remains revisable under source changes.
It does not imply native streaming finalization, watermarks or state expiry.

References: [PostgreSQL window functions](https://www.postgresql.org/docs/17/functions-window.html),
[date_bin](https://www.postgresql.org/docs/17/functions-datetime.html), and
[generate_series](https://www.postgresql.org/docs/17/functions-srf.html).
These forms are target definitions; current compilation still rejects their
unsupported relational nodes before registration.

## Query targets

| Query | PostgreSQL adaptation and required behavior |
| --- | --- |
| q0 | Project bid fields, preserving native timestamp values and multiplicity. |
| q1 | Exact numeric multiplication by decimal 0.908; no floating-point approximation. |
| q2 | Integral modulo selection; PostgreSQL signed remainder and NULL qualification. |
| q3 | Auction/person inner join, text state selection and category predicate. |
| q4 | Per-auction maximum eligible bid, followed by per-category exact average. |
| q5 | Expand each bid into five ten-second windows spaced two seconds apart; count by auction/window and retain all maximum ties. |
| q6 | Eligible winning price per closed auction; per seller average of exactly the latest ten closed auctions, ordered by expiry then stable auction identity. Explicit clock changes trigger closure. |
| q7 | All bids tied for maximum price in each ten-second event-time bucket. |
| q8 | Distinct person and seller ten-second memberships, joined on person/seller and bucket. |
| q9 | One eligible winning bid per auction, ordered by price descending, event time ascending, then stable bid identity. |
| q10 | Weighted bid output with UTC date/minute partition keys; filesystem delivery is outside this engine slice. |
| q11 | Bidder sessions from ordered event-time connectivity; insertion can merge sessions and deletion can split them. Emit count and explicit session boundaries. |
| q12 | Bidder counts in ten-second buckets of recorded processing time; retain its distinction from q11/event time. |
| q13 | Left temporal predecessor lookup by modulo auction key against side input, using recorded processing time; choose latest eligible time then stable side-input identity. Historical changes revise existing matches. |
| q14 | Exact numeric conversion, numeric range filter, UTC time-of-day CASE and Unicode character count via length/replace. |
| q15 | Daily filtered counts and distinct bidder/auction counts; COUNT DISTINCT ignores NULL. |
| q16 | q15 grouped also by channel, with minute formatting after the group's maximum timestamp. |
| q17 | Auction/day filtered counts, retractable min/max, exact average and sum. |
| q18 | Latest bid per bidder/auction, with stable bid identity breaking equal-time ties. |
| q19 | Top ten bids per auction, deterministic price/time/identity ordering; retain bag semantics under ranking. |
| q20 | Bid/auction inner join and broad column projection with category selection. |
| q21 | Explicit deterministic text collation/case behavior, CASE and PostgreSQL regular-expression capture for channel id. |
| q22 | URL directory extraction using PostgreSQL string splitting; absent components yield NULL to preserve the declared Nexmark extraction semantics. |

q13 predecessor eligibility is side-input time <= recorded bid processing time,
with time descending then stable side-input identity descending. Ranking selects
source events using their stable identity; afterward, visible full-tuple weights
sum selected events rather than deduplicating coincident projected tuples.

Eligible auction bids use [auction start,auction expiry). This is an explicit
boundary adaptation from templates using inclusive BETWEEN. q6 counts only
auctions with eligible non-NULL winning prices, and closure is clock >= expiry.
Averages use PostgreSQL numeric division semantics; the current engine's i64
SUM statistics are not sufficient to claim these queries.

## Execution design

Lower parsed PostgreSQL AST into typed reusable relational nodes, not query-number
callbacks. Required nodes include scalar project/filter, join, grouped aggregates,
distinct, ordered partition selection, temporal expansion and session partitioning.
The existing typed circuit scheduler supplies synchronized delta ticks; every new
stateful node needs explicit maintained state, checkpoint codecs and runtime lowering.

Use weighted indexes for distinct/min/max and count/sum sufficient statistics for
averages. Ranking, temporal predecessor selection and sessions may initially
recompute affected partitions from maintained ordered bags. Their cost depends on
the changed partitions, and must be measured with large and skewed partitions.
Whole-database snapshot recomputation is an independent oracle, not the production
incremental implementation. Incremental joins retain all three delta terms.

Terminal deterministic tuple-local expressions can execute in PostgreSQL inside
publication. Expressions influencing qualification, joins, groups or order need
Rust implementations at those operator boundaries. Pin built-in signatures,
function implementations, codecs and relevant environmental semantics in durable
identity. Volatility declarations alone do not establish replay safety for arbitrary
user-defined functions. Clock-dependent semantics need explicit inputs.

## Qualification and storage work

Each query needs compiled execution compared after each committed change with
PostgreSQL and an independent full-state memory oracle, including NULLs, duplicate
projections, ties, simultaneous source changes and cold restart. Temporal histories
also need out-of-order changes, explicit clock advances, exact boundary timestamps
and session merge/split. PostgreSQL-compatible SQL files alone do not establish
compiler or runtime support.

Measure cold and warm object GETs/bytes, repeated probes, cache residency, root/block
counts, compaction writes and skewed partition work against identical histories.
Choose storage changes from those measurements and prove invariant output deltas
across physical layouts, compaction and restart. Preserve object PUT followed by
atomic PG membership/result/progress COMMIT and then ACK. Historical time state
cannot be discarded while arbitrary source retractions remain permitted.


## Qualified session primitives

Explicit q11-style PostgreSQL sessions now execute through composed LAG, lazy
CASE, running SUM, temporal MIN/MAX and fixed-time boundary addition. Qualification
uses bidder-like integral keys and native timestamps, with gap >= ten seconds
starting a new session. Late bridge insertions merge components; deleting bridges
splits them. The source WHERE explicitly excludes NULL event times. Counts and
boundaries are revised after each complete committed source delta and restore
through the same durable publication protocol. Local/instant, NULL key, infinity,
exact gap, microsecond, update/delete and cold-restart cases compare with PostgreSQL
and an independent direct-connectivity oracle. This qualifies these primitives
and this explicit session formulation, not all q0–q22 target queries above.

Integral grouped COUNT DISTINCT is also qualified, including FILTER and nested
aggregate/window consumption. The remaining portfolio includes broader native/terminal numeric
and text expressions, general joins with grouped/ordered derived stages, and the
explicit clock-driven auction-closure formulations. These remain compiler work;
no unsupported query is approximated or accepted as an executable placeholder.
