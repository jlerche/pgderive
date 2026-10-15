Vendored crates.io lz4-sys 1.11.1+lz4-1.10.0, licensed MIT (LICENSE).
Changes: build.rs defines XXH_NAMESPACE=PGDERIVE_LZ4_.
Foyer transitively links LZ4, whose bundled xxHash otherwise collides with the
bundled xxHash exported by pg_query. Namespacing both LZ4's internal callers and
its xxhash.c definitions avoids duplicate symbols without linker overrides or
changing either algorithm. The Rust WASI cfg uses the current target_os spelling instead of the
obsolete target_env=wasi condition, avoiding unexpected-cfg warnings on the pinned
toolchain. Upstream examples, tools, build systems and tests unused by this
crate build are omitted; the bundled C library sources and licenses are retained.
