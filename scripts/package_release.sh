#!/usr/bin/env bash
# Build a host-native package; never install a service or credentials.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ $# != 1 ]]; then
    printf 'usage: scripts/package_release.sh NEW_OUTPUT_DIRECTORY\n' >&2
    exit 2
fi
output=$(realpath -m -- "$1")
# Packaging is host-native and uninstrumented, even inside the coverage gate.
unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS CARGO_BUILD_RUSTFLAGS CARGO_BUILD_TARGET
unset RUSTC RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER RUSTUP_TOOLCHAIN
unset CARGO_BUILD_RUSTC CARGO_BUILD_RUSTC_WRAPPER CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER
while IFS= read -r build_variable; do
    case "$build_variable" in
        CARGO_PROFILE_RELEASE_*|CARGO_TARGET_*) unset "$build_variable" ;;
    esac
done < <(compgen -A variable)
export CARGO_TARGET_DIR="$PWD/target/package-release"
export RUSTFLAGS="" CARGO_ENCODED_RUSTFLAGS=""
package_host=$(rustc -vV | sed -n 's/^host: //p')
test -n "$package_host"
export PGDERIVE_PACKAGE_HOST="$package_host"
# Refuse reuse so a failed build and its logs cannot be overwritten.
mkdir -m 700 -- "$output"
mkdir -- "$output/bin" "$output/examples" "$output/docs"
if [[ "$output" == "$PWD/"* ]] && ! git check-ignore --quiet "$output"; then
    printf 'Output inside the repository must be git-ignored.\n' >&2
    exit 2
fi
source_snapshot() {
    python3 - "$1" <<'SNAPSHOT'
import hashlib,json,os,pathlib,subprocess,sys
root=pathlib.Path.cwd()
paths=set(subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard','-z']).decode().split('\0'))
manifest={'git_head':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
          'git_status':subprocess.check_output(['git','status','--porcelain'],text=True).splitlines(),
          'rustc':subprocess.check_output(['rustc','--version'],text=True).strip(),
          'cargo':subprocess.check_output(['cargo','--version'],text=True).strip(),
          'build':{'profile':'release','target':os.environ['PGDERIVE_PACKAGE_HOST'],'instrumented':False,'target_directory':os.environ['CARGO_TARGET_DIR'],
                   'environment_policy':'host-native; inherited Rust flags, compiler wrappers, target and release profile overrides cleared'},
          'source_sha256':{path:hashlib.sha256((root/path).read_bytes()).hexdigest() for path in sorted(paths) if path and (root/path).is_file()}}
pathlib.Path(sys.argv[1]).write_text(json.dumps(manifest,indent=2)+'\n')
SNAPSHOT
}
source_snapshot "$output/source-before.json"
cargo build --release --locked --target "$package_host" --bin pgderive > "$output/build.log" 2>&1
source_snapshot "$output/source-after.json"
if ! cmp -s "$output/source-before.json" "$output/source-after.json"; then
    printf 'Source tree changed during compilation; package rejected. Evidence retained at %s\n' "$output" >&2
    exit 1
fi
target_directory=$(cargo metadata --locked --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
install -m 755 "$target_directory/$package_host/release/pgderive" "$output/bin/pgderive"
ldd "$output/bin/pgderive" > "$output/runtime-libraries.log"
install -m 644 worker.example.toml "$output/examples/worker.toml"
install -m 644 deploy/pgderive@.service "$output/examples/pgderive@.service"
for document in storage-configuration postgres-security worker-health deployment backup-failover sql-compiler; do
    install -m 644 "docs/$document.md" "$output/docs/$document.md"
done
source_snapshot "$output/source-final.json"
if ! cmp -s "$output/source-before.json" "$output/source-final.json"; then
    printf 'Source tree changed during packaging; package rejected.\n' >&2
    exit 1
fi
python3 - "$output" <<'PY'
import hashlib,json,pathlib,subprocess,sys
output=pathlib.Path(sys.argv[1])
manifest=json.loads((output/'source-before.json').read_text())
manifest['package_sha256']={str(path.relative_to(output)):hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(output.rglob('*')) if path.is_file()}
(output/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
PY
printf 'Built host-native package at %s; inspect manifest.json before installation.\n' "$output"
