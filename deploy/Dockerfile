# Runtime files and their hashes are assembled by scripts/build_container.py.
# FROM scratch avoids a mutable distribution tag and package repository.
FROM scratch
COPY rootfs/ /
USER 10001:10001
ENV TMPDIR=/var/lib/pgderive/scratch
STOPSIGNAL SIGINT
ENTRYPOINT ["/usr/local/bin/pgderive"]
