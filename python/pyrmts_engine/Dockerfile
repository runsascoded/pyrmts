# pyrmts-engine base image.
#
# Build from the REPO ROOT (the context needs python/):
#   docker build -f python/pyrmts_engine/Dockerfile -t pyrmts-engine:$(git rev-parse --short HEAD) .
#
# Run shape (AWS Batch containerOverrides supplies the args):
#   pyrmts-engine build s3://bucket/configs/avail.yaml -n avail -r <from>/<to> [...]
#
# Storage + config creds via env: R2_ENDPOINT_URL / R2_ACCESS_KEY_ID /
# R2_SECRET_ACCESS_KEY (or AWS_* equivalents).
#
# App images derive from this (option (b) in specs/engine-batch-packaging.md):
#   FROM pyrmts-engine:<rev>
#   RUN pip install <app>            # brings the app's raw-ingest Source
#   # then submit with the app's own CLI, or the base CLI + --source module:factory

FROM python:3.12-slim

WORKDIR /opt/pyrmts

COPY python/pyrmts/ pyrmts/
COPY python/pyrmts_engine/ pyrmts_engine/

RUN pip install --no-cache-dir './pyrmts[s3]' './pyrmts_engine[batch]'

# Fargate/aarch64 runtime hardening (ctbk first-smoke findings 5-6):
# - faulthandler turns mute SIGSEGVs (exit 139) into tracebacks;
# - the system Arrow pool sidesteps pyarrow's bundled-jemalloc cold-start
#   segv AND routes Arrow buffers through glibc so the engine's post-flush
#   malloc_trim can actually return freed pages to the cgroup;
# - the jemalloc decay conf bounds polars' own retention (py-polars reads
#   the _RJEM_-prefixed env).
# All overridable via job-def / submit env.
ENV PYTHONFAULTHANDLER=1 \
    ARROW_DEFAULT_MEMORY_POOL=system \
    _RJEM_MALLOC_CONF=dirty_decay_ms:1000,muzzy_decay_ms:1000

ENTRYPOINT ["pyrmts-engine"]
