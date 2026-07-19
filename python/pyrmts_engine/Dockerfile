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

ENTRYPOINT ["pyrmts-engine"]
