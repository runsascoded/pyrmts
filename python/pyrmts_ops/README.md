# pyrmts-ops

Ops layer for pyrmts pyramids (`specs/pyrmts-ops-adoption.md` phase 3): the fan-out rebuild driver (single-gap Lambda invocations with scaffold layering + build-progress docs), registry-driven GC, the Lambda bundle/deploy skeleton, and the generic handler entry. Consumers supply a pyramid config, an ingester, and bindings; this package owns the orchestration shapes.
