// `substituteKey` — substring-substitute `{name}` placeholders in a
// key template. Used by the planner (to derive shard keys for fetch
// segments) and gap-discovery (to derive expected shard keys).
import { formatPeriod, parseDuration } from './axis.js';
export function substituteKey(template, values) {
    return template.replace(/\{(\w+)\}/g, (_, name) => {
        if (!(name in values)) {
            throw new Error(`substituteKey: missing value for {${name}}`);
        }
        return String(values[name]);
    });
}
// Substitute the pyramid's `keyTemplate` for one shard. `filter` supplies
// values for any extra `{dim_name}` placeholders (e.g. `{device_id}` in an
// awair-style multi-tenant layout). Twin of Python
// `pyrmts_engine.materialize.shard_key`.
export function shardKey(pyramid, tierName, shardDur, periodStart, filter = {}) {
    const span = parseDuration(shardDur);
    return substituteKey(pyramid.keyTemplate, {
        ...filter,
        tier: tierName,
        shard: shardDur,
        period: formatPeriod(periodStart, span),
    });
}
//# sourceMappingURL=keys.js.map