// `substituteKey` — substring-substitute `{name}` placeholders in a
// key template. Used by the planner (to derive shard keys for fetch
// segments) and gap-discovery (to derive expected shard keys).
export function substituteKey(template, values) {
    return template.replace(/\{(\w+)\}/g, (_, name) => {
        if (!(name in values)) {
            throw new Error(`substituteKey: missing value for {${name}}`);
        }
        return String(values[name]);
    });
}
//# sourceMappingURL=keys.js.map