"""Cloudflare-side components: the R2 shard store and the D1 shard index.

Needs `CLOUDFLARE_API_TOKEN` (Read on D1 + Workers R2 Storage is enough for
`preview`) and a `cloudflareAccountId` config value."""
import pulumi
from pyrmts_pulumi import R2ShardStore, ShardIndex

cfg = pulumi.Config()
account_id = cfg.require('cloudflareAccountId')

store = R2ShardStore('store', account_id=account_id)
index = ShardIndex('index', account_id=account_id)

pulumi.export('bucket', store.bucket)
pulumi.export('d1_database_id', index.database_id)
# Hand the worker its binding instead of pasting the id into wrangler.toml.
pulumi.export('wrangler_binding', index.wrangler_binding())
