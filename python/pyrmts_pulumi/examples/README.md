# Examples

Two runnable Pulumi programs, each serving three purposes: a reference implementation to read, a template to copy into your own stack, and a **schema check** — `pulumi preview` is read-only, so running these against a real account verifies the providers still accept what the components declare, which unit tests against `pulumi.runtime.set_mocks` structurally cannot. (That is not hypothetical: the first preview caught `s3.BucketV2` and `BucketLifecycleConfigurationV2` being deprecated in `pulumi-aws` 7.x.)

| | Resources | Credentials |
|---|---|---|
| `aws/` | 23 — store, fill Lambda + 2 schedules, batch engine (spot + on-demand) | the usual AWS chain; `aws:region` config |
| `cloudflare/` | 5 — R2 bucket, D1 database | `CLOUDFLARE_API_TOKEN`, `cloudflareAccountId` config |

Both run with placeholder image URIs and networking, so `preview` needs no setup beyond credentials.

```bash
cd aws && pulumi stack select --create dev && pulumi config set aws:region us-east-1 && pulumi preview
```

**`preview` needs only read permissions**, and is worth scoping that way — a Cloudflare token with `D1:Read` + `Workers R2 Storage:Read` is enough (verified). Reach for write scopes only when you actually intend to `pulumi up`.

Neither example is packaged into the wheel; they exist to be read and copied.
