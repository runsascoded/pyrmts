from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable


class MemStorage:
    def __init__(self, data: dict[str, bytes] | None = None) -> None:
        self._data: dict[str, bytes] = dict(data) if data else {}

    def head(self, key: str) -> dict | None:
        b = self._data.get(key)
        if b is None: return None
        return {'size': len(b)}

    def get(self, key: str) -> bytes | None:
        return self._data.get(key)

    def put(self, key: str, data: bytes) -> None:
        self._data[key] = data

    def list(self, prefix: str) -> Iterable[str]:
        for k in sorted(self._data):
            if k.startswith(prefix):
                yield k


class FsStorage:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def _path(self, key: str) -> Path:
        return self.root / key

    def head(self, key: str) -> dict | None:
        p = self._path(key)
        if not p.exists(): return None
        return {'size': p.stat().st_size}

    def get(self, key: str) -> bytes | None:
        p = self._path(key)
        if not p.exists(): return None
        return p.read_bytes()

    def put(self, key: str, data: bytes) -> None:
        p = self._path(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)

    def list(self, prefix: str) -> Iterable[str]:
        prefix_path = self.root / prefix
        if prefix_path.is_dir():
            base = prefix_path
            rel_prefix = prefix
        else:
            base = prefix_path.parent
            rel_prefix = prefix
        if not base.exists(): return
        for p in sorted(base.rglob('*')):
            if not p.is_file(): continue
            rel = str(p.relative_to(self.root))
            if rel.startswith(rel_prefix):
                yield rel


class S3Storage:
    """S3-compatible storage (works with AWS S3, Cloudflare R2, etc.).

    Lazy-imports boto3 so the dependency is opt-in (install `pyrmts[s3]`).
    """

    def __init__(
        self,
        bucket: str,
        prefix: str = '',
        endpoint_url: str | None = None,
        region_name: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
    ) -> None:
        import boto3  # type: ignore[import-untyped]
        self.bucket = bucket
        self.prefix = prefix.rstrip('/') + '/' if prefix and not prefix.endswith('/') else prefix
        self._client = boto3.client(
            's3',
            endpoint_url=endpoint_url or os.environ.get('AWS_ENDPOINT_URL') or os.environ.get('R2_ENDPOINT_URL'),
            region_name=region_name or os.environ.get('AWS_REGION') or 'auto',
            aws_access_key_id=aws_access_key_id or os.environ.get('AWS_ACCESS_KEY_ID') or os.environ.get('R2_ACCESS_KEY_ID'),
            aws_secret_access_key=aws_secret_access_key or os.environ.get('AWS_SECRET_ACCESS_KEY') or os.environ.get('R2_SECRET_ACCESS_KEY'),
        )

    def _key(self, key: str) -> str:
        return f"{self.prefix}{key}"

    def head(self, key: str) -> dict | None:
        try:
            r = self._client.head_object(Bucket=self.bucket, Key=self._key(key))
        except self._client.exceptions.ClientError as e:
            if e.response['Error']['Code'] in ('404', 'NoSuchKey', 'NotFound'):
                return None
            raise
        return {'size': r['ContentLength'], 'etag': r.get('ETag', '').strip('"')}

    def get(self, key: str) -> bytes | None:
        try:
            r = self._client.get_object(Bucket=self.bucket, Key=self._key(key))
        except self._client.exceptions.NoSuchKey:
            return None
        except self._client.exceptions.ClientError as e:
            if e.response['Error']['Code'] in ('404', 'NoSuchKey'):
                return None
            raise
        return r['Body'].read()

    def put(self, key: str, data: bytes) -> None:
        self._client.put_object(Bucket=self.bucket, Key=self._key(key), Body=data)

    def list(self, prefix: str):
        paginator = self._client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self._key(prefix)):
            for obj in page.get('Contents', []):
                full = obj['Key']
                if self.prefix and full.startswith(self.prefix):
                    yield full[len(self.prefix):]
                else:
                    yield full
