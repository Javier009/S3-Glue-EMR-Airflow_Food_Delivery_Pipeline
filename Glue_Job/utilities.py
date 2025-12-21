import boto3
import re

def _get_s3_client():
    # In Glue, credentials are provided by the job role. Locally you can use a profile.
    try:
        session = boto3.Session(profile_name='AdministratorAccess-978177281350', region_name='us-east-2')
        return session.client('s3')
    except Exception:
        return boto3.client('s3')

def list_partitions_ymd(bucket: str, base_prefix: str = ""):
    """
    Returns a set of (YYYY, MM, DD) for keys like:
    {base_prefix}/year=YYYY/month=MM/day=DD/...
    """
    s3 = _get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    # We scan keys and extract partitions; avoids fragile Delimiter nesting and pagination issues.
    pattern = re.compile(rf"^{re.escape(base_prefix)}year=(\d{{4}})/month=(\d{{2}})/day=(\d{{2}})/")

    parts = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=base_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            m = pattern.match(key)
            if m:
                parts.add((m.group(1), m.group(2), m.group(3)))
    return parts

def partition_has_output(bucket: str, prefix: str) -> bool:
    """
    True if the partition prefix contains at least one parquet file or _SUCCESS marker.
    """
    s3 = _get_s3_client()
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=50)
    for obj in resp.get("Contents", []):
        k = obj["Key"]
        if k.endswith(".parquet") or k.endswith("_SUCCESS"):
            return True
    return False