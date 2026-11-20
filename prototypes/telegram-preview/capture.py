"""Fetch six named R2 objects for a private one-shot replay, never listing or writing R2."""
import json
import os
from pathlib import Path
import shutil
import boto3

out = Path(os.environ['SHADOW_INPUT'])
out.mkdir(mode=0o700, exist_ok=True)
s3 = boto3.client('s3', region_name='auto',
    endpoint_url='https://dfdb02b4139b31c0ab4456a50eb371a2.r2.cloudflarestorage.com',
    aws_access_key_id=os.environ['R2_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['R2_SECRET_ACCESS_KEY'])
for key in ['publisher/v7.1/state.json'] + [f'arab_quote_day{i}.json' for i in range(1, 6)]:
    result = s3.get_object(Bucket='arabsniper-gold', Key=key)
    dest = out / Path(key).name
    with result['Body'] as body, dest.open('wb') as target:
        shutil.copyfileobj(body, target, length=1 << 20)
    if dest.stat().st_size != result['ContentLength']:
        raise ValueError('Incomplete R2 download')
    print(key, dest.stat().st_size, flush=True)
