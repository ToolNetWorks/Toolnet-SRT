import assert from 'node:assert/strict';
import pg from 'pg';
import { S3Client, ListObjectsV2Command, HeadObjectCommand } from '@aws-sdk/client-s3';

const schema = process.env.TEST_SERVER_SCHEMA;
const prefix = process.env.TEST_STAGING_R2_PREFIX;
const base = process.argv[2];
if (!/^srt_http_server_[a-f0-9]{12}$/.test(schema ?? '') ||
    !/^staging\/toolnet-srt\/[a-f0-9]{12}\/$/.test(prefix ?? '') ||
    !/^http:\/\/127\.0\.0\.1:\d+$/.test(base ?? '')) throw Error('INVALID_STAGING_CONFIG');
const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
const r2 = new S3Client({ region:'auto', endpoint:`https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: { accessKeyId:process.env.R2_ACCESS_KEY_ID, secretAccessKey:process.env.R2_SECRET_ACCESS_KEY } });
try {
  const jobs = (await pool.query(`SELECT id, status, source_key, result_key FROM "${schema}".toolnet_srt_jobs ORDER BY created_at`)).rows;
  assert.equal(jobs.length, 2); assert.ok(jobs.every(job => job.status === 'completed'));
  const objects = await r2.send(new ListObjectsV2Command({ Bucket:process.env.R2_BUCKET, Prefix:prefix }));
  assert.ok((objects.Contents ?? []).length >= 4);
  assert.ok(objects.Contents.every(object => object.Key.startsWith(prefix)));
  for (const [index,job] of jobs.entries()) {
    for (const key of [job.source_key,job.result_key]) {
      assert.ok(key); await r2.send(new HeadObjectCommand({ Bucket:process.env.R2_BUCKET, Key:prefix+key }));
    }
    const response = await fetch(`${base}/v1/jobs/${job.id}/result`, { headers:{ 'X-API-Key':process.env.TOOLNET_SRT_API_KEY } });
    assert.equal(response.status,200); const text=await response.text(); let previous=-1, number=0;
    for (const block of text.trim().split(/\r?\n\s*\r?\n/)) {
      const lines=block.split(/\r?\n/), m=/^(\d+):(\d\d):(\d\d),(\d{3}) --> (\d+):(\d\d):(\d\d),(\d{3})$/.exec(lines[1] ?? '');
      assert.ok(m); assert.equal(Number(lines[0]),++number);
      const ms=i=>((Number(m[i])*60+Number(m[i+1]))*60+Number(m[i+2]))*1000+Number(m[i+3]);
      const start=ms(1),end=ms(5); assert.ok(start>=previous && end>start); if(index===0) assert.ok(end<=300069); previous=start;
    }
    assert.ok(number>0);
  }
  console.log('STAGING R2 PREFIX AND SRT TIMELINE: PASS');
} finally { r2.destroy(); await pool.end(); }
