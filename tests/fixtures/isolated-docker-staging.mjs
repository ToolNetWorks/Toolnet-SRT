// Staging-only adapter. The production server never imports this module.
import './isolated-server-db.mjs';
import { S3Client } from '@aws-sdk/client-s3';

const prefix = process.env.TEST_STAGING_R2_PREFIX;
if (!/^staging\/toolnet-srt\/[a-f0-9]{12}\/$/.test(prefix ?? '')) {
  throw new Error('INVALID_STAGING_R2_PREFIX');
}

const send = S3Client.prototype.send;
S3Client.prototype.send = async function(command, ...options) {
  const input = command?.input;
  const name = command?.constructor?.name;
  if (!input || typeof input !== 'object') throw new Error('INVALID_STAGING_R2_COMMAND');
  const originalKey = input.Key;
  const originalPrefix = input.Prefix;
  if (typeof originalKey === 'string') input.Key = prefix + originalKey;
  else if (typeof originalPrefix === 'string') input.Prefix = prefix + originalPrefix;
  else if (name !== 'HeadBucketCommand') throw new Error('UNSCOPED_STAGING_R2_COMMAND');
  try {
    const result = await send.call(this, command, ...options);
    if (Array.isArray(result?.Contents)) {
      for (const object of result.Contents) {
        if (typeof object.Key === 'string' && object.Key.startsWith(prefix)) object.Key = object.Key.slice(prefix.length);
      }
    }
    return result;
  } finally {
    if (typeof originalKey === 'string') input.Key = originalKey;
    if (typeof originalPrefix === 'string') input.Prefix = originalPrefix;
  }
};
