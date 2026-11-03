import test from 'node:test';
import assert from 'node:assert/strict';
import { mergeChunkSegments } from '../server/storage/chunk-timestamps.mjs';
import { toSrt } from '../dist/index.js';

test('Source offsets preserve absolute timing at 10m, 30m, 1h, 2h and 3h', () => {
  const anchors = [600000, 1800000, 3600000, 7200000, 10800000];
  const chunks = anchors.map((anchor,index) => ({ index, startOffsetMs:anchor-120, durationMs:1000,
    overlapMs:0, segments:[{ text:`anchor-${index}`, startMs:120, endMs:220 }] }));
  const actual = mergeChunkSegments(chunks, 10801000);
  assert.deepEqual(actual.map(segment => segment.startMs), anchors);
  assert.deepEqual(actual.map(segment => segment.endMs), anchors.map(value => value + 100));
  const srt = toSrt(actual);
  assert.match(srt, /^1\n00:10:00,000 --> 00:10:00,100/m);
  assert.match(srt, /5\n03:00:00,000 --> 03:00:00,100/);
});

test('Only matching text with substantial overlap across chunks is deduplicated', () => {
  const result = mergeChunkSegments([
    { index:0, startOffsetMs:0, durationMs:11000, overlapMs:0, segments:[
      { text:'Xin chào',startMs:9500,endMs:10300 },
      { text:'Khác',startMs:9600,endMs:10000 },
    ] },
    { index:1, startOffsetMs:9000, durationMs:11000, overlapMs:2000, segments:[
      { text:'  XIN CHÀO ',startMs:500,endMs:1300 },
      { text:'Lời khác',startMs:600,endMs:1000 },
    ] },
  ],20000);
  assert.equal(result.length,3);
  assert.deepEqual(new Set(result.map(x=>x.text)),new Set(['Xin chào','Khác','Lời khác']));
});

test('Invalid and out of range timestamps are safely bounded', () => {
  const result = mergeChunkSegments([{ index:0, startOffsetMs:0, durationMs:1000, overlapMs:0, segments:[
    { text:'negative',startMs:-300,endMs:100 }, { text:'over',startMs:950,endMs:4000 },
    { text:'invalid',startMs:800,endMs:100 }, { text:'nan',startMs:NaN,endMs:2 },
  ] }],1000);
  assert.deepEqual(result,[{text:'negative',startMs:0,endMs:100},{text:'over',startMs:950,endMs:1000}]);
});
