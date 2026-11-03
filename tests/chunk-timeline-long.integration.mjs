import assert from 'node:assert/strict';
import { mkdtemp, open, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { planChunkMedia } from '../server/storage/chunk-media-plan.mjs';
import { extractTimelineChunk } from '../server/storage/chunk-extraction.mjs';
import { mergeChunkSegments } from '../server/storage/chunk-timestamps.mjs';
const dir = await mkdtemp(join(tmpdir(),'chunk-long-test-'));
const input = join(dir,'long.wav');
const durationMs = 10802000, sampleRate = 16000, totalSamples = durationMs*sampleRate/1000;
const anchors = [600000,1800000,3600000,7200000,10800000];
let passed = false;
try {
  const handle = await open(input,'w');
  try {
    const header=Buffer.alloc(44); header.write('RIFF',0); header.writeUInt32LE(36+totalSamples*2,4); header.write('WAVEfmt ',8);
    header.writeUInt32LE(16,16); header.writeUInt16LE(1,20); header.writeUInt16LE(1,22); header.writeUInt32LE(sampleRate,24);
    header.writeUInt32LE(sampleRate*2,28); header.writeUInt16LE(2,32); header.writeUInt16LE(16,34); header.write('data',36); header.writeUInt32LE(totalSamples*2,40);
    await handle.write(header,0,header.length,0); await handle.truncate(44+totalSamples*2);
    const pulse=Buffer.alloc(320*2); for(let i=0;i<320;i++) pulse.writeInt16LE(30000,i*2);
    for(const anchor of anchors) await handle.write(pulse,0,pulse.length,44+anchor*sampleRate/1000*2);
  } finally { await handle.close(); }
  const video = join(dir,'long.mkv');
  const mux = spawnSync('ffmpeg',['-hide_banner','-loglevel','error','-y',
    '-f','lavfi','-i','color=c=black:s=32x32:r=1/60','-i',input,
    '-map','0:v:0','-map','1:a:0','-c:v','mpeg4','-c:a','copy','-t','10802',video]);
  assert.equal(mux.status,0);
  const halfHour = join(dir,'half-hour.mkv');
  const shortMux = spawnSync('ffmpeg',['-hide_banner','-loglevel','error','-y','-i',video,'-t','1802','-c','copy',halfHour]);
  assert.equal(shortMux.status,0);
  const halfPlan = await planChunkMedia(halfHour,{chunkSeconds:600,overlapSeconds:5});
  assert.ok(halfPlan.durationMs >= 1800000 && halfPlan.durationMs < 1900000);
  const halfChunk = halfPlan.chunks.filter(c => c.startOffsetMs <= 1800000 && c.startOffsetMs+c.durationMs > 1800000).at(-1);
  assert.ok(halfChunk);
  const halfOutput = join(dir,'half-chunk.wav');
  await extractTimelineChunk(halfHour,halfOutput,{formatStartMs:halfPlan.formatStartMs,chunk:halfChunk});
  const halfBytes = await readFile(halfOutput), halfData = halfBytes.indexOf('data')+8;
  let halfSample = -1;
  for(let p=halfData;p+1<halfBytes.length;p+=2) if(halfBytes.readInt16LE(p)>20000) { halfSample=(p-halfData)/2; break; }
  assert.ok(halfSample>=0);
  assert.ok(Math.abs(halfChunk.startOffsetMs+halfSample*1000/sampleRate-1800000)<=1);
  await rm(halfOutput);
  console.log('30-MINUTE VIDEO TIMELINE: PASS');
  const plan = await planChunkMedia(video,{chunkSeconds:600,overlapSeconds:5});
  assert.equal(plan.durationMs,durationMs);
  const offsets=[];
  for(const [i,anchor] of anchors.entries()) {
    const chunk = plan.chunks.filter(c => c.startOffsetMs <= anchor && c.startOffsetMs+c.durationMs > anchor).at(-1);
    assert.ok(chunk,`missing chunk ${i}`);
    const output=join(dir,`part-${i}.wav`);
    await extractTimelineChunk(video,output,{formatStartMs:plan.formatStartMs,chunk});
    const bytes=await readFile(output);
    const seekOutput=join(dir,`seek-${i}.wav`);
    await extractTimelineChunk(video,seekOutput,{formatStartMs:plan.formatStartMs,chunk,seekPrerollMs:2000});
    assert.deepEqual(await readFile(seekOutput),bytes);
    await rm(seekOutput);
    const dataPos=bytes.indexOf('data'); assert.ok(dataPos>=0); const start=dataPos+8;
    let sample=-1;
    for(let p=start;p+1<bytes.length;p+=2) { if(bytes.readInt16LE(p)>20000) {sample=(p-start)/2;break;} }
    assert.ok(sample>=0,`missing marker ${i}`);
    const observed=chunk.startOffsetMs+sample*1000/sampleRate;
    const errorMs=Math.abs(observed-anchor); offsets.push(errorMs);
    assert.ok(errorMs<=1,`anchor ${anchor}: error ${errorMs}ms`);
    const merged=mergeChunkSegments([{...chunk,segments:[{text:'marker',startMs:sample*1000/sampleRate,endMs:sample*1000/sampleRate+20}]}],durationMs);
    assert.ok(Math.abs(merged[0].startMs-anchor)<=1);
    await rm(output);
  }
  console.log(`3-HOUR VIDEO ANCHOR ERROR MS: ${offsets.join(', ')}`);
  console.log('LONG SOURCE PROBE / EXTRACT / MERGE: PASS'); passed=true;
} catch(e) { process.exitCode=1; console.error('LONG TIMELINE TEST FAILED:',e.message); }
finally { await rm(dir,{recursive:true,force:true}); console.log('TEMP FILE CLEANUP: PASS'); }
if(passed) console.log('TOOLNET-SRT LONG TIMELINE TEST PASSED');
