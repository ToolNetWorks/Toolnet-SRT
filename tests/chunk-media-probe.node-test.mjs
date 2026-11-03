import test from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtemp, rm, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { probeChunkMedia } from '../server/storage/chunk-media-probe.mjs';
import { planChunkMedia } from '../server/storage/chunk-media-plan.mjs';
import { extractTimelineChunk } from '../server/storage/chunk-extraction.mjs';

test('FFprobe retains the audio stream delay relative to the source', async () => {
  const dir = await mkdtemp(join(tmpdir(),'chunk-probe-'));
  try {
    const path = join(dir,'delayed.mp4');
    const ffmpeg = spawnSync('ffmpeg',['-hide_banner','-loglevel','error','-y',
      '-f','lavfi','-i','color=c=black:s=64x64:r=5:d=8', '-itsoffset','2',
      '-f','lavfi','-i','sine=frequency=500:duration=4', '-c:v','mpeg4','-c:a','aac',path]);
    assert.equal(ffmpeg.status,0);
    const info = await probeChunkMedia(path);
    assert.equal(info.formatStartMs,0);
    assert.ok(Math.abs(info.audioStartMs-2000) < 30);
    assert.equal(info.durationMs,8000);
    const plan = await planChunkMedia(path,{ chunkSeconds:3, overlapSeconds:1 });
    assert.ok(plan.chunks.length >= 2);
    assert.ok(Math.abs(plan.chunks[0].startOffsetMs - info.audioStartMs) < 30);
    assert.ok(Math.abs((plan.chunks[1].startOffsetMs-plan.chunks[0].startOffsetMs)-2000) < 30);
    const last = plan.chunks.at(-1);
    assert.ok(last.startOffsetMs + last.durationMs < 6100);
    const output = join(dir,'chunk.wav');
    await extractTimelineChunk(path,output,{formatStartMs:plan.formatStartMs,chunk:plan.chunks[1]});
    const seekOutput = join(dir,'chunk-seek.wav');
    await extractTimelineChunk(path,seekOutput,{formatStartMs:plan.formatStartMs,chunk:plan.chunks[1],seekPrerollMs:2000});
    assert.deepEqual(await readFile(seekOutput),await readFile(output));
    const actual = await probeChunkMedia(output);
    assert.ok(actual.durationMs > 1000 && actual.durationMs < 3100);
    const shifted = join(dir,'nonzero-start.mp4');
    const remux = spawnSync('ffmpeg',['-hide_banner','-loglevel','error','-y','-i',path,
      '-c','copy','-output_ts_offset','10',shifted]);
    assert.equal(remux.status,0);
    const shiftedPlan = await planChunkMedia(shifted,{ chunkSeconds:3, overlapSeconds:1 });
    assert.equal(shiftedPlan.formatStartMs,10000);
    assert.deepEqual(shiftedPlan.chunks.map(x => x.startOffsetMs),plan.chunks.map(x => x.startOffsetMs));
    const shiftedChunk = join(dir,'shifted.wav');
    await extractTimelineChunk(shifted,shiftedChunk,{formatStartMs:shiftedPlan.formatStartMs,chunk:shiftedPlan.chunks[1]});
    const shiftedSeek = join(dir,'shifted-seek.wav');
    await extractTimelineChunk(shifted,shiftedSeek,{formatStartMs:shiftedPlan.formatStartMs,chunk:shiftedPlan.chunks[1],seekPrerollMs:2000});
    assert.deepEqual(await readFile(shiftedSeek),await readFile(shiftedChunk));
    const shiftedInfo = await probeChunkMedia(shiftedChunk);
    assert.ok(Math.abs(shiftedInfo.durationMs - actual.durationMs) <= 30);
    const variable = join(dir,'variable-rate.mp4');
    const vfr = spawnSync('ffmpeg',['-hide_banner','-loglevel','error','-y','-i',path,
      '-vf','select=not(mod(n\\,2))+not(mod(n\\,3))','-fps_mode','vfr','-c:v','mpeg4','-c:a','copy',variable]);
    assert.equal(vfr.status,0);
    const vfrProbe = spawnSync('ffprobe',['-v','error','-select_streams','v:0','-show_entries',
      'stream=r_frame_rate,avg_frame_rate','-of','json',variable],{encoding:'utf8'});
    const videoStream = JSON.parse(vfrProbe.stdout).streams[0];
    assert.notEqual(videoStream.r_frame_rate, videoStream.avg_frame_rate);
    const variablePlan = await planChunkMedia(variable,{ chunkSeconds:3, overlapSeconds:1 });
    assert.deepEqual(variablePlan.chunks.map(x => x.startOffsetMs),plan.chunks.map(x => x.startOffsetMs));
    const vfrNormal=join(dir,'vfr-normal.wav'), vfrSeek=join(dir,'vfr-seek.wav');
    await extractTimelineChunk(variable,vfrNormal,{formatStartMs:variablePlan.formatStartMs,chunk:variablePlan.chunks[1]});
    await extractTimelineChunk(variable,vfrSeek,{formatStartMs:variablePlan.formatStartMs,chunk:variablePlan.chunks[1],seekPrerollMs:2000});
    assert.deepEqual(await readFile(vfrSeek),await readFile(vfrNormal));
  } finally { await rm(dir,{recursive:true,force:true}); }
});

test('Probe handles invalid input without exposing filenames', async () => {
  await assert.rejects(probeChunkMedia('/does-not-exist/private-name.mp4'), { message:'MEDIA_PROBE_FAILED' });
});
