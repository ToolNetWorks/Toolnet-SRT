import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { transcribeCheckpointed } from '../server/storage/chunk-transcription.mjs';

test('Retry transcribes only the failed chunk and preserves absolute offsets', async () => {
  const directory = await mkdtemp(join(tmpdir(),'chunk-checkpoint-unit-'));
  const chunks=[{index:0,startOffsetMs:0,durationMs:1000,overlapMs:0}, {index:1,startOffsetMs:1000,durationMs:1000,overlapMs:0}];
  const rows=chunks.map(chunk=>({status:'pending',segments:null,chunk_index:chunk.index,attempt_count:0}));
  const calls=[], progress=[], timings=[]; let fail=true;
  const checkpoints = {
    async initialize() { return rows.map(row=>({...row})); },
    async claim(_lease,index) { calls.push(`claim-${index}`); rows[index].status='processing'; rows[index].attempt_count++; return rows[index]; },
    async complete(_lease,index,segments) { rows[index].status='completed'; rows[index].segments=segments; return rows[index]; },
    async fail(_lease,index) { rows[index].status='failed'; return rows[index]; },
  };
  const controller = new AbortController();
  const options = { input:'mock',directory,lease:{},checkpoints,signal:controller.signal,
    chunkMaxAttempts:3, ensureOwnership:async()=>{},onProgress:async(...args)=>progress.push(args),onTiming:event=>timings.push(event),
    plan:async()=>({formatStartMs:0,durationMs:2000,chunks}),
    extract:async(_input,path)=>writeFile(path,'audio'),
    transcribe:async (path,_signal,context)=>{ const index=Number(path.match(/chunk-(\d+)/)[1]); assert.equal(context.chunkIndex,index); calls.push(`asr-${index}`);
      if(index===1 && fail) { fail=false; controller.abort(); throw Error('temporary'); } return [{text:`chunk-${index}`,startMs:100,endMs:200}]; },
  };
  try {
    await assert.rejects(transcribeCheckpointed(options),/temporary/);
    assert.equal(rows[0].status,'completed'); assert.equal(rows[1].status,'processing');
    options.signal = new AbortController().signal;
    const merged=await transcribeCheckpointed(options);
    assert.equal(calls.filter(x=>x==='asr-0').length,1); assert.equal(calls.filter(x=>x==='asr-1').length,2);
    assert.deepEqual(merged.map(x=>x.startMs),[100,1100]); assert.equal(progress.at(-1)[2],95);
    assert.ok(['probe_and_plan','extract','asr','checkpoint_complete','merge'].every(name=>timings.some(t=>t.phase===name && t.durationMs>=0)));
  } finally { await rm(directory,{recursive:true,force:true}); }
});

test('A transient ASR failure retries the same chunk inside its lease', async () => {
  const directory=await mkdtemp(join(tmpdir(),'chunk-retry-')); let attempts=0, failures=0;
  const chunk={index:0,startOffsetMs:0,durationMs:1000,overlapMs:0};
  try {
    const result=await transcribeCheckpointed({input:'mock',directory,lease:{},signal:new AbortController().signal,
      ensureOwnership:async()=>{},onProgress:async()=>{}, plan:async()=>({formatStartMs:0,durationMs:1000,chunks:[chunk]}),
      extract:async(_input,path)=>writeFile(path,'audio'),
      transcribe:async()=>{if(attempts===1)throw Error('temporary');return [{text:'ok',startMs:20,endMs:40}];},
      checkpoints:{async initialize(){return [{status:'pending',attempt_count:0}];},
        async claim(){attempts++;return {attempt_count:attempts};}, async fail(){failures++;return {};},
        async complete(_lease,_index,segments){return {segments};}},
    });
    assert.equal(attempts,2);assert.equal(failures,1);assert.equal(result[0].text,'ok');
  } finally { await rm(directory,{recursive:true,force:true}); }
});

test('A provider 429 respects Retry-After before retrying its chunk', async () => {
  const directory=await mkdtemp(join(tmpdir(),'chunk-rate-limit-')); let attempts=0, delay=0;
  const chunk={index:0,startOffsetMs:0,durationMs:1000,overlapMs:0};
  try {
    await transcribeCheckpointed({input:'mock',directory,lease:{},signal:new AbortController().signal,
      ensureOwnership:async()=>{},onProgress:async()=>{},plan:async()=>({formatStartMs:0,durationMs:1000,chunks:[chunk]}),
      extract:async(_input,path)=>writeFile(path,'audio'),retryWait:async ms=>{delay=ms;},
      transcribe:async()=>{if(attempts===1)throw Object.assign(new Error('rate limited'),{status:429,retryAfterMs:5000});return [{text:'ok',startMs:20,endMs:40}];},
      checkpoints:{async initialize(){return [{status:'pending',attempt_count:0}];},
        async claim(){return {attempt_count:++attempts};},async fail(){return {};},async complete(_lease,_index,segments){return {segments};}},
    });
    assert.equal(attempts,2);assert.ok(delay>=5000);
  } finally { await rm(directory,{recursive:true,force:true}); }
});

test('Two chunks run concurrently but checkpoints merge in source order', async () => {
  const directory=await mkdtemp(join(tmpdir(),'chunk-parallel-'));
  const chunks=Array.from({length:3},(_,index)=>({index,startOffsetMs:index*1000,durationMs:1000,overlapMs:0}));
  const rows=chunks.map(c=>({chunk_index:c.index,status:'pending',attempt_count:0,segments:null}));
  let active=0,peak=0;const progress=[];
  try {
    const result=await transcribeCheckpointed({input:'mock',directory,lease:{},chunkConcurrency:2,
      ensureOwnership:async()=>{},onProgress:async(done)=>{progress.push(done);},
      plan:async()=>({formatStartMs:0,durationMs:3000,chunks}),extract:async(_input,path)=>writeFile(path,'audio'),
      transcribe:async(_path,_signal,{chunkIndex})=>{active++;peak=Math.max(peak,active);await new Promise(r=>setTimeout(r,chunkIndex===0?30:5));active--;return [{text:`line-${chunkIndex}`,startMs:10,endMs:20}];},
      checkpoints:{async initialize(){return rows;},async claim(_lease,i){rows[i].status='processing';return {attempt_count:++rows[i].attempt_count};},
        async complete(_lease,i,segments){rows[i].status='completed';rows[i].segments=segments;return rows[i];},async fail(){throw Error('UNEXPECTED_FAILURE');}},
    });
    assert.equal(peak,2);assert.deepEqual(result.map(x=>x.startMs),[10,1010,2010]);
    assert.deepEqual(progress,[0,1,2,3,3]);assert.ok(rows.every(row=>row.attempt_count===1));
  } finally {await rm(directory,{recursive:true,force:true});}
});

test('PCM disk reservation uses actual planned chunk duration and releases after cleanup',async()=>{
  const directory=await mkdtemp(join(tmpdir(),'chunk-disk-'));let reserved=0,released=0;
  const chunk={index:0,startOffsetMs:0,durationMs:61000,overlapMs:0};
  try {
    await transcribeCheckpointed({input:'mock',directory,lease:{},ensureOwnership:async()=>{},onProgress:async()=>{},
      reserveChunkSpace:async bytes=>{reserved=bytes;return ()=>released++;},
      plan:async()=>({formatStartMs:0,durationMs:61000,chunks:[chunk]}),extract:async(_i,path)=>writeFile(path,'audio'),
      transcribe:async()=>[{text:'ok',startMs:0,endMs:10}],
      checkpoints:{async initialize(){return [{status:'pending',attempt_count:0}];},async claim(){return {attempt_count:1};},
        async complete(_l,_i,segments){return {segments};},async fail(){throw Error('UNEXPECTED');}},
    });
    assert.equal(reserved,61000*32+65536);assert.equal(released,1);
  } finally {await rm(directory,{recursive:true,force:true});}
});
