// Standalone provider experiment: no Neon/R2 and no production scheduler.
// Run one benchmark at a time. Stops immediately on any provider error,
// including 429; it does not replay or try to work around provider limits.
import { mkdtemp, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { performance } from 'node:perf_hooks';
import { planChunkMedia } from '../server/storage/chunk-media-plan.mjs';
import { extractTimelineChunk } from '../server/storage/chunk-extraction.mjs';
import { mergeChunkSegments } from '../server/storage/chunk-timestamps.mjs';
import { transcribe } from '../dist/index.js';

const input=process.env.BENCH_AUDIO;
const concurrency=Number(process.env.BENCH_CONCURRENCY ?? '1');
if (!input || ![1,2].includes(concurrency)) throw new Error('INVALID_BENCHMARK_CONFIG');
const directory=await mkdtemp(join(tmpdir(),'toolnet-asr-benchmark-'));
const controller=new AbortController();
const stats={planMs:0,extractMs:0,asrMs:0,mergeMs:0,completed:0,retries:0,peakNodeRssBytes:0,peakChunkBytes:0};
let peakFiles=0, next=0, failed=null;
const start=performance.now();
try {
  let t=performance.now(); const plan=await planChunkMedia(input,{chunkSeconds:600,signal:controller.signal}); stats.planMs=performance.now()-t;
  const results=new Array(plan.chunks.length);
  async function worker() {
    while (!controller.signal.aborted) {
      const index=next++; if(index>=plan.chunks.length)return;
      const output=join(directory,`chunk-${index}.wav`);
      try {
        const extractionStarted=performance.now(); await extractTimelineChunk(input,output,{formatStartMs:plan.formatStartMs,chunk:plan.chunks[index],seekPrerollMs:2000,signal:controller.signal});
        stats.extractMs+=performance.now()-extractionStarted;
        const size=(await stat(output)).size; peakFiles+=size; stats.peakChunkBytes=Math.max(stats.peakChunkBytes,peakFiles);
        const started=performance.now(); let segments;
        for (let attempt=0; ; attempt++) {
          try { segments=await transcribe({input:output,signal:controller.signal,queryTimeoutMs:300000}); break; }
          catch(error) {
            if (controller.signal.aborted || error?.status === 429 || !(error?.status >= 500) || attempt >= 2) throw error;
            stats.retries++;
            await new Promise((resolve,reject)=>{
              const timer=setTimeout(done,1000 * 2 ** attempt + Math.floor(Math.random() * 1000));
              function done(){controller.signal.removeEventListener('abort',abort);resolve();}
              function abort(){clearTimeout(timer);reject(new Error('BENCHMARK_ABORTED'));}
              controller.signal.addEventListener('abort',abort,{once:true});
              if(controller.signal.aborted) abort();
            });
          }
        }
        stats.asrMs+=performance.now()-started;
        peakFiles-=size; results[index]={...plan.chunks[index],segments}; stats.completed++;
        stats.peakNodeRssBytes=Math.max(stats.peakNodeRssBytes,process.memoryUsage().rss);
        console.log(`chunk_completed=${stats.completed}/${plan.chunks.length}`);
      } catch(error) {
        if(!failed){failed={status:error?.status??null,code:error?.status===429?'ASR_RATE_LIMITED':'ASR_BENCHMARK_FAILED',retryAfterMs:error?.retryAfterMs??null};controller.abort();}
      } finally { await rm(output,{force:true}).catch(()=>{}); }
    }
  }
  await Promise.all(Array.from({length:concurrency},()=>worker()));
  if(failed)throw Object.assign(new Error(failed.code),failed);
  t=performance.now();const merged=mergeChunkSegments(results,plan.durationMs);stats.mergeMs=performance.now()-t;
  if(process.env.BENCH_OUTPUT) await writeFile(process.env.BENCH_OUTPUT,JSON.stringify(merged));
  console.log(JSON.stringify({status:'PASS',mode:'standalone_asr',concurrency,wallMs:Math.round(performance.now()-start),chunks:plan.chunks.length,segments:merged.length,...stats}));
} catch(error) {
  process.exitCode=1;
  console.error(JSON.stringify({status:'FAIL',code:error.code??'BENCHMARK_FAILED',httpStatus:error.status??null,retryAfterMs:error.retryAfterMs??null,completed:stats.completed}));
} finally {controller.abort();await rm(directory,{recursive:true,force:true});}
