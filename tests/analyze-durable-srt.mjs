// Heuristic comparison only: unpaired cues are not proof of missing speech.
// Detailed text is written only to the caller-specified private output file.
import { readFile, writeFile } from 'node:fs/promises';
const [pathA,pathB,output]=process.argv.slice(2);
if(!pathA||!pathB||!output)throw Error('COMPARISON_INPUT_REQUIRED');
function parse(text){return text.trim().split(/\r?\n\s*\r?\n/).map(block=>{
  const lines=block.split(/\r?\n/),m=/^(\d+):(\d\d):(\d\d),(\d{3}) --> (\d+):(\d\d):(\d\d),(\d{3})$/.exec(lines[1]??'');
  if(!m)throw Error('INVALID_SRT_TIMESTAMP');
  const ms=o=>(((Number(m[o])*60+Number(m[o+1]))*60+Number(m[o+2]))*1000+Number(m[o+3]));
  const text=lines.slice(2).join(' ').trim();
  return {start:ms(1),end:ms(5),text,normalized:text.toLocaleLowerCase().replace(/[^\p{L}\p{N}]+/gu,' ').trim()};
});}
const [a,b]=await Promise.all([readFile(pathA,'utf8').then(parse),readFile(pathB,'utf8').then(parse)]);
const used=new Set(),details=[],starts=[],ends=[];let startIndex=0,exact=0,changed=0;
const nearBoundary=ms=>ms>0 && Math.abs(ms/600000-Math.round(ms/600000))*600000<=5000;
for(const first of a){
  while(startIndex<b.length&&b[startIndex].start<first.start-1000)startIndex++;
  let best=-1,distance=1001;
  for(let i=startIndex;i<b.length&&b[i].start<=first.start+1000;i++){
    const d=Math.abs(b[i].start-first.start);if(!used.has(i)&&d<distance){best=i;distance=d;}
  }
  if(best<0){details.push({type:'unpaired_first',boundary:nearBoundary(first.start),first});continue;}
  used.add(best);const second=b[best];starts.push(distance);ends.push(Math.abs(first.end-second.end));
  if(first.normalized===second.normalized)exact++;else {changed++;details.push({type:'different_text',boundary:nearBoundary(first.start),first,second});}
}
for(let i=0;i<b.length;i++)if(!used.has(i))details.push({type:'unpaired_second',boundary:nearBoundary(b[i].start),second:b[i]});
function adjacentDuplicates(rows){let count=0;for(let i=1;i<rows.length;i++)if(rows[i].normalized===rows[i-1].normalized&&rows[i].start-rows[i-1].end<=1000)count++;return count;}
function p95(x){x.sort((a,b)=>a-b);return x.length?x[Math.ceil(x.length*.95)-1]:null;}
const summary={firstCues:a.length,secondCues:b.length,paired:used.size,pairedSameNormalized:exact,pairedDifferentText:changed,
  firstUnpaired:details.filter(x=>x.type==='unpaired_first').length,secondUnpaired:details.filter(x=>x.type==='unpaired_second').length,
  boundaryDifferences:details.filter(x=>x.boundary).length,firstAdjacentDuplicates:adjacentDuplicates(a),secondAdjacentDuplicates:adjacentDuplicates(b),
  pairedStartP95Ms:p95(starts),pairedEndP95Ms:p95(ends)};
await writeFile(output,JSON.stringify({method:'nearest start within 1000 ms; no reference transcript',summary,details}),{mode:0o600});
console.log(JSON.stringify(summary));
