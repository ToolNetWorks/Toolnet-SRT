// Compare aggregate output characteristics without logging transcript text.
import { readFile } from 'node:fs/promises';
const [firstPath,secondPath]=process.argv.slice(2);
if(!firstPath||!secondPath) throw new Error('TWO_BENCHMARK_FILES_REQUIRED');
const first=JSON.parse(await readFile(firstPath,'utf8'));
const second=JSON.parse(await readFile(secondPath,'utf8'));
const normalize=s=>s.trim().toLocaleLowerCase().replace(/\s+/g,' ');
function inspect(rows){
  if(!Array.isArray(rows))throw new Error('INVALID_BENCHMARK_RESULTS');
  let invalid=0,overlaps=0,adjacentDuplicates=0;
  const counts=new Map();let previous=null;
  for(const row of rows){
    if(!Number.isFinite(row.startMs)||!Number.isFinite(row.endMs)||row.startMs<0||row.endMs<=row.startMs||typeof row.text!=='string')invalid++;
    const text=typeof row.text==='string'?normalize(row.text):'';
    if(previous && row.startMs<previous.startMs)invalid++;
    if(previous && row.startMs<previous.endMs)overlaps++;
    if(previous && previous.text===text && row.startMs-previous.endMs<=1000)adjacentDuplicates++;
    counts.set(text,(counts.get(text)??0)+1);previous={...row,text};
  }
  return {cues:rows.length,invalid,overlaps,adjacentDuplicates,counts};
}
const a=inspect(first),b=inspect(second);let common=0;
for(const [text,count] of a.counts)common+=Math.min(count,b.counts.get(text)??0);
function words(rows){const map=new Map();for(const row of rows)for(const word of normalize(row.text).match(/[\p{L}\p{N}]+/gu)??[])map.set(word,(map.get(word)??0)+1);return map;}
const wa=words(first),wb=words(second);let waCount=0,wbCount=0,wordCommon=0;
for(const [word,count] of wa){waCount+=count;wordCommon+=Math.min(count,wb.get(word)??0);}
for(const count of wb.values())wbCount+=count;
const used=new Set(), offsets=[];let nearExact=0,nearDifferent=0,windowStart=0;
for(const x of first){
  while(windowStart<second.length && second[windowStart].startMs<x.startMs-1000)windowStart++;
  let best=-1,distance=1001;
  for(let i=windowStart;i<second.length && second[i].startMs<=x.startMs+1000;i++){
    const d=Math.abs(second[i].startMs-x.startMs);
    if(!used.has(i)&&d<distance){best=i;distance=d;}
  }
  if(best<0)continue;
  used.add(best);offsets.push(distance);
  if(normalize(x.text)===normalize(second[best].text))nearExact++; else nearDifferent++;
}
offsets.sort((x,y)=>x-y);
delete a.counts;delete b.counts;
console.log(JSON.stringify({first:a,second:b,identicalTextCues:common,firstUnmatched:a.cues-common,secondUnmatched:b.cues-common,
  temporalPairsWithin1000ms:offsets.length,pairedExactText:nearExact,pairedDifferentText:nearDifferent,
  pairedStartDifferenceP95Ms:offsets.length?offsets[Math.ceil(offsets.length*.95)-1]:null,
  firstWords:waCount,secondWords:wbCount,commonWordOccurrences:wordCommon}));
