// Aggregate-only comparison; do not print source text or signed URLs.
import { readFile } from 'node:fs/promises';
const [firstPath,secondPath,durationValue]=process.argv.slice(2);
const duration=Number(durationValue);
if(!firstPath||!secondPath||!Number.isSafeInteger(duration)||duration<=0)throw Error('INVALID_COMPARISON_INPUT');
function parse(text){
  const blocks=text.trim().split(/\r?\n\s*\r?\n/);let previous=-1;
  return blocks.map((block,index)=>{
    const lines=block.split(/\r?\n/);if(Number(lines[0])!==index+1)throw Error('INVALID_SRT_INDEX');
    const m=/^(\d+):(\d\d):(\d\d),(\d{3}) --> (\d+):(\d\d):(\d\d),(\d{3})$/.exec(lines[1]??'');
    if(!m)throw Error('INVALID_SRT_TIMESTAMP');
    const ms=offset=>(((Number(m[offset])*60+Number(m[offset+1]))*60+Number(m[offset+2]))*1000+Number(m[offset+3]));
    const start=ms(1),end=ms(5);if(start<previous||start<0||end<=start||end>duration)throw Error('INVALID_SRT_TIMELINE');previous=start;
    return {start,end,text:lines.slice(2).join(' ').trim().toLocaleLowerCase().replace(/\s+/g,' ')};
  });
}
const [firstText,secondText]=await Promise.all([readFile(firstPath,'utf8'),readFile(secondPath,'utf8')]);
const a=parse(firstText),b=parse(secondText);
const counts=new Map();for(const row of b)counts.set(row.text,(counts.get(row.text)??0)+1);
let identical=0;for(const row of a){const n=counts.get(row.text)??0;if(n){identical++;counts.set(row.text,n-1);}}
const used=new Set(),startDiff=[],endDiff=[];let cursor=0;
for(const row of a){
  while(cursor<b.length&&b[cursor].start<row.start-1000)cursor++;
  let best=-1,delta=1001;
  for(let i=cursor;i<b.length&&b[i].start<=row.start+1000;i++){
    const d=Math.abs(b[i].start-row.start);if(!used.has(i)&&d<delta){best=i;delta=d;}
  }
  if(best>=0){used.add(best);startDiff.push(delta);endDiff.push(Math.abs(b[best].end-row.end));}
}
const p95=values=>{values.sort((x,y)=>x-y);return values.length?values[Math.ceil(values.length*.95)-1]:null;};
function duplicates(rows){let count=0;for(let i=1;i<rows.length;i++)if(rows[i].text===rows[i-1].text&&rows[i].start-rows[i-1].end<=1000)count++;return count;}
console.log(JSON.stringify({firstCues:a.length,secondCues:b.length,identicalTextCues:identical,
  firstUnmatched:a.length-identical,secondUnmatched:b.length-identical,firstAdjacentDuplicates:duplicates(a),
  secondAdjacentDuplicates:duplicates(b),firstBytes:Buffer.byteLength(firstText),secondBytes:Buffer.byteLength(secondText),
  pairedWithin1000ms:startDiff.length,pairedStartP95Ms:p95(startDiff),pairedEndP95Ms:p95(endDiff),timelineValid:true}));
