// Bounds local FFmpeg work shared by workers in this runtime only.
export function createLocalLimiter({ limit = 2 } = {}) {
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 32) throw new Error('INVALID_LOCAL_LIMIT');
  let active=0;const queue=[];
  function drain(){
    while(active<limit&&queue.length){
      const item=queue.shift();item.signal?.removeEventListener('abort',item.abort);
      if(item.signal?.aborted){item.reject(new Error('PROCESSING_ABORTED'));continue;}
      active++;
      Promise.resolve().then(item.task).then(item.resolve,item.reject).finally(()=>{active--;drain();});
    }
  }
  function run(task,signal){
    if(typeof task!=='function')return Promise.reject(new Error('INVALID_LIMITED_TASK'));
    if(signal?.aborted)return Promise.reject(new Error('PROCESSING_ABORTED'));
    return new Promise((resolve,reject)=>{
      const item={task,signal,resolve,reject};
      item.abort=()=>{const index=queue.indexOf(item);if(index>=0){queue.splice(index,1);signal?.removeEventListener('abort',item.abort);reject(new Error('PROCESSING_ABORTED'));}};
      queue.push(item);signal?.addEventListener('abort',item.abort,{once:true});if(signal?.aborted)item.abort();drain();
    });
  }
  return {run,getStatus:()=>({active,waiting:queue.length,limit})};
}
