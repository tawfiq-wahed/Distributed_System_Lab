import multiprocessing as mp
import os
import time
documents=[
    "MY NAME IS TAWFIQ BIN WAHED",
    "I LIVE IN SYLHET",
    "I LOVE NBS ",
    "IS SHE DO THE SAME"
]

map_timeout=2.0
def flaky_task(doc_id,text,attempt):
    pid=os.getpid()
    print(f"[map worker] pid attempt #{attempt} on doc id:{doc_id}:{text!r}")
    if doc_id==1 and attempt==1:
        print(f"map worker {pid} working on {doc_id} get failure")
        raise RuntimeError("Simulated worker crash")
    if doc_id==3:
        time.sleep(3.0)
    pairs=[(word,1) for word in text.split()]
    return pairs
def run_map_with_retry(pool,doc_id,text,max_attempts=3):
    for attempt in range(1,max_attempts+1):
        async_result=pool.apply_async(flaky_task,(doc_id,text,attempt))
        try:
            return async_result.get(timeout=map_timeout)
        except mp.TimeoutError:
            print(f"running master with doc:{doc_id} is a straggler")
            print(f"calling backup copy")
            backup_result=pool.apply_async(flaky_task,(doc_id,text,attempt))
            return backup_result.get()
        except Exception as e:
            print(f"document :{doc_id} is crashed #attempt{attempt};" 
            f"re-executing workers")
            continue
        raise RuntimeError(f"in doc:{doc_id} after maximum attempts:{max_attempts}")
def main():
    print(f"master spliting {len(documents)} into map task")
    with mp.Pool(processes=4) as pool:
        all_pairs=[]
        for doc_id,text in enumerate(documents):
            pairs=(run_map_with_retry(pool,doc_id,text))
            all_pairs.extend(pairs)
    counts={}
    for word,c in all_pairs:
        counts[word]=counts.get(word,0)+c
    print(f"all task eventually succeded final word counts are")
    for word,count in sorted(counts.items(),key=lambda kv:-kv[1]):
        print(f"{word:12s} {count}")
if __name__=="__main__":
    main()            
            
        
           
    

