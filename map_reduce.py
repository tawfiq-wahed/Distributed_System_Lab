import multiprocessing as mp
import os
reducer=3
documents=[
    "I AM TAWFIQ BIN WAHED",
    "I AM FROM CHITTAGONG",
    "I AM A SOFTWARE UNDERGRADUATE STUDENT"
]
def map_task(doc,text):
    print(f"[map workers] {os.getpid()} working on {doc}:{text!r}")
    pairs=[]
    for word in text.split():
        pairs.append((word,1))
    return pairs
def shuffle(all_pairs,reducer):
    groups={}
    for pairs in all_pairs:
        for word,count in pairs:
            groups.setdefault(word,[]).append(count)
    buckets=[{} for _ in range(reducer)]
    for word,counts in groups.items():
        bucket_id=hash(word)%reducer
        buckets[bucket_id][word]=counts
    return buckets
def reduce_task(bucket_id,bucket):
    print(
            f"[REDUCE worker pid={os.getpid()}] "
            f"reducing bucket {bucket_id} "
            f"({len(bucket)} distinct words)"
        )
    result = {}

    for word, counts in bucket.items():

        result[word] = sum(counts)

    return result
def main():

    print(
        "[MASTER] Splitting input into",
        len(documents),
        "map tasks"
    )


    with mp.Pool(processes=4) as pool:

        map_results = pool.starmap(
            map_task,
            [(i, doc) for i, doc in enumerate(documents)]
        )

    print(
        "[MASTER] Map phase done. Starting shuffle..."
    )

    buckets = shuffle(
        map_results,
        reducer
    )

    print(
        f"[MASTER] Shuffle done. "
        f"Created {reducer} reduce partitions."
    )


    with mp.Pool(processes=reducer) as pool:

        reduce_results = pool.starmap(
            reduce_task,
            list(enumerate(buckets))
        )


    final_counts = {}

    for partial in reduce_results:

        final_counts.update(partial)

    print("\n[MASTER] FINAL WORD COUNTS:")

    for word, count in sorted(
        final_counts.items(),
        key=lambda kv: -kv[1]
    ):

        print(
            f"  {word:12s} {count}"
        )
if __name__=="__main__":
    main()

