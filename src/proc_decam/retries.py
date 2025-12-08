import lsst.daf.butler as dafButler
import re
import sys

def check_for_retries(butler, run):
    retry_regexes = [
        re.compile(".*MemoryError: std::bad_alloc.*"),
        re.compile(".*ValueError: Failure from formatter.*std::bad_alloc.*"),
        re.compile(".*MemoryError: Unable to allocate.*"),
        re.compile(".*MemoryError.*"),
        re.compile(".*RuntimeError: Failed to serialize dataset.*")
    ]
    
    refs = set(list(butler.registry.queryDatasets("*_log", collections=run)))
    retries = []
    for ref in refs:
        log = butler.get(ref)
        for record in log:
            if any([regex.match(record.message) for regex in retry_regexes]):
                print("ref needs retry", ref, file=sys.stderr)
                retries.append(ref)
                break
                
    return retries

def copy_ref(butler, ref, run, _from, to):
    datasetType = ref.datasetType.__class__(
        ref.datasetType.name.replace(_from, to), 
        ref.datasetType.dimensions, 
        ref.datasetType.storageClass
    )
    if len(butler.registry.queryDatasetTypes(datasetType.name)) == 0:
        print("registering", datasetType)
        butler.registry.registerDatasetType(datasetType)    
    existing = list(map(lambda x : x.dataId, butler.registry.queryDatasets(datasetType, dataId=ref.dataId, collections=run)))
    if ref.dataId not in existing:
        print("moving", ref.datasetType, "to", datasetType)
        butler.put(butler.get(ref), datasetType, dataId=ref.dataId, run=run)
    
def move_refs(butler, refs, run, _from, to):
    to_prune = []
    for ref in refs:
        copy_ref(butler, ref, run, _from, to)
        to_prune.append(ref)
    if len(to_prune) > 0:
        print("pruning", to_prune, file=sys.stderr)
        butler.pruneDatasets(to_prune, disassociate=True, unstore=True, purge=True)
    
def get_metadata_refs(butler, refs, run):
    return sum(
        [list(
            butler.registry.queryDatasets(
                ref.datasetType.name.replace("_log", "_metadata"),
                dataId=ref.dataId,
                collections=run
            )
        ) for ref in refs], []
    )
    

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("repo")
    parser.add_argument("run")
    
    args = parser.parse_args()

    butler = dafButler.Butler(args.repo, writeable=True)
    retries = check_for_retries(butler, args.run)
    print("need to retry", len(retries), "tasks", file=sys.stderr)
    move_refs(butler, retries, args.run, "_log", "_log_retry")

if __name__ == "__main__":
    main()
