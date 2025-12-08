# ingest raws into the butler
import sys
import os
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)

def _log(*args, **kwargs):
    print(*args, **kwargs, file=sys.stderr)

def normalize_collection(values):
    collection = []
    for value in values:
        if value == "dome flat":
            value = "flat"
        if value == "zero":
            value = "bias"
        collection.append(str(value))
    collection.append("raw")
    return "/".join(collection)

def _ingest(butler, image_dir, exposures, run, processes=1, reingest=False):
    from lsst.obs.base import RawIngestTask
    from lsst.daf.butler.registry import MissingCollectionError

    raw = exposures[exposures['proc_type'] == "raw"]
    valid = raw[raw['valid_on_disk']]
    filenames = map(os.path.basename, valid['path'])
    paths = list(map(lambda x : os.path.join(image_dir, x), filenames))
    logger.info("ingesting %d files into %s", len(paths), run)

    if not reingest:
        try:
            if butler.registry.queryCollections(run):
                logger.info("skipping ingest because %s exists" % run)
                return
        except MissingCollectionError:
            pass

    config = RawIngestTask.ConfigClass()
    task = RawIngestTask(config=config, butler=butler)
    try:
        task.run(
            paths,
            run=run,
            processes=processes,
            skip_existing_exposures=True,
        )
    except RuntimeError as e:
        _log(str(e))

def ingest(butler, image_dir, exposures, collection, collection_keys, processes=4, reingest=False):
    if collection == "{keys}":
        for group in exposures.group_by(collection_keys).groups:
            run = normalize_collection(group[0][collection_keys])
            _ingest(butler, image_dir, group, run, processes=processes, reingest=reingest)
    else:
        _ingest(butler, image_dir, exposures, collection, processes=processes, reingest=True)

def main():
    import argparse
    import astropy.table
    import lsst.daf.butler as dafButler

    parser = argparse.ArgumentParser()
    parser.add_argument("exposures_file")
    parser.add_argument("--repo", "-b", required=True, type=str)
    parser.add_argument("--image-dir", type=str, required=True)
    parser.add_argument("--select", nargs="+", type=str)
    parser.add_argument("--collection-keys", nargs="+", default=["night", "obs_type", "band"])
    parser.add_argument("--collection", default="DECam/raw/all")
    parser.add_argument("--processes", "-J", default=4, type=int)
    parser.add_argument("--reingest", action="store_true")
    parser.add_argument("--log-level", default="INFO")

    args = parser.parse_args()

    logging.getLogger().setLevel(args.log_level)

    exposures = astropy.table.Table.read(args.exposures_file)

    if args.select:
        for select in args.select:
            _log("sub selecting", select)
            k, v = select.split("=")
            if len(exposures) > 0:
                _select = exposures[k].astype(str) == v
                exposures = exposures[_select]

    downloaded_exposures = astropy.table.Table.read(os.path.join(os.path.join(os.path.dirname(args.exposures_file), "downloaded_" + os.path.basename(args.exposures_file))))
    exposures = astropy.table.join(exposures, downloaded_exposures, keys=["md5sum"])

    ingest(dafButler.Butler(args.repo, writeable=True), args.image_dir, exposures, args.collection, args.collection_keys, processes=args.processes, reingest=args.reingest)

if __name__ == "__main__":
    main()
