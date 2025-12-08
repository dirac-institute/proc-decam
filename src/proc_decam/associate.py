import os
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)

def main():
    import argparse
    import lsst.daf.butler as dafButler
    from lsst.daf.butler.registry import CollectionType, MissingCollectionError

    parser = argparse.ArgumentParser()
    parser.add_argument("repo")
    parser.add_argument("collection")
    parser.add_argument("--datasets", "-d", nargs="+", required=True)
    parser.add_argument("--collections", nargs="+", default=[])
    parser.add_argument("--where")
    parser.add_argument("--log-level", default="INFO")

    args = parser.parse_args()
    
    logging.getLogger().setLevel(args.log_level)

    butler = dafButler.Butler(args.repo, writeable=True)

    tagged = os.path.normpath(f"{args.collection}")
    
    if args.collections:
        collections = butler.registry.queryCollections(args.collections)
    else:
        collections = butler.registry.queryCollections("*")

    butler.registry.registerCollection(tagged, CollectionType.TAGGED)
    for dataset in args.datasets:
        refs = list(set(list(butler.registry.queryDatasets(
            dataset,
            collections=collections,
            where=args.where,
            findFirst=True,
        ))))

        logger.info("associatating %s of %s into %s", len(refs), dataset, tagged)
        butler.registry.associate(tagged, refs)

if __name__ == "__main__":
    main()
