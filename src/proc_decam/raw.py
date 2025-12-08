import logging

logging.basicConfig()
logger = logging.getLogger(__name__)

def main():
    """
    
    """
    import argparse
    import lsst.daf.butler as dafButler
    from lsst.daf.butler.registry import CollectionType, MissingCollectionError

    parser = argparse.ArgumentParser()
    parser.add_argument("repo")
    parser.add_argument("obs_type")
    parser.add_argument("night", type=int)
    parser.add_argument("--log-level", default="INFO")

    args = parser.parse_args()
    
    logging.getLogger().setLevel(args.log_level)

    butler = dafButler.Butler(args.repo, writeable=True)

    tagged = f"{args.night}/{args.obs_type}/raw"
    
    obs_type_lookup = dict(
        bias="zero",
        flat="dome flat",
        science="science",
        drp="science",
    )

    exposures = butler.registry.queryDimensionRecords(
        "exposure", 
        where=f"instrument='DECam' and exposure.day_obs={args.night} and exposure.observation_type='{obs_type_lookup[args.obs_type]}'"
    )
    raws = butler.registry.queryDatasets(
        "raw",
        collections="DECam/raw/all",
        where="instrument='DECam' and exposure in (" + ",".join(map(lambda x : str(x.id), exposures)) + ")",
    )
    raws = list(raws)

    logger.info("associatating %s raws into %s", len(raws), tagged)
    butler.registry.registerCollection(tagged, CollectionType.TAGGED)
    butler.registry.associate(tagged, raws)

if __name__ == "__main__":
    main()
