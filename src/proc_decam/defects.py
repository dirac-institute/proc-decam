import logging
import requests
import os

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BADPIX_VALUE = 1
SUSPECTPIX_VALUE = 7

def download_data(download_dir):
    REMOTE_URL = "https://epyc.astro.washington.edu/~stevengs/decam_bpm"
    for d in ['cp', 'des']:
        r = requests.get(f"{REMOTE_URL}/{d}/index.dat")
        r.raise_for_status()
        files = r.text.strip().split("\n")
        for f in files:
            if os.path.exists(os.path.join(download_dir, d, f)):
                logger.info("skipping existing file %s", f)
                continue
            os.makedirs(os.path.join(download_dir, d), exist_ok=True)
            logger.info("downloading %s", f)
            r2 = requests.get(f"{REMOTE_URL}/{d}/{f}")
            r2.raise_for_status()
            with open(os.path.join(download_dir, d, f), "wb") as fp:
                fp.write(r2.content)

def load_des(path, detector):
    import os
    import astropy.io.fits as fits_io
    p = os.path.join(path, "des", f"D_n20150105t0115_c{detector:02d}_r2134p01_bpm.fits")
    logger.info("loading %s", p)
    if os.path.exists(p):
        des = fits_io.open(p)
        assert(des[0].header['CCDNUM'] == detector)
        bad = ((des[0].data != 512) & (des[0].data != 0)).astype(int) * BADPIX_VALUE
        suspect = (des[0].data == 512).astype(int) * SUSPECTPIX_VALUE
        return bad, suspect
    return None, None

def load_cp(path, detector):
    import os
    import astropy.io.fits as fits_io
    p = os.path.join(path, "cp", f"DECam_Master_20140209v2_cd_{detector:02d}.fits")
    logger.info("loading %s", p)
    if os.path.exists(p):
        cp = fits_io.open(p)
        assert(cp[0].header['CCDNUM'] == detector)
        bad = (cp[0].data != 0).astype(int)
        return bad, None
    return None, None

def create_defects(defects, bad, suspect):
    import numpy as np
    import astropy.table
    from lsst.ip.isr.defects import Defects

    pl = defects.toDict()['metadata'].deepCopy()
    if bad is not None:
        d = []
        for x, y in zip(*np.where(bad)):
            d.append({
                "x0": y,
                "y0": x,
                "width": 1,
                "height": 1,
            })

        d = astropy.table.Table(d)
        d = Defects.fromTable([d])
        d.setMetadata(pl)
    else:
        d = defects

    pl = defects.toDict()['metadata'].deepCopy()
    if suspect is not None:
        pl['DEFECTTYPE'] = "SUSPECTPIXEL"
        s = []
        for x, y in zip(*np.where(suspect)):
            s.append({
                "x0": y,
                "y0": x,
                "width": 1,
                "height": 1,
            })

        s = astropy.table.Table(s)
        s = Defects.fromTable([s])
        s.setMetadata(pl)
    else:
        s = Defects()
        s.setMetadata(pl)
    return d, s

def main():
    import argparse
    import lsst.daf.butler as dafButler
    import astropy.time
    from datetime import datetime
    import os

    parser = argparse.ArgumentParser()
    parser.add_argument("repo")
    parser.add_argument("bpm_path")
    parser.add_argument("--calib-collection", default="DECam/calib_bpm")
    parser.add_argument("--bad-dataset", default="defects")
    parser.add_argument("--suspect-dataset", default="suspectMask")
    parser.add_argument("--run-prefix", default="DECam/calib_bpm")

    args = parser.parse_args()

    butler = dafButler.Butler(args.repo, writeable=True)

    def register_collection_if_not_exists(collection, collection_type):
        try:
            butler.registry.queryCollections(collection)
        except dafButler.MissingCollectionError:
            logger.info("registering %s as %s", collection, collection_type)
            butler.registry.registerCollection(collection, collection_type)
    
    bad_run = os.path.join(args.run_prefix, "bad")
    suspect_run = os.path.join(args.run_prefix, "suspect")
    register_collection_if_not_exists(args.calib_collection, dafButler.registry.CollectionType.CALIBRATION)
    register_collection_if_not_exists(bad_run, dafButler.registry.CollectionType.RUN)
    register_collection_if_not_exists(suspect_run, dafButler.registry.CollectionType.RUN)

    def register_dataset_if_not_exists(dataset):
        if len(list(butler.registry.queryDatasetTypes(dataset))) == 0:
            logger.info("registering %s", dataset)
            butler.registry.registerDatasetType(
                dafButler.DatasetType(
                    dataset, 
                    dafButler.DimensionGroup(
                        butler.dimensions, ("instrument", "detector")
                    ), 
                    "Defects",
                    isCalibration=True,
                )
            )
    
    register_dataset_if_not_exists(args.bad_dataset)
    register_dataset_if_not_exists(args.suspect_dataset)

    refs = list(butler.registry.queryDatasets(args.bad_dataset, collections=bad_run))
    logger.info("removing %d of %s from %s", len(refs), args.bad_dataset, bad_run)
    butler.pruneDatasets(
        refs,
        disassociate=True,
        unstore=True,
        purge=True,
    )
    refs = list(butler.registry.queryDatasets(args.suspect_dataset, collections=suspect_run))
    logger.info("removing %d of %s from %s", len(refs), args.suspect_dataset, suspect_run)
    butler.pruneDatasets(
        refs,
        disassociate=True,
        unstore=True,
        purge=True,
    )

    for detector in range(1, 63):
        ref = list(sorted(
            butler.registry.queryDatasets(
                "defects", 
                instrument='DECam', 
                detector=detector, 
                collections="DECam/calib"
            ),
            key=lambda x : datetime.fromisoformat(x.run.split("/")[-1])
        ))[-1]
        logger.info("loading %s", ref)
        defects = butler.get(ref)
        bad_des, suspect_des = load_des(args.bpm_path, detector)
        bad_cp, _ = load_cp(args.bpm_path, detector)
        if bad_des is not None and bad_cp is not None:
            bad = ((bad_des == BADPIX_VALUE) | (bad_cp == BADPIX_VALUE)).astype(int) * BADPIX_VALUE
        elif bad_des is not None:
            bad = bad_des
        elif bad_cp is not None:
            bad = bad_cp
        else:
            bad = None

        bad_defects, suspect_defects = create_defects(defects, bad, suspect_des)
        logger.info("putting %s in %s for detector %d", args.bad_dataset, bad_run, detector)
        butler.put(
            bad_defects, args.bad_dataset, 
            dataId={"instrument": "DECam", "detector": detector},
            run=bad_run,
        )
        logger.info("putting %s in %s for detector %d", args.suspect_dataset, suspect_run, detector)
        butler.put(
            suspect_defects, args.suspect_dataset, 
            dataId={"instrument": "DECam", "detector": detector},
            run=suspect_run,
        )

    timespan = dafButler.Timespan(
        astropy.time.Time("1970-01-01T00:00:00", scale='tai'),
        astropy.time.Time("2100-01-01T00:00:00", scale='tai')
    )

    def certify(dataset, calib, run):
        if len(list(butler.registry.queryDatasets(dataset, collections=calib))) > 0:
            butler.registry.decertify(calib, dataset, timespan)
        refs = list(butler.registry.queryDatasets(dataset, collections=run))
        logger.info("certifying %d of %s from %s into %s", len(refs), dataset, run, calib)
        butler.registry.certify(calib, refs, timespan)

    certify(args.bad_dataset, args.calib_collection, bad_run)
    certify(args.suspect_dataset, args.calib_collection, suspect_run)

if __name__ == "__main__":
    main()
