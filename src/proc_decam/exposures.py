from .noirlab import query as noirlab_query
from .noirlab import api as noirlab_api
import astropy.table
import astropy.time
import sys
import astropy.units as u
import astropy.coordinates
import os
import logging

logging.basicConfig()
log = logging.getLogger(__name__)

def survey_exposures(proposal):
    outfields = [ 
        "archive_filename", "obs_type", "proc_type", 
        "prod_type", "md5sum", "dateobs_center", "caldat", 
        "exposure", 
        "RA", "DEC", "OBJECT", "FILTER",
        "depth", "AIRMASS", "seeing",
        "PROPID", "EXPNUM",
    ]

    log.info("searching for raws under proposal %s", proposal)
    raws = noirlab_api.search(
        noirlab_query.query(
            "raw", "object", outfields, 
            proposal=proposal, caldat='2019-04-01'
        )
    )
    log.info("found %d raws under proposal %s", len(raws), proposal)
    raws = astropy.table.Table(raws)

    log.info("searching for instcals under proposal %s", proposal)
    instcals = noirlab_api.search(
        noirlab_query.query(
            "instcal", "object", outfields, 
            proposal=proposal, caldat='2019-04-01'
        )
    )
    log.info("found %d instcals under proposal %s", len(instcals), proposal)
    instcals = astropy.table.Table(instcals)

    caldats = sorted(list(set(list(map(lambda x : x['caldat'], raws)))))

    calibrations = []

    log.info("searching for calibrations for %d nights from survey dates %s - %s", len(caldats), caldats[0], caldats[-1])
    missing = []
    for caldat in caldats:
        images = list(filter(lambda x : x['caldat'] == caldat, raws))
        bands = sorted(list(set(list(map(lambda x : x['FILTER'].split(" ")[0], images)))))
        bias = noirlab_api.search(
            noirlab_query.query(
                "raw", "zero", outfields, 
                caldat=caldat
            )
        )
        calibrations.extend(bias)
        if len(bias) == 0:
            missing.append({"observation_type": "bias", "caldat": caldat, "band": None})
            log.info("no bias on %s", caldat)
        for band in bands:
            flat = noirlab_api.search(
                noirlab_query.query(
                    "raw", "dome flat", outfields, 
                    caldat=caldat, band=band
                )
            )
            calibrations.extend(flat)
            if len(flat) == 0:
                missing.append({"observation_type": "flat", "caldat": caldat, "band": band})
                log.info("no flat for %s on %s", band, caldat)
    
    missing = astropy.table.Table(missing)
    calibrations = astropy.table.vstack(calibrations)

    exposures = astropy.table.vstack([raws, instcals, calibrations])
    # return exposures, missing
    # print(exposures)
    exposures['dateobs_midpoint'] = astropy.time.Time(exposures['dateobs_center'])
    exposures['dateobs_min'] = exposures['dateobs_midpoint'] - astropy.time.TimeDelta(exposures['exposure']/2 + 0.5, format='sec')
    exposures['mjd'] = exposures['dateobs_min'].mjd
    exposures['mjd_midpoint'] = exposures['dateobs_midpoint'].mjd
    exposures['night'] = list(map(lambda x : int("".join(x.split("-"))), exposures['caldat']))
    coords = astropy.coordinates.SkyCoord(ra=exposures['RA'], dec=exposures['DEC'], unit=(u.hourangle, u.deg))
    exposures['RA(deg)'] = coords.ra
    exposures['DEC(deg)'] = coords.dec

    exposures['band'] = list(map(lambda x : x.split(" ")[0], exposures['FILTER']))

    return exposures, missing

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("data_dir")
    parser.add_argument("--proposal-id", "-p", default="2019A-0337")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    log.setLevel(args.log_level.upper())
    noirlab_api.logger.setLevel(args.log_level.upper())

    exposures_file = os.path.join(args.data_dir, "exposures.ecsv")
    missing_file = os.path.join(args.data_dir, "missing_data.ecsv")
    exposures, missing = survey_exposures(args.proposal_id)
    
    os.makedirs(args.data_dir, exist_ok=True)
    log.info("writing exposures to %s", exposures_file)
    exposures.write(exposures_file, format='ascii.ecsv', overwrite=True)
    log.info("writing missing data to %s", missing_file)
    missing.write(missing_file, format='ascii.ecsv', overwrite=True)

if __name__ == "__main__":
    main()
