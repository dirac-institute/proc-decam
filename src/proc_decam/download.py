#!/usr/bin/env python

import logging
import joblib
import argparse
from .noirlab import api as noirlab_api
import hashlib
import sys
import os
import astropy.table
from pathlib import Path

logging.basicConfig()
logger = logging.getLogger(__name__)

def download_to_file(md5, fname, progress=True, headers={}):
    with open(fname, "wb") as outfile:
        for chunk in noirlab_api.download(md5, progress=progress, headers=headers):
            outfile.write(chunk)

def verify_md5_of_file(fname, md5, return_md5=False):
    with open(fname, "rb") as f:
        data = f.read()
        md5_to_check = hashlib.md5(data).hexdigest()

    if return_md5:
        return md5_to_check == md5, md5_to_check
    else:
        return md5_to_check == md5

def _log(*args, **kwargs):
    print(*args, **kwargs, file=sys.stderr)

def _download(row, download_dir, headers={}):
    md5 = row['md5sum']
    valid_in_archive = row['valid_in_archive']
    valid_on_disk = row['valid_on_disk']
    did_download = row['did_download']
    did_check_archive = row['did_check_archive']
    did_check_disk = row['did_check_disk']
    
    download_filename = md5 + "_" + os.path.basename(row['archive_filename'])
    path = os.path.join(download_dir, download_filename)
    path = str(Path(path).absolute())
    exists_on_disk = os.path.exists(path)

    do_download = False
    do_check_archive = False
    do_check_disk = False

    if exists_on_disk:
        if not did_check_disk:
            _log(download_filename, "exists but may be invalid on disk; will check disk")
            do_check_disk = True
        else:
            # we have already checked on disk
            if not valid_on_disk:
                # file is not valid
                if did_check_archive and valid_in_archive:
                    # the file isn't valid on disk but it is valid in the archive
                    _log(download_filename, "exists but is not valid on disk and is valid in the archive; will re-download")
                    do_download = True
                else:
                    # check the file and download if needed
                    _log(download_filename, "exists but is not valid on disk and may be valid in the archive; will check archive")
                    do_check_archive = True
    else:
        # the file doesn't exist
        valid_on_disk = False
        did_check_disk = False
        _log(download_filename, "does not exist on filesystem; will download")
        do_download = True

    if do_check_disk:
        valid_on_disk = verify_md5_of_file(path, md5)
        did_check_disk = True
        if not valid_on_disk:
            do_check_archive = True

    if do_check_archive:
        valid_in_archive = noirlab_api.check(md5, headers=headers)
        did_check_archive = True
        if valid_in_archive:
            do_download = True
    
    if do_download:
        try:
            _log(f"downloading {download_filename}")
            download_to_file(md5, path, progress=False, headers=headers)
            did_download = True
        except Exception as e:
            _log(f"failed downloading {download_filename}. Error was: {e}")
            did_download = False

        try:
            _log(f"checking validity of {download_filename}")
            valid_on_disk = verify_md5_of_file(path, md5)
            did_check_disk = True
            if not verify_md5_of_file(path, md5):
                _log(f"md5 of {download_filename} did not match, download may be incomplete or file corrupt")
        except Exception as e:
            _log(f"failed checking {download_filename}. Error was: {e}")
            valid_on_disk = False
            did_check_disk = False

    return dict(
        path=path,
        md5sum=md5,
        did_download=did_download,
        valid_in_archive=valid_in_archive,
        valid_on_disk=valid_on_disk,
        did_check_archive=did_check_archive,
        did_check_disk=did_check_disk,
    )

def download(exposures, download_dir, log_level="INFO", parallel_backend="loky", processes=1):
    def job(result, headers={}):
        return _download(result, download_dir, headers=headers)
    
    auth_headers = noirlab_api.get_auth_headers()
    with joblib.parallel_config(backend=parallel_backend, n_jobs=processes):
        results = joblib.Parallel()(joblib.delayed(job)(exposure, headers=auth_headers) for exposure in exposures)
    
    return results

def merge(default, new, on):
    rows = []
    for row_1 in default:
        v = row_1[on]
        d = new[new[on] == v]
        # default to the values in t1
        row = {
            k: row_1[k]
            for k in default.columns
        }
        d = new[new[on] == v]
        if len(d) > 0:
            # update with the values in t2
            # this perhaps shouldn't be more than one row
            for row_2 in d:
                row = {
                    k: row_2[k]
                    for k in new.columns
                }
                rows.append(row)
        else:
            rows.append(row)
        
    return astropy.table.Table(rows)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("exposures_file", type=str)
    parser.add_argument("--download-dir", type=str, default=".")
    parser.add_argument("-j", "--processes", type=int, default=4)
    parser.add_argument("--parallel-backend", type=str, default="loky")
    parser.add_argument("--log-level", type=str, default="INFO")
    parser.add_argument("--select", nargs="+", type=str)
    args, _ = parser.parse_known_args()

    logging.getLogger().setLevel(getattr(logging, args.log_level.upper()))

    exposures_file = os.path.join(args.exposures_file)
    downloaded_file = os.path.join(os.path.join(os.path.dirname(args.exposures_file), "downloaded_" + os.path.basename(args.exposures_file)))

    exposures = astropy.table.Table.read(exposures_file)

    defaults = dict(
        md5sum="",
        path="",
        did_download=False,
        valid_on_disk=False,
        valid_in_archive=True,
        did_check_archive=False,
        did_check_disk=False,
    )
    downloaded = astropy.table.Table([{k: v for k, v in defaults.items()} for e in exposures])
    downloaded['md5sum'] = exposures['md5sum']

    if os.path.exists(downloaded_file):
        downloaded = merge(downloaded, astropy.table.Table.read(downloaded_file), "md5sum")
        
    # only download exposures we have the integrity value for
    exposures = astropy.table.join(exposures, downloaded, keys=['md5sum'])

    if args.select:
        for select in args.select:
            _log("sub selecting", select)
            k, v = select.split("=")
            if len(exposures) > 0:
                _select = exposures[k].astype(str) == v
                exposures = exposures[_select]

    os.makedirs(args.download_dir, exist_ok=True)
    _log(f"downloading {len(exposures)} exposures")
    downloaded = download(exposures, args.download_dir, log_level=args.log_level, parallel_backend=args.parallel_backend, processes=args.processes)
    downloaded = astropy.table.Table(downloaded)
    
    if os.path.exists(downloaded_file):
        downloaded = merge(astropy.table.Table.read(downloaded_file), downloaded, "md5sum")

    _log(f"writing downloaded to {downloaded_file}")
    downloaded.write(downloaded_file, format='ascii.ecsv', overwrite=True)
    # os.makedirs(os.path.join(args.download_dir, "bad"), exist_ok=True)

if __name__ == "__main__":
    main()
