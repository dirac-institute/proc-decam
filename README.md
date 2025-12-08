# proc-decam

Process a DECam imaging survey using the LSST Science Pipelines.

Install LSST Science Pipelines: https://pipelines.lsst.io/
- This package has been tested with version `w_2024_34` of the Science Pipelines. 
- Later version may break compatibility with this package due to the shared dependence on `parsl`, which has a quickly changing API.

Install this package on top of the pipelines:
```
$ python -m pip install git+github.com/dirac-institute/proc-decam.git
```

Create LSST repository:
```
$ butler create ./repo
$ butler register-instrument ./repo lsst.obs.decam.DarkEnergyCamera
$ butler write-curated-calibrations ./repo lsst.obs.decam.DarkEnergyCamera
```

Register Skymap
```
$ butler register-skymap ./repo -c name='discrete'
```

Query for survey images:
```
$ proc-decam exposures ./data --proposal-id 2019A-0337
```

Download survey images:
```
$ proc-decam download ./data/exposures.ecsv --download-dir ./data/images
```

Ingest defects:
```
$ proc-decam defects ./repo ./data/bpm
```

Get reference catalogs:
```
$ proc-decam refcats ./repo ./data/exposures.ecsv
```

Ingest fakes:
```
$ proc-decam fakes ./repo path/to/fakes.fits # astropy readable table with columns RA/DEC/MAG/BAND/EXPNUM
```

Process a night of the survey (and a subset of the data with `--where`). Control parallelism of the survey processing pipeline with `-J`. Control parallelism of the execution of the LSST Science Pipelines with the `J` environment variable:
```
$ J=24 proc-decam night ./repo ./data/exposures.ecsv --nights 20190401 --where "instrument='DECam' and detector=1" -J 4
```
This pipeline will take advantage of at most `24*4 = 96` cores.

Executing the nightly pipeline will execute (via `proc-decam pipeline`) the master bias construction pipeline ([pipelines/bias.yaml])(), master flat construction pipeline ([pipelines/flat.yaml])(), and the science exposure calibration pipelines ([pipelines/DRP.yaml]())for survey nights matching the `--nights` argument and respecting the LSST Pipelines `--where` query. In this case, detector `1` from night `20190401` of the survey will be processed.

The `proc-decam pipeline` command executes processing pipelines, the definition of which are stored in the `pipelines` top-level directory. 
```
$ proc-decam pipeline --help
usage: proc-decam pipeline [-h] [--template-type TEMPLATE_TYPE] [--coadd-subset COADD_SUBSET]
                           [--steps STEPS [STEPS ...]] [--where WHERE] [--log-level LOG_LEVEL] [--slurm]
                           [--workers WORKERS]
                           repo proc_type subset

positional arguments:
  repo
  proc_type
  subset

options:
  -h, --help            show this help message and exit
  --template-type TEMPLATE_TYPE
  --coadd-subset COADD_SUBSET
  --steps STEPS [STEPS ...]
  --where WHERE
  --log-level LOG_LEVEL
  --slurm
  --workers WORKERS, -J WORKERS
```
The pipelines are collections of individual tasks from the LSST Science Pipelines that will be executed on a provided set of input data. Tasks are groups into `steps` of the pipeline that have a common set of input data dimensions (e.g. tasks that apply per-detector or per-exposure of the survey). The `proc-decam pipeline` utilizes the `proc-decam execute` command to execute a single step (or an entire pipeline definition)
```
$ proc-decam execute --help
usage: proc-decam execute [-h] [--pipeline PIPELINE] [--where WHERE] [--no-skip-existing] [--no-skip-failures]
                          [--no-loop] [--no-trigger-retry]
                          repo parent

positional arguments:
  repo
  parent

options:
  -h, --help           show this help message and exit
  --pipeline PIPELINE
  --where WHERE
  --no-skip-existing
  --no-skip-failures
  --no-loop
  --no-trigger-retry
```
The `proc-decam execute` command handles executing a pipeline definition file (or a subset of it), handling the requirement to skip datasets that have already been produced, skip tasks which result in non-recoverable failures, and retry execution of tasks that have recoverable failures. 

# Reference Catalogs

The calibration of science exposures requires reference catalogs which enable photometric and astrometric calibration. These reference catalogs are available via the `get-lsst-refcats` package: [https://github.com/dirac-institute/lsst_refcats](https://github.com/dirac-institute/lsst_refcats).

If providing and ingesting your own reference catalogs, update the reference catalog configurations of the `CalibrateTask` in `pipelines/DRP.yaml`.

# Using Postgres

To enable scalable and distributed processing, it is recommended to use a Postgres database for the LSST Science Pipelines registry. We provide a utility to set up a Postgres database when creating Butler repository:
```
$ proc-decam db create ./repo
```

One should execute
```
$ proc-decam db start ./repo
```
to start the database and
```
$ proc-decam db stop ./repo
```
to stop it.
