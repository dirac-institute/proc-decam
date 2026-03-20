#!/usr/bin/env bash

source /opt/lsst/software/stack/loadLSST.bash
setup lsst_distrib
set -xeuo pipefail

# set up data
python ${WORK}/proc_decam/tests/make_exposures.py \
--mastercals-dir ${WORK}/kbmod_mastercals_recipe/trimmedRawData/210318/calib \
--science-dir ${WORK}/kbmod_imdiff_recipe/trimmedRawData/210318/science \
--output ${DATA}/exposures.ecsv \
--image-dir ${DATA}/images

# start repo
proc-decam db start ${REPO}

# Ingest refcats
proc-decam refcats ${REPO} ${DATA}/exposures.ecsv

# ingest fakes
cd ${WORK}/kbmod_imdiff_recipe/trimmedRawData/fakes
python create_fakes.py
cd ${WORK}

proc-decam fakes ${REPO} \
  ${WORK}/kbmod_imdiff_recipe/trimmedRawData/fakes/fakes_fakeSrcCat.fits \
  --format fits

# ingest and process bias
proc-decam ingest ${DATA}/exposures.ecsv -b ${REPO} --image-dir ${DATA}/images/
proc-decam raw ${REPO} bias 20210318
proc-decam collection ${REPO} bias 20210318

cd ${WORK}/proc_decam # must have proc-decam pipelines available
proc-decam execute ${REPO} 20210318/bias \
  --pipeline pipelines/bias.yaml#step1 \
  --where "instrument='DECam' and detector=35"
