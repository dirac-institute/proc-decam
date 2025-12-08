import lsst.pipe.base as pipeBase
from lsst.pipe.base import PipelineTask, PipelineTaskConfig, PipelineTaskConnections
import lsst.pipe.base.connectionTypes as cT
from lsst.skymap import BaseSkyMap
import pandas as pd
from astropy.table import vstack, Table
import numpy as np

class PartitionFakesConnections(PipelineTaskConnections, dimensions=("skymap",)):
    skyMap = cT.Input(
        doc="Skymap that defines tracts",
        name=BaseSkyMap.SKYMAP_DATASET_TYPE_NAME,
        dimensions=("skymap",),
        storageClass="SkyMap",
    )

    fakeCat = cT.Input(
        doc="Unpartitioned fake catalogs.",
        name="fakes",
        storageClass="Catalog",
        dimensions=(),
        deferLoad=True,
        multiple=True,
    )

    partitionedFakes = cT.Output(
        doc="Fakes partitioned by tract",
        name="fakes_partitioned",
        storageClass="DataFrame",
        dimensions=("skymap", "tract"),
        multiple=True,
    )

class PartitionFakesConfig(PipelineTaskConfig, pipelineConnections=PartitionFakesConnections):
    pass

class PartitionFakesTask(PipelineTask):
    _DefaultName = "partitionFakes"
    ConfigClass = PartitionFakesConfig

    def run(self, skyMap, fakeCat):
        print(skyMap)
        print(fakeCat)
        # determine the tract assignment of each fake based on ra/dec

        fakes = Table()
        for deferred in fakeCat:
            print(dir(deferred))
            catalog = deferred.butler.get(deferred.ref)
            fakes = vstack([fakes, catalog.asAstropy()])

        tracts = skyMap.findTractIdArray(fakes['RA'], fakes['DEC'], degrees=True)

        outputCats = {}
        for tract in set(tracts):
            print("subsetting tract", tract)
            subset = fakes[tracts == tract]
            _none = [None] * len(subset)
            _true = [True] * len(subset)
            outputCats[tract] = pd.DataFrame(
                dict(
                    ra=subset['RA'] * np.pi/180,
                    dec=subset['DEC'] * np.pi/180,
                    bulge_semimajor=_none,
                    bulge_axis_ratio=_none,
                    bulge_pa=_none,
                    bulge_n=_none,
                    disk_semimajor=_none,
                    disk_axis_ratio=_none,
                    disk_pa=_none,
                    disk_n=_none,
                    bulge_disk_flux_ratio=_none,
                    trail_length=_none,
                    trail_angle=_none,
                    select=_true,
                    VR_mag=subset['MAG'],
                    sourceType=["star"] * len(subset),
                    visit=subset['EXPNUM'], # convert to visit?
                    ORBITID=subset['ORBITID'],
                    CCDNUM=subset['CCDNUM'],
                )
            )

        return outputCats

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)

        print("inputs=", inputs)
        print("outputRefs")
        print(outputRefs) # all of the tracts in the skymap

        runOutputs = self.run(**inputs)
        if runOutputs:
            tracts = [ref.dataId['tract'] for ref in outputRefs.partitionedFakes]
            outputs = [runOutputs.get(tract, pd.DataFrame()) for tract in tracts] # trim outputs to just those 
            butlerQC.put(pipeBase.Struct(partitionedFakes=outputs), outputRefs)
