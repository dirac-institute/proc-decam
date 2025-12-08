import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase
import lsst.pipe.base.connectionTypes as cT

class ApplyDefectsConnections(pipeBase.PipelineTaskConnections, dimensions=("instrument", "exposure", "detector")):
    ccdExposure = cT.Input(
        doc="Input exposure",
        name="postISRCCD",
        dimensions=("instrument", "exposure", "detector"),
        storageClass="Exposure",
    )

    defects = cT.PrerequisiteInput(
        name='defects',
        doc="Input defect tables.",
        storageClass="Defects",
        dimensions=["instrument", "detector"],
        isCalibration=True,
    )

    outputExposure = cT.Output(
        name='postISRCCD_masked',
        doc="Output exposure with mask applied.",
        storageClass="Exposure",
        dimensions=["instrument", "exposure", "detector"],
    )

class ApplyDefectsConfig(pipeBase.PipelineTaskConfig, pipelineConnections=ApplyDefectsConnections):
    maskPlane = pexConfig.Field(
        dtype=str,
        doc="Mask plane name",
        default="BAD",
    )

class ApplyDefectsTask(pipeBase.PipelineTask):
    _DefaultName = "applyDefects"
    ConfigClass = ApplyDefectsConfig

    def run(self, ccdExposure, defects):
        defects.maskPixels(ccdExposure.getMaskedImage(), self.config.maskPlane)
        return pipeBase.Struct(
            outputExposure=ccdExposure
        )

