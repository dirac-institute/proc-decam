
from typing import TYPE_CHECKING, List

import os

from parsl.executors.base import ParslExecutor
from parsl.launchers import SrunLauncher

from lsst.ctrl.bps.parsl.configuration import get_bps_config_value
from lsst.ctrl.bps.parsl.sites import Slurm

__all__ = ("Hyak",)

class Hyak(Slurm):
    def get_executors(self) -> List[ParslExecutor]:    
        max_blocks = get_bps_config_value(self.site, "max_blocks", int, 2)
        return [
            self.make_executor(
                "hyak",
                nodes=1,
                provider_options=dict(
                    init_blocks=1,
                    min_blocks=1,
                    max_blocks=max_blocks,
                    parallelism=1.0,
                    launcher=SrunLauncher(overrides="-K0 -k"),
                    exclusive=False,
                    worker_init=f"source {os.path.join(os.environ.get('DEEP_PROJECT_DIR', '/gscratch/dirac/DEEP'), 'bin/setup.sh')}",
                ),
            )
        ]

    def select_executor(self, job: "ParslJob") -> str:
        """Get the ``label`` of the executor to use to execute a job

        Parameters
        ----------
        job : `ParslJob`
            Job to be executed.

        Returns
        -------
        label : `str`
            Label of executor to use to execute ``job``.
        """
        return "hyak"
