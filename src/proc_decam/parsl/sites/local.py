
from typing import TYPE_CHECKING, List

import os

from lsst.ctrl.bps.parsl.configuration import get_bps_config_value
from lsst.ctrl.bps.parsl.sites import Local as bps_Local
from parsl.executors import HighThroughputExecutor
from parsl.executors.base import ParslExecutor
from parsl.providers import LocalProvider
import parsl.config


__all__ = ("Local",)

class Local(bps_Local):
    def get_executors(self) -> List[ParslExecutor]:    
        cores = int(os.environ.get("J", get_bps_config_value(self.site, "cores", int, required=True)))
        return [
            HighThroughputExecutor(
                "local", 
                provider=LocalProvider(
                    min_blocks=0,
                    max_blocks=1,
                    worker_init=f"""
source {os.path.join(os.getcwd(), 'etc/worker_setup.sh')}
""",
                ), 
                max_workers=cores,
                worker_debug=True,
            )
        ]
    
    def get_parsl_config(self) -> parsl.config.Config:
        """Get Parsl configuration for using CC-IN2P3 Slurm farm as a
        Parsl execution site.

        Returns
        -------
        config : `parsl.config.Config`
            The configuration to be used to initialize Parsl for this site.
        """
        executors = self.get_executors()
        monitor = self.get_monitor()
        retries = get_bps_config_value(self.site, "retries", int, 1)
        run_dir = get_bps_config_value(self.site, "run_dir", str, "runinfo")
        # Strategy for scaling blocks according to workflow needs.
        # Use a strategy that allows for scaling in and out Parsl
        # workers.
        strategy = get_bps_config_value(self.site, "strategy", str, "htex_auto_scale")
        return parsl.config.Config(
            executors=executors,
            monitoring=monitor,
            retries=retries,
            checkpoint_mode="task_exit",
            run_dir=run_dir,
            strategy=strategy,
        )

