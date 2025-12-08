import logging
from parsl import AUTO_LOGNAME
from .providers import *

logging.basicConfig()
logger = logging.getLogger(__name__)

def run_command(cmd, inputs=(), outputs=(), stdout=AUTO_LOGNAME, stderr=AUTO_LOGNAME):
    logger.info("running %s inputs=%s outputs=%s stdout=%s stderr=%s ", cmd, inputs, outputs, stdout, stderr)
    return cmd
