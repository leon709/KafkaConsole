import os
import logging
from logging.handlers import RotatingFileHandler as RFHandler

LOG_PATH = "/tmp/logs/"
def get_logger(tag="test"):
    logger = logging.getLogger("clogger")
    
    if not os.path.exists(os.path.join(LOG_PATH)):
        os.makedirs(os.path.join(LOG_PATH))
    
    logfile = os.path.join(LOG_PATH) + '%s.log' % tag
    
    fh = RFHandler(logfile, maxBytes=1024 * 1024 * 100, backupCount=10, delay=0.05)
    formatter = logging.Formatter('[%(asctime)s %(levelno)s] %(message)s')
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)
    
    logger.addHandler(fh)
    return logger