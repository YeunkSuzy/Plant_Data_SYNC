import logging
import os
from datetime import datetime
# configure logging
def setup_logging():
    # create logs directory (if not exist)
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    # generate log file name (include date)
    log_filename = f"logs/plant_sync_{datetime.now().strftime('%Y%m%d')}.log"
    
    # configure log format
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()  # output to console
        ]
    )
    
    return logging.getLogger(__name__)

def setup_verify_logging(table_name, id_start, id_end):
    # create logs directory (if not exist)
    log_dir = f"verify_log/{table_name}"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    # generate log file name (include date)
    log_path = f"{log_dir}/{id_start}_{id_end}.log"
    # create logger
    logger = logging.getLogger("verify_logger")
    # set log level
    logger.setLevel(logging.INFO)
    # clear old handler, avoid duplicate
    if logger.hasHandlers():
        logger.handlers.clear()
    # create file handler
    file_handler = logging.FileHandler(log_path, encoding='utf-8', mode='w')
    file_handler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(file_handler)
    # create console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(console_handler)
    return logger, log_path