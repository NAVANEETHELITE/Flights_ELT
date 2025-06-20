import logging

def get_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        filename="upsert_log.log")
    logger = logging.getLogger()
    return logger

class Dev:
    server = 'localhost'
    database = 'ETL'
