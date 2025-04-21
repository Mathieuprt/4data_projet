import logging
import os
from dagster import logger

@logger
def file_logger(init_context):
    logger = logging.getLogger("dagster_file_logger")
    logger.setLevel(logging.INFO)

    # Affiche dans la console où on s'attend à voir le fichier
    init_context.log.info(f"Initialisation du logger. Dossier courant : {os.getcwd()}")

    if not logger.handlers:
        log_path = os.path.join(os.getcwd(), "dagster.log")
        handler = logging.FileHandler(log_path, mode='a', encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s — %(levelname)s — %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger
