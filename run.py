"""
Running the Xetra ETL application
"""

import logging
import logging
from logging.config import dictConfig





import yaml

def main():
    """"
        entry point to run the Xetra ETL job
    """
    #Parsing YAML config file
    config_path = '/Users/maikhanhhuyen/xetra/xetra_1234/configs/xetra_report1_config.yml'
    config = yaml.safe_load(open(config_path))

    # print(config)

    # configure logging
    log_config = config['logging']
    dictConfig(log_config)
    logger = logging.getLogger(__name__)
    logger.info("This is an info message")

if __name__ == '__main__':
    main()


