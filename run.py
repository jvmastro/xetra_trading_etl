""" Runnning the Xetra ETL application"""
import logging
import logging.config

import yaml


def main():
    """Entry point to run the xetra ETL job
    """
    # Parsing YAML file
    config_path = '/Users/johnmastronardo/Desktop/etl_production/xetra_trading_etl/configs/xerta_report1_config.yml'
    config = yaml.safe_load(open(config_path))
    # configure logging
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    logger.info('This is a test.')

    pass


if __name__ == '__main__':
    main()
