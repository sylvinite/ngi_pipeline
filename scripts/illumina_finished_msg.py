import argparse
import pika

from ngi_pipeline.distributed import CeleryMessenger
from ngi_pipeline.utils.config import load_yaml_config

def main(config_file):
    config = load_yaml_config(config_file)
    messenger = CeleryMessenger(config.get('celery'), 'ngi_pipeline')
    messenger.send_message('launch_main_analysis',
                           '/Users/guillem/archive/test_data_empty_files')

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("config", help="The path to the configuration file.")
    args = parser.parse_args()
    main(args.config)
