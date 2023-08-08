import configparser

config = configparser.ConfigParser()
config['default'] = {
    'path_to_save_files': '.',
    'files_count': '1',
    'file_name': 'file',
    'file_prefix': 'prefix',
    'data_schema': 'schema',
    'data_lines': '1',
    'clear_path': 'False',
    'multiprocessing': '1'
}


def read_config():
    with open('default.ini', 'w') as configfile:
        config.write(configfile)
