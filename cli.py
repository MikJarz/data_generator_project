import argparse
import configparser
import logging
import os
import random
import sys
import json
import re
import time
import uuid
from multiprocessing import Pool

""" Setting up default values """
config = configparser.ConfigParser()
config.read('default.ini')
default = config['default']

""" Main Class"""


class ConsoleUtility:
    def __init__(self, path: str, file_count: int, file_name: str, file_prefix: str, data_schema: str, data_lines: int,
                 clear_path: bool, multiprocessing: int):
        self.path = path
        self.file_count = file_count
        self.file_name = file_name
        self.file_prefix = file_prefix
        self.data_schema = data_schema
        self.data_lines = data_lines
        self.clear_path = clear_path
        self.multiprocessing = multiprocessing

    def check_args(self):
        """ Checking if provided arguments are correct """
        file_prefixes = ['count', 'random', 'uuid']
        path_booleans = ['True', 'False']

        if self.path == '.':
            self.path = os.getcwd()
        if not os.path.exists(self.path):
            logging.error('Error: Directory does not exist.')
            return False
        if self.file_prefix not in file_prefixes:
            logging.error('Error: File prefix must be either "count", "random" or "uuid"')
            return False
        if self.clear_path not in path_booleans:
            logging.error('Error: Clear path only takes True or False values')
            return False
        if self.file_count < 0:
            logging.error('Error: Files count must be greater than 0')
            return False
        if self.multiprocessing <= 0:
            logging.error('Error: Multiprocessing must be greater than 0')
            return False
        if self.multiprocessing > os.cpu_count():
            self.multiprocessing = os.cpu_count()
            logging.info("Number of input processing units larger than amount of cpu processors, setting input to "
                         "cpu_count")
        return True

    def clear_path_and_files(self, path: str):
        """ Clearing directory path files """
        for file in os.listdir(path):
            if file.find(self.file_name) != -1:
                os.remove(path + '/' + file)
        logging.info('Removed files that match filename')

    def check_path_or_schema(self, path_to_schema):
        """ Checking if the provided schema is a string schema or JSON file """
        try:
            with open(path_to_schema, 'r') as file:
                path_to_schema = json.load(file)
            logging.info('Provided with JSON file schema')
        except:
            logging.info('Provided with command line schema')
            return False
        return True

    def load_schema(self, path_to_schema: str):
        """ Loading string with dict schema and returning dict object """
        with open(path_to_schema, 'r') as file:
            data_schema = json.load(file)
        return data_schema

    def is_list_regex(string):
        """ Returning true or false if regex finds a list """
        isList_regex = re.match(r'\[|\(|\]|\)', string)
        return (False, True)[bool(isList_regex)]

    def convert_str_to_list(self, my_str):
        """ Converting provided string to list """
        if my_str.find('[') != -1 and my_str.find(']') != -1:
            list_values = my_str[1:-1]
        return list_values.replace("'", '').replace(' ', '').split(',')

    def has_numbers(self, my_list: list):
        """ Returning true of false if number contains digit or not """
        digits = list(filter(lambda character: character.isdigit(), my_list))
        return len(digits) > 0

    def convert_str_to_dict(self, data_schema):
        """ Converting provided string to dict """
        try:
            result_schema = json.loads(data_schema)
        except ValueError:
            logging.warning('Error: Provided data schema is not valid json')
            sys.exit(1)
        return result_schema

    def validate_schema(self, schema: dict):
        """ Validating if provided schema has correct keys and values """
        logging.info('Validating keys and values')
        for key, value in schema.items():
            # Get both left and right value
            left_value = value.split(':')[0]
            try:
                right_value = value.split(':')[1]
            except:
                pass

            # Checking if left values are valid
            if left_value in ['timestamp', 'int', 'str']:
                if left_value.find('timestamp') != -1:
                    # Value has more attributes on the right side
                    if len(list(filter(None, value.split(":")))) > 1:  # Filter removes unnecessary '' produced by split
                        logging.warning(
                            'Error: Timestamp does not support any values and it will be ignored \'{}\':\'{}\''.format(
                                key,
                                value))
                    # Value is valid
                    else:
                        logging.info(
                            'Valid timestamp schema \'{}\':\'{}\''.format(key, left_value))

                # Checking if the value is a valid string
                if left_value.find('str') != -1:
                    # Checking if the rand type has a correct schema
                    if right_value.find('rand') != -1:
                        if right_value.find('(') != -1 and right_value.find(')') != -1:
                            logging.error('Error: Invalid rand schema \'{}\':\'{}\''.format(
                                key, left_value + ':' + right_value))
                            sys.exit(1)
                        else:
                            logging.info('Valid string schema for: \'{}\':\'{}:{}\''.format(
                                key, left_value, right_value))

                    # Checking if the list type has a correct schema
                    if ConsoleUtility.is_list_regex(right_value):
                        logging.info(
                            'List schema found, transforming string schema to actual list...')
                        # Converting str to list
                        right_value = ConsoleUtility.convert_str_to_list(self, right_value)
                        # Checking if the list is valid
                        if ConsoleUtility.has_numbers(self, right_value):
                            logging.error(
                                'Invalid list schema str:list should not contain numbers \'{}\':\'{}:{}\''.format(
                                    key, left_value, right_value))
                            sys.exit(1)
                        else:
                            logging.info('Valid list schema \'{}\':\'{}:{}\''.format(
                                key, left_value, right_value))
                    elif isinstance(right_value, str) and right_value != '':
                        logging.info('Valid stand alone value string schema \'{}\':\'{}:{}\''.format(
                            key, left_value, right_value))

                    # Checking if right value it's empty
                    if right_value == '':
                        logging.info('Valid empty schema, \'\' replacing with: \'{}\':\'{}:{}\''.format(
                            key, left_value, ''))

                # Checking if the value is a valid integer
                if left_value.find('int') != -1:

                    # Checking if the rand type has a correct schema
                    if right_value.find('rand') != -1:
                        if right_value.find('(') and right_value.find(')') != -1:
                            logging.info('Valid integer rand(from, to) schema for \'{}\':\'{}\''.format(
                                key, left_value + ':' + right_value))
                        else:
                            logging.info('Valid integer schema for: \'{}\':\'{}:{}\''.format(
                                key, left_value, right_value))

                    # Checking if the list type has a correct schema
                    if ConsoleUtility.is_list_regex(right_value):
                        logging.info(
                            'List schema found, transforming integer schema to actual list')
                        # Converting str to list
                        right_value = ConsoleUtility.convert_str_to_list(self, right_value)

                        # Checking if the list is valid
                        if not ConsoleUtility.has_numbers(right_value):
                            logging.error(
                                'Error:Invalid list schema int:list should not contain strings \'{}\':\'{}:{}\''.format(
                                    key, left_value, right_value))
                            sys.exit(1)
                        else:
                            logging.info('Valid list schema \'{}\':\'{}\':\'{}\''.format(
                                key, left_value, right_value))

                    # Checking if stand alone integer is valid
                    try:
                        int(right_value)
                        logging.info('Valid stand alone integer schema \'{}\':\'{}:{}\''.format(
                            key, left_value, right_value))
                    except:
                        pass

                    # Checking if right value it's empty
                    if right_value == '':
                        logging.info('Valid empty schema, None will replace it \'{}\':\'{}:{}\''.format(
                            key, left_value, None))

                return True
            return False

    def generate_jsonl_content(self, provided_schema: dict):
        """ Generating JSON content for each data line """
        result = {}
        for key, value in provided_schema.items():
            # Getting left and right value
            try:
                left_value = value.split(":")[0]
                right_value = value.split(":")[1]
            except:
                pass

            # Creating actual values
            # Checking timestamp types
            if left_value == 'timestamp':
                result[key] = str(time.time())
            # Checking string types
            elif left_value == 'str':
                # str:rand
                if right_value == 'rand':
                    result[key] = str(uuid.uuid4())
                # str:['a', 'b', 'c']
                elif ConsoleUtility.is_list_regex(right_value):
                    list_values = ConsoleUtility.convert_str_to_list(self, right_value)
                    result[key] = random.choice(list_values)
                # str:'cat' stand alone value
                elif isinstance(right_value, str) and right_value != '':
                    result[key] = str(right_value.replace('\'', ''))
                # str:'' empty value
                elif right_value == '':
                    result[key] = ''

            # Checking int types
            elif left_value == 'int':
                # int:rand
                if right_value.find('rand') != -1:
                    if right_value.find('rand(') != -1 and right_value.find(')') != -1:
                        regex = re.findall(r'\d+', right_value)
                        result[key] = random.randint(int(regex[0]), int(regex[1]))
                    else:
                        result[key] = random.randint(0, 100)
                # int:[1,2,3]
                elif ConsoleUtility.is_list_regex(right_value):
                    result[key] = random.choice(right_value)
                # int:1 stand alone value
                elif isinstance(right_value, int) and right_value != '':
                    result[key] = right_value
                # int:empty value
                elif right_value == '':
                    result[key] = 'None'
        return result

    def generate_jsonl(self, prefix):
        """ Generating a single JSON file """
        with open(self.path + '/' + self.file_name + '_' + prefix + '.jsonl', 'w') as file:
            for j in range(1, self.data_lines + 1):
                data = ConsoleUtility.generate_jsonl_content(self, provided_schema=self.data_schema)
                json.dump(data, file, ensure_ascii=True)
                if j <= self.data_lines - 1:
                    file.write('\n')

    def generate_jsonl_loop(self):
        """ Generating multiple JSON files """
        if self.file_prefix == 'count':
            for i in range(1, self.file_count + 1):
                prefix = str(i)
                ConsoleUtility.generate_jsonl(self, prefix)
            logging.info("Done generating files")

        elif self.file_prefix == 'random':
            for i in range(1, self.file_count + 1):
                prefix = str(random.randint(0, 100000))
                ConsoleUtility.generate_jsonl(self, prefix)
            logging.info("Done generating files")

        elif self.file_prefix == 'uuid':
            for i in range(1, self.file_count + 1):
                prefix = str(uuid.uuid4())
                ConsoleUtility.generate_jsonl(self, prefix)
            logging.info("Done generating files")

    def multiprocess_prefix_count(self):
        """ Multiprocess JSON file generator with count prefix """
        counter = 1
        ConsoleUtility.generate_jsonl(self, str(counter))
        counter += 1
        return 'Done generating {}'.format(self.file_count)

    def multiprocess_prefix_random(self):
        """ Multiprocess JSON file generator with random prefix """
        prefix = str(random.randint(0, 100000))
        ConsoleUtility.generate_jsonl(self, prefix)
        return 'Done generating {}'.format(self.file_count)

    def multiprocess_prefix_uuid(self):
        """ Multiprocess JSON file generator with uuid prefix """
        prefix = str(uuid.uuid4())
        ConsoleUtility.generate_jsonl(self, prefix)
        return 'Done generating {}'.format(self.file_count)

    def multiprocess_generate_jsonl(self):
        """ Generating JSON files using multiprocessing with provided prefix """
        if self.file_prefix == 'count':
            ConsoleUtility.multiprocess_prefix_count(self)
        elif self.file_prefix == 'random':
            pass
            ConsoleUtility.multiprocess_prefix_random(self)
        else:
            ConsoleUtility.multiprocess_prefix_uuid(self)


""" Main method """


def main():
    """ Set up argparse to handle command line arguments """
    parser = argparse.ArgumentParser(prog='Console Utility',
                                     description='Console utility for generating test data based '
                                                 'on the provided data schema.')
    parser.add_argument('--path_to_save_files', help='Path to save the generated files')
    parser.add_argument('--files_count', help='How many JSON files to generate', type=int)
    parser.add_argument('--file_name', help='File name prefix', type=str)
    parser.add_argument('--file_prefix', help='File prefix', type=str, choices=['count', 'random', 'uuid'])
    parser.add_argument('--data_schema', help='It’s a string with json schema. It could be loaded in two ways: '
                                              '\n1) With path to json file with schema '
                                              '\n2) with schema entered to command line.', type=str)
    parser.add_argument('--data_lines', help='Count of lines per each file', type=int)
    parser.add_argument('--clear_path', help='Clear the path before generating files', action="store_true")
    parser.add_argument('--multiprocessing', help='The number of processes used to create files', type=int)
    logging.basicConfig(level=logging.INFO)
    parser.set_defaults(**default)
    args = parser.parse_args()
    cli = ConsoleUtility(args.path_to_save_files, args.files_count, args.file_name, args.file_prefix, args.data_schema,
                         args.data_lines, args.clear_path, args.multiprocessing)

    if cli.check_args():
        if args.clear_path == 'True':
            cli.clear_path_and_files(path=args.path_to_save_files)
        if cli.multiprocessing != 1:
            logging.info('Multiprocessing enabled with {} processes'.format(cli.multiprocessing))
            if cli.check_path_or_schema(cli.data_schema):
                if os.path.exists(cli.data_schema):
                    logging.info('Opening data schema from JSON file')
                    async_results = []
                    schema = cli.load_schema(path_to_schema=args.data_schema)
                    cli.data_schema = schema
                    cli.validate_schema(schema)
                    pool = Pool(processes=cli.multiprocessing)
                    start = time.time()
                    for n_files in range(cli.file_count):
                        async_results.append(pool.apply_async(cli.multiprocess_generate_jsonl()))
                    pool.close()
                    pool.join()
                    logging.info('Time to generate {} files: {}'.format(cli.file_count, time.time() - start))
                else:
                    logging.error('Data schema does not exist')
                    sys.exit(1)
            else:
                logging.info('Opening data schema from provided schema')
                async_results = []
                schema = cli.convert_str_to_dict(cli.data_schema)
                cli.data_schema = schema
                cli.validate_schema(schema)
                pool = Pool(processes=cli.multiprocessing)
                start = time.time()
                for n_files in range(cli.file_count):
                    async_results.append(pool.apply_async(cli.multiprocess_generate_jsonl()))
                pool.close()
                pool.join()
                logging.info('Time to generate {} files: {}'.format(cli.file_count, time.time() - start))
        else:
            logging.info('Running with one process')
            if cli.check_path_or_schema(cli.data_schema):
                if os.path.exists(cli.data_schema):
                    logging.info('Opening data schema from JSON file')
                    schema = cli.load_schema(path_to_schema=args.data_schema)
                    cli.validate_schema(schema=schema)
                    start = time.time()
                    cli.generate_jsonl_loop()
                    logging.info("Time to generate {} files: {}".format(cli.file_count, time.time() - start))
                else:
                    logging.error('Data schema does not exist')
                    sys.exit(1)
            else:
                logging.info('Opening data schema from provided schema')
                cli.data_schema = cli.convert_str_to_dict(cli.data_schema)
                cli.validate_schema(cli.data_schema)
                start = time.time()
                cli.generate_jsonl_loop()
                logging.info("Time to generate {} files: {}".format(cli.file_count, time.time() - start))
    else:
        logging.error('Error: Invalid arguments')
        sys.exit(1)
    sys.exit(0)


if __name__ == '__main__':
    main()
