import pytest
from cli import ConsoleUtility
from pathlib import Path
import logging
import os


# Tests
# 1 Data Types

@pytest.mark.parametrize("data_type, expected",
                         [("{\"date\": \"timestamp:\"}", True), ("{\"name\": \"str:'Anna'\"}", True),
                          ("{\"age\": \"int:rand(0,100)\"}", True), ("{\"id\": \"str:rand:\"}", True),
                          ("{\"salary\": \"float:rand\"}", False),
                          ("{\"experience\": \"list:['intern','junior','mid','senior']\"}", False),
                          ("{\"None\": \"NoneType:\"}", False), ("{\"Plan to extend contract\": \"bool:\"}", False)])
def test_data_types(data_type, expected):
    new_Cli = ConsoleUtility(path='/Users/mjarzabkowski/PycharmProjects/PYTHON-BASIC/practice/data_generator_project',
                             file_count=5, file_name='data', file_prefix='count', data_schema=data_type, data_lines=10,
                             clear_path=True, multiprocessing=1)
    new_schema = new_Cli.convert_str_to_dict(data_type)
    assert new_Cli.validate_schema(new_schema) == expected


# 2 Data schemas
@pytest.mark.parametrize("data_schema, expected", [(
        "{\"date\": \"timestamp:\",\"name\": \"str:rand\",\"type\": \"['client', 'partner', 'government']\","
        "\"age\": \"int:rand(1, 100)\"}", dict),
    ("{\"name\": \"str:'Mikolaj'\",\"surname\": \"str:'Jarzabkowski'\",\"level\": \"['intern', 'junior', 'mid', "
     "'senior']\",\"age\": \"int:rand(1, 100)\"}", dict),
    ("{\"CompanyId\": \"str:rand\",\"CompanyCode\": \"int:rand\",\"Employees\": \"['Miko1', 'Miko2', 'Miko3']\","
     "\"TotalSalaries\": \"int:rand\"}", dict)])
def test_data_schemas(data_schema, expected, caplog):
    new_Cli = ConsoleUtility(path='/Users/mjarzabkowski/PycharmProjects/PYTHON-BASIC/practice/data_generator_project',
                             file_count=5, file_name='data', file_prefix='count', data_schema=data_schema,
                             data_lines=10,
                             clear_path=True, multiprocessing=1)
    new_schema = new_Cli.convert_str_to_dict(data_schema)
    assert type(new_schema) is expected


# 3 temp file
@pytest.fixture
def tmppath(tmpdir):
    return Path(tmpdir)


def test_temp_file_fixture(tmpdir, tmppath):
    p = tmpdir.mkdir("sub").join('temp_js.json')
    p.write(
        "{\"date\": \"timestamp:\",\"name\": \"str:rand\",\"type\": \"['client', 'partner', 'government']\","
        "\"age\": \"int:rand(1, 90)\"}")
    new_Cli = ConsoleUtility(path='/Users/mjarzabkowski/PycharmProjects/PYTHON-BASIC/practice/data_generator_project',
                             file_count=5, file_name='data', file_prefix='count', data_schema=p, data_lines=10,
                             clear_path=True, multiprocessing=1)
    data_schema = new_Cli.load_schema(p)
    assert type(data_schema) is dict


# 4 Check clear path
def test_clear_path(caplog):
    caplog.set_level(logging.INFO)
    new_cli = ConsoleUtility(path='/Users/mjarzabkowski/PycharmProjects/PYTHON-BASIC/practice/data_generator_project',
                             file_count=5, file_name='data', file_prefix='count',
                             data_schema="{\"date\": \"timestamp:\",\"name\": \"str:rand\",\"type\": \"['client', "
                                         "'partner', 'government']\",\"age\": \"int:rand(1, 90)\"}",
                             data_lines=10, clear_path=True, multiprocessing=1)
    new_cli.clear_path_and_files(path='/Users/mjarzabkowski/PycharmProjects/PYTHON-BASIC/practice/data_generator_project')
    assert 'Removed files that match filename' in caplog.text


# 5 Check saving files to disk
@pytest.fixture
def tmppath(tmpdir):
    return Path(tmpdir)


def test_check_files(tmpdir, tmppath):
    p = tmpdir.mkdir("sub").join('temp_js.json')
    p.write("{\"date\": \"timestamp:\",\"name\": \"str:rand\",\"type\": \"['client', 'partner', 'government']\","
            "\"age\": \"int:rand(1, 90)\"}")
    cli = ConsoleUtility(path='/Users/mjarzabkowski/PycharmProjects/PYTHON-BASIC/practice/data_generator_project',
                         file_count=5, file_name='data', file_prefix='count',
                         data_schema="{\"date\": \"timestamp:\",\"name\": \"str:rand\",\"type\": \"['client', "
                                     "'partner', 'government']\",\"age\": \"int:rand(1, 90)\"}",
                         data_lines=10, clear_path=True, multiprocessing=1)
    data_schema = cli.load_schema(p)
    cli.data_schema = data_schema
    cli.generate_jsonl_loop()
    _, _, files = next(os.walk(cli.path))
    assert len(files) - 4 == cli.file_count  # XD zrobic zmienna liczaca ilosc plikow w folderze czy zmienic kod?


# 6 Check number of files generated if multiprocessing > 1
@pytest.fixture
def tmppath(tmpdir):
    return Path(tmpdir)


def test_check_files_multiprocessing(tmpdir, tmppath):
    p = tmpdir.mkdir("sub").join('temp_js.json')
    p.write("{\"date\": \"timestamp:\",\"name\": \"str:rand\",\"type\": \"['client', 'partner', 'government']\","
            "\"age\": \"int:rand(1, 90)\"}")
    cli = ConsoleUtility(path='/Users/mjarzabkowski/PycharmProjects/PYTHON-BASIC/practice/data_generator_project',
                         file_count=5, file_name='data', file_prefix='count',
                         data_schema="{\"date\": \"timestamp:\",\"name\": \"str:rand\",\"type\": \"['client', "
                                     "'partner', 'government']\",\"age\": \"int:rand(1, 90)\"}",
                         data_lines=10, clear_path=True, multiprocessing=2)
    data_schema = cli.load_schema(p)
    cli.data_schema = data_schema
    cli.generate_jsonl_loop()
    _, _, files = next(os.walk(cli.path))
    assert len(files)-4 == cli.file_count # to samo co u gory


# 7 Own test - Parametrized test that takes data schema and checks if schema is dict and writes correct number of files
@pytest.mark.parametrize("data_schema, expected", [("{\"date\": \"timestamp:\",\"name\": \"str:rand\",\"type\": \"["
                                                    "'client', 'partner', 'government']\",\"age\": \"int:rand(1, "
                                                    "90)\"}",
                                                   dict),
                                                   (
                                                   "{\"date\": \"timestamp:\",\"name\": \"str:rand\",\"type\": \"["
                                                   "'client', 'partner', 'government']\",\"age\": \"int:rand(1, "
                                                   "90)\"}",
                                                   dict)])
def test_own(data_schema, expected):
    cli = ConsoleUtility(path='/Users/mjarzabkowski/PycharmProjects/PYTHON-BASIC/practice/data_generator_project',
                         file_count=5, file_name='data', file_prefix='count',
                         data_schema="{\"date\": \"timestamp:\",\"name\": \"str:rand\",\"type\": \"['client', "
                                     "'partner', 'government']\",\"age\": \"int:rand(1, 90)\"}",
                         data_lines=10, clear_path=True, multiprocessing=1)
    schema = cli.convert_str_to_dict(data_schema)
    cli.data_schema = schema
    cli.validate_schema(cli.data_schema)
    cli.generate_jsonl_loop()
    _, _, files = next(os.walk(cli.path))
    assert type(cli.data_schema) is dict
    assert len(files)-4 == cli.file_count # to samo
