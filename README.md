# Data Generation Utility for Testing Data Pipelines

# Introduction
When working with data pipelines, it's essential to validate the correctness of data transformations and validations. To facilitate this process, we've developed a versatile console utility as part of our capstone project. This utility is designed to generate test data in JSON format, providing the necessary input variety for comprehensive testing.
# Features
* Generate diverse input data for testing data pipelines.
* Ensure data transformation and validation correctness.
* Output data in a structured JSON format.

# Usage

Example of CU launch with provided data_schema from cmd:  
$ cli.py . --file_count=3 --file_name=super_data --prefix=count --multiprocessing=4 --data_schema="{\"date\": \"timestamp:\", \"name\": \"str:rand\", \"type\": \"['client', 'partner', 'goverment']\", \"age\": \"int:rand(1,90)\"}"  
  Data schema from file:  
$ cli.py . --file_count=3 --file_name=super_data --prefix=count --data_schema=./path/to/schema.json
