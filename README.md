# PDF Parser and Database Loader

## Overview

This Python script is designed for extracting chemical composition data from PDF documents, processing the data, and then loading it into a SQL database. It makes use of several powerful libraries and services including `pandas` for data manipulation, `boto3` for interacting with AWS S3 and Textract services, and `pyodbc` for database connectivity. The script operates in a highly efficient manner, leveraging concurrent futures for parallel processing and is structured to handle specific data extraction patterns related to the chemical industry.

## Features

- **AWS S3 Integration**: Upload PDF files to AWS S3 buckets for storage and management.
- **AWS Textract Usage**: Extract text and data from PDF files stored in S3 buckets using AWS Textract.
- **Data Processing**: Process extracted data to identify chemical composition and bundle information.
- **Database Connectivity**: Connect to SQL databases using `pyodbc` to load processed data for further use.
- **Concurrent Processing**: Utilize `ThreadPoolExecutor` for improved efficiency in processing multiple PDFs.

## Requirements

- Python 3.x
- AWS account with access to S3 and Textract
- SQL Server database
- `pandas`, `boto3`, `pyodbc`, `glob`

## Setup

1. Install the required Python libraries:

```bash
pip install pandas boto3 pyodbc
```

2. Configure AWS CLI with your credentials:

```bash
aws configure
```

3. Set the following environment variables for AWS access, SQL database connectivity, and S3 bucket details:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `S3_BUCKET_TT`
- `S3_TT_DIR`
- `SQL_SERVER`
- `SQL_DB`
- `SQL_USERNAME`
- `SQL_PASSWORD`
- `SQL_CC_TABLE`

## Usage

1. Place the PDF files in a local directory.
2. Run the script to process and upload PDFs to S3, extract and process data, and finally load it into the SQL database:

```python
python script_name.py
```

## Classes and Methods

- `PDFParser`: Handles the uploading of PDFs to S3, extraction of data using Textract, and processing of extracted data.
- `DBConnector`: Manages database connectivity and operations.
- `load_dfs_to_db`: Function to load data from pandas DataFrame into the SQL database.

## Customization

Modify the `PDFParser` and `DBConnector` classes as needed to accommodate different PDF structures, database schemas, or AWS configurations.
