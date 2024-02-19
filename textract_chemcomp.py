import os
import re
import pandas as pd
import boto3
from glob import glob
import pyodbc
from concurrent.futures import ThreadPoolExecutor

class PDFParser:
    def __init__(self):
        self.AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID")
        self.AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
        self.AWS_REGION = 'us-west-1'

        self.s3 = boto3.resource('s3', region_name=self.AWS_REGION,
                                 aws_access_key_id=self.AWS_ACCESS_KEY, aws_secret_access_key=self.AWS_SECRET_ACCESS_KEY)
        self.textract = boto3.client('textract', region_name=self.AWS_REGION, aws_access_key_id=self.AWS_ACCESS_KEY,
                                      aws_secret_access_key=self.AWS_SECRET_ACCESS_KEY)

        self.cast_num_template = {"SupplierNum": "", "Alloy": "6063", "AlloyType": "F", "Diameter": 4.5, "PassFlag": 1, "Other": 0, "OtherPrefix": "="}
        self.elements = ["Si", "Fe", "Cu", "Mn", "Mg", "Cr", "Ni", "Zn", "Ti", "Ga", "V", "B", "Zr", "Bi", "Pb", "Sn", "Sr", "Co", "Al"]
        self.prefix = ["="] * len(self.elements)

    def upload_pdfs_to_s3(self, local_directory, s3_bucket_name, s3_directory):
        pdf_files = glob(os.path.join(local_directory, "*.pdf"))
        for pdf_file in pdf_files:
            s3_path = os.path.join(s3_directory, os.path.basename(pdf_file))
            self.s3.meta.client.upload_file(pdf_file, s3_bucket_name, s3_path)

    def extract_data_from_pdf(self, s3_bucket_name, pdf_key):
        s3_object = self.s3.Object(s3_bucket_name, pdf_key)
        s3_object_data = s3_object.get()['Body'].read()
        response = self.textract.analyze_document(Document={'Bytes': s3_object_data}, FeatureTypes=["TABLES"])

        raw_text = ""
        for item in response["Blocks"]:
            if item["BlockType"] == "LINE":
                raw_text += item["Text"] + "\n"
        print(raw_text)
        return self.process_raw_text(raw_text)

    def process_raw_text(self, raw_text):
        analysis_pattern = re.compile(r'(\d+-\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+)')
        analysis_rows = analysis_pattern.findall(raw_text)

        cast_bundle_pattern = re.compile(r'(\d+)\s+([\d,]+)(?:\s+(\d+\w+\d+\s+\w+))?(?:\s+(\d+\w+\d+\s+\w+))?(?:\s+-){,3}\s+(\d+)')

        cast_bundle_rows = cast_bundle_pattern.findall(raw_text)

        # Custom processing to handle multiple cast numbers per line
        clean_cast_bundle_rows = []
        for row in cast_bundle_rows:
            for i in range(2, 4):
                if row[i]:
                    clean_cast_bundle_rows.append((row[0], row[1], row[i].replace(' ', ''), row[4]))

        analysis_df = pd.DataFrame(analysis_rows, columns=["NO.", "Si", "Fe", "Cu", "Mn", "Mg", "Zn", "Ti", "Cr", "Ni", "Pb", "Al", "PCS"]).set_index("NO.")

        cast_and_bundle_df = pd.DataFrame(clean_cast_bundle_rows, columns=["BUNDLES_NUMBER", "NET_WEIGHT", "CAST_NUMBER", "PCS"]).set_index("BUNDLES_NUMBER")

        return analysis_df, cast_and_bundle_df


    def create_final_dataframe(self, cast_number, chemical_comp_df, cast_and_bundle_df):
        # Handle bundle numbers without a hyphen
        bundle_number = cast_and_bundle_df[cast_and_bundle_df['CAST_NUMBER'] == cast_number].index[0]
        range_numbers = [int(x) for x in re.findall(r'\d+', bundle_number)]
        range_min, range_max = range_numbers[0], range_numbers[-1]

        # Find the closest match in chemical_comp_df
        closest_match = None
        min_diff = float('inf')
        for index in chemical_comp_df.index:
            start, end = [int(x) for x in re.findall(r'\d+', index)]
            if start <= range_min <= end or start <= range_max <= end:
                closest_match = index
                break

            current_diff = abs(start - range_min)
            if current_diff < min_diff:
                min_diff = current_diff
                closest_match = index

        # Retrieve available element values
        element_vals = {}
        for element in self.elements:
            if element in chemical_comp_df.columns:
                element_vals[element] = chemical_comp_df.loc[closest_match][element]
            else:  # Set missing elements to zero
                element_vals[element] = 0

        final_data = self.cast_num_template.copy()
        final_data.update({"CastNum": cast_number, **element_vals})
        final_data.update({element + "Prefix": prefix for element, prefix in zip(self.elements, self.prefix)})

        final_df = pd.DataFrame(final_data, index=[0])
        return final_df

    s3_bucket_name = os.environ.get("S3_BUCKET_TT")
    s3_directory = os.environ.get("S3_TT_DIR")
  
    def main(self, local_directory='./', s3_bucket_name=s3_bucket_name, s3_directory=s3_directory):
        self.upload_pdfs_to_s3(local_directory, s3_bucket_name, s3_directory)

        pdf_files = glob(os.path.join(local_directory, "*.pdf"))
        final_df = pd.DataFrame()
        for pdf_file in pdf_files:
            s3_pdf_key = os.path.join(s3_directory, os.path.basename(pdf_file))
            chemical_comp_df, cast_and_bundle_df = self.extract_data_from_pdf(s3_bucket_name, s3_pdf_key)

            for cast_number in cast_and_bundle_df['CAST_NUMBER'].unique():
                current_df = self.create_final_dataframe(cast_number, chemical_comp_df, cast_and_bundle_df)
                final_df = pd.concat([final_df, current_df], ignore_index=True)

        # Reorder the columns as requested
        final_ordered_columns = ["CastNum", "SupplierNum", "Alloy", "AlloyType", "Diameter",
                                 "Si", "Fe", "Cu", "Mn", "Mg", "Cr", "Ni", "Zn", "Ti", "Ga", "V", "B", "Zr", "Bi", "Pb", "Sn", "Sr", "Co", "Al", "Other", "PassFlag",
                                 "SiPrefix", "FePrefix", "CuPrefix", "MnPrefix", "MgPrefix", "CrPrefix", "NiPrefix", "ZnPrefix", "TiPrefix", "GaPrefix", "VPrefix", "BPrefix", "ZrPrefix", "BiPrefix", "PbPrefix", "SnPrefix", "SrPrefix", "CoPrefix", "AlPrefix", "OtherPrefix"]
        final_df = final_df[final_ordered_columns]
        final_df.to_csv("final_df_18-19.csv", index=False)

        final_df.drop_duplicates(subset=['CastNum'], inplace=True)
        print(final_df)
        return final_df


class DBConnector:
    def __init__(self):
        self.server = os.environ.get("SQL_SERVER")
        self.database = os.environ.get("SQL_DB")
        self.username = os.environ.get("SQL_USERNAME")
        self.password = os.environ.get("SQL_PASSWORD")
        self.driver = "{ODBC Driver 17 for SQL Server}"
        self.connection = None

    def connect(self):
        self.connection = pyodbc.connect(
            f"DRIVER={self.driver};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password}"
        )

    def get_connection(self):
        if not self.connection:
            self.connect()
        return self.connection

def load_dfs_to_db(df, db_connector, write_table):
    connection = db_connector.get_connection()
    cursor = connection.cursor()

    # Convert DataFrame to a list of tuples
    rows = [tuple(row) for row in df.itertuples(index=False)]

    # Extract column names and create placeholders for the values
    columns = ','.join(df.columns)
    placeholders = ','.join(['?'] * len(df.columns))

    # Create the INSERT query
    query = f"INSERT INTO {write_table} ({columns}) VALUES ({placeholders})"
    
    try:
        # Batch insert records to the database
        cursor.executemany(query, rows)
        connection.commit()
        
        print(f"Inserted {len(rows)} rows into {write_table}")
    except Exception as e:
        print(f"Error occurred while writing dataframe to table {write_table}: {e}")
        connection.rollback()



if __name__ == "__main__":
    pdf_parser = PDFParser()
    final_df = pdf_parser.main()
    
    # Update the write_table variable to the desired table name
    write_table = os.environ.get("SQL_CC_TABLE")
    db_connector = DBConnector()

    load_dfs_to_db(final_df, db_connector, write_table)
    
