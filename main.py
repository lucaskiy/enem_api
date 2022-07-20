import pandas as pd
import time
import os
from jsonschema import validate
from utils.variables import schema, columns, final_columns, column_data_types
from utils.gcloud_object import GcloudObject
from utils.logger_print import print_log


class ExtractEnem(GcloudObject):

    def __init__(self):
        super().__init__()
        self.files_path = "./enem_files/"
        self.parquet_files_path = "./parquet_files2/"

    def proccess_enem_files(self) -> None:
        print(print_log("Starting process to read csv files and transform them into parquet!!"))
        files = os.listdir(self.files_path)

        for file in files:
            if file == ".gitignore":
                continue

            csv_path = str(self.files_path + file)
            self.read_csvs(file=file, csv_path=csv_path, chunksize=100000)

        print(print_log(f"All raw enem files were saved to parquet format"))
        time.sleep(5)

        save_data_to_bucket = self.upload_parquet_to_bucket()
        if save_data_to_bucket:
            time.sleep(5)
            self.load_bucket_data_to_big_query()


    def read_csvs(self, file:str, csv_path: str, chunksize=int) -> None:
        print(print_log(f"Reading File -> {file}"))
            
        chunk_reader = pd.read_csv(csv_path, encoding="latin-1", sep=';', chunksize=chunksize)

        for i, chunk in enumerate(chunk_reader):
            try:
                df_filename = f'{file.upper().strip(".CSV")}_part({i+1}).parquet'
                df = chunk.copy()
                df = df[columns]
                df = df.where(pd.notnull(df), -1)
                df = self.normalize_string_columns(df)
                df = self.data_treatment(df, file)

                valid_data = self.data_validation(df)
                if valid_data is True:
                    df = df.replace({-1.0: None})
                    df.columns = final_columns
                    self.save_new_file_as_parquet(df, df_filename)
                    
            except Exception as error:
                print(print_log(f"Something went wrong while reading chunk -> {i}"))
                raise error

        print(print_log(f"{i} total files from {file} were converted from csv to parquet successfully!!"))

    def save_new_file_as_parquet(self, df: pd.DataFrame, df_filename: str) -> None:
        try:
            df.to_parquet(path=self.parquet_files_path + df_filename, index=False)

        except Exception as error:
            print(print_log(f"Something went wrong while converting {df_filename} to parquet"))
            raise error

    @staticmethod
    def data_treatment(df: pd.DataFrame, file: str) -> pd.DataFrame:
        df_copy = df.copy()

        try:
            df_copy = df_copy.astype(column_data_types)
            return df_copy

        except Exception as error:
            print(print_log(f"Something went wrong while at the data treatment method on file - {file}"))
            raise error            
    
    @staticmethod
    def data_validation(df: pd.DataFrame) -> bool:
        enem_data = df.to_dict("records")

        for row in enem_data:
            try:
                validate(row, schema)
                return True
            except Exception as error:
                print(print_log(row))
                raise error

    @staticmethod
    def normalize_string_columns(df: pd.DataFrame) -> pd.DataFrame:
        string_columns = ["TP_SEXO", "NO_MUNICIPIO_PROVA", "SG_UF_PROVA"]
        df_copy = df.copy()

        for column in list(df.columns):
            if column in string_columns:
                df_copy[column] = df_copy[column].str.normalize('NFKD') \
                                    .str.encode('ascii', 'ignore') \
                                    .str.decode('utf-8') \
                                    .str.upper()

        return df_copy

    def upload_parquet_to_bucket(self) -> bool:
        print(print_log("Starting process to upload parquet files to GCP storage"))
        files = os.listdir(self.parquet_files_path) 
        bucket = "files-etl"

        files_already_uploaded_to_bucket = self.get_files_inside_bucket(bucket)
        count = 1

        for file in files:
            try:
                if file in files_already_uploaded_to_bucket or file == ".gitignore":
                    continue

                self.upload_to_bucket(blob_name=file, path_to_file=self.parquet_files_path+file, bucket_name=bucket)
                print(print_log(f"Uploading file {count} of {len(files)} to bucket"))
                count += 1 
                os.remove(self.parquet_files_path+file)  # delete parquet file after uploading it to bucket
                time.sleep(1)
                
            except Exception as error:
                raise error

        print(print_log("All files uploaded successfully!!"))
        return True
