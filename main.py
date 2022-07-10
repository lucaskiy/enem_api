import pandas as pd
import os
from jsonschema import validate
from utils.variables import schema, columns, final_columns
from utils.gcloud_object import GcloudObject


class ExtractEnem(GcloudObject):

    def __init__(self):
        super().__init__()
        self.files_path = "./enem_files/"
        self.parquet_files_path = "./parquet_files/"


    def read_csvs(self) -> None:
        files = os.listdir(self.files_path)

        for file in files:
            csv_files = str(self.files_path + file)
            chunk_reader = pd.read_csv(csv_files, encoding="latin-1", sep=';', chunksize=50000)

            for i, chunk in enumerate(chunk_reader):
                try:
                    df_filename = f'{file.strip(".csv")}_part({i+1}).parquet'
                    df = chunk.copy()
                    df = df[columns]
                    df = df.where(pd.notnull(df), -1)
                    df = self.normalize_string_columns(df)

                    valid_data = self.data_validation(df)
                    if valid_data is True:
                        df = df.replace({-1.0: None})
                        df.columns = final_columns
                        self.save_new_file_as_parquet(df, df_filename)
                        
                except Exception as error:
                    print(f"Something went wrong while reading chunk -> {i}")
                    raise 

    def save_new_file_as_parquet(self, df: pd.DataFrame, df_filename: str) -> None:
        try:
            df.to_parquet(path=self.parquet_files_path + df_filename, index=False)
        except Exception as error:
            print(f"Something went wrong while converting {df_filename} to parquet")
            raise error
    
    @staticmethod
    def data_validation(df: pd.DataFrame) -> bool:
        enem_data = df.to_dict("records")

        for row in enem_data:
            try:
                validate(row, schema)
                return True
            except Exception as error:
                print(row)
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

    def upload_parquet_to_bucket(self):
        print("Starting process to upload parquet files to GCP storage")
        files = os.listdir(self.parquet_files_path) 
        bucket = "files-etl"
        i = 0

        for file in files:
            try:
                self.upload_to_bucket(blob_name=file, path_to_file=self.parquet_files_path+file, bucket_name=bucket)
                print(f"Uploading file {i+1} of {len(files)} to bucket")
                i += 1 
            except Exception as error:
                raise error

        print("All files uploaded successfully!!")

        
if __name__ == '__main__':
    x = ExtractEnem()
    x.upload_parquet_to_bucket()
    