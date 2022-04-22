import boto3
import pandas as pd

# Criar um cliente para interagir com a AWS S3
s3_client = boto3.client('s3')

s3_client.download_file('datalake-rafael-igti-edc',
                        'data/spotify_top50_2021.csv',
                        'data/spotify_top50_songs.csv')

df = pd.read_csv("data/spotify_top50_songs.csv")
print(df)

s3_client.upload_file("name",
                      "datalake-rafael-igti-edc",
                      "data/filename.csv")