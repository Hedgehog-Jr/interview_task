import logging
import os
import re
import shutil
import yaml

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, arrays_zip, explode


logger = logging.getLogger(__name__)


def refactor_csv_file(input_file_name: str):
    """
    Moves and renames spark CSV file from tmp to output location
    :param input_file_name:
    """

    tmp_dir = os.environ['TMP_DIR']
    output_dir = os.environ['OUTPUT_DIR']
    src_dir = os.path.join(tmp_dir, input_file_name)
    new_name = f'{input_file_name[:-3]}csv'

    for filename in os.listdir(src_dir):
        if re.match(r'part-.*\.csv', filename):
            src_file = os.path.join(src_dir, filename)
            dst_file = os.path.join(output_dir, new_name)
            shutil.copy2(src_file, dst_file)


def get_app_config(config_path: str = 'config.yaml') -> dict:
    with open(config_path, 'r') as config_file:
        app_config = yaml.full_load(config_file).get('xml_path')

    return app_config


def get_spark_session() -> SparkSession:
    """
    Returns spark session
    :return:
    """
    jar_path = os.environ['JAR_PATH']
    spark = SparkSession.builder \
        .appName("Interview task 1") \
        .config("spark.jars", jar_path) \
        .getOrCreate()

    return spark


def delete_folder(folder_path: str):
    """
    Deletes a folder and all its contents.
    :param folder_path: Path to the folder to be deleted.
    """
    try:
        shutil.rmtree(folder_path)
    except Exception as e:
        logger.error(f"An error occurred while deleting the folder: {e}")


def write_df_to_tmp_csv(df: DataFrame, filename: str):
    """
    Writes a DataFrame to a CSV file with a header and comma as the separator.
    :param df:
    :param filename:
    """
    tmp_dir = os.environ['TMP_DIR']
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)

    try:
        output_path = f'{tmp_dir}/{filename}'
        df.coalesce(1) \
            .write \
            .csv(output_path, header=True, sep=';', mode='overwrite')
    except Exception as e:
        logger.error(f"An error occurred while writing the DataFrame to CSV: {e}")


def transform_df(df: DataFrame, config: dict) -> DataFrame:
    """
    Filters XML data based on config
    :param df:
    :param config:
    :return:
    """
    df = df.select(
        col(config.get('MCC')).alias('MCC'),
        col(config.get('MNC')).alias('MNC'),
        col(config.get('Country_code')).alias('Country_code'),
        col(config.get('NDC')).alias('NDC'),
        col(config.get('SN_Start')).alias('SN_Start'),
        col(config.get('SN_Stop')).alias('SN_Stop')
    ) \
        .filter(
        (col(config.get('MCC')) == config.get('target_MCC'))
        & (col(config.get('MNC')) == config.get('target_MNC'))
    )

    zipped_df = df.withColumn("zipped", arrays_zip("Country_code", "NDC", "SN_Start", "SN_Stop"))
    exploded_df = zipped_df.withColumn(
        "exploded",
        explode("zipped")
    ) \
        .select(
        "MCC",
        "MNC",
        col("exploded.Country_code").alias("Country_code"),
        col("exploded.NDC").alias("NDC"),
        col("exploded.SN_Start").alias("SN_Start"),
        col("exploded.SN_Stop").alias("SN_Stop")
    )

    return exploded_df
