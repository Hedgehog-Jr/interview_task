import logging
import os
import time

from pyspark.sql import DataFrame

from helper import refactor_csv_file, write_df_to_tmp_csv, delete_folder, get_spark_session, transform_df, \
    get_app_config


logger = logging.getLogger(__name__)

def read_xml_into_df(input_file: str, row_tag: str) -> DataFrame:
    """
    Reads an XML file using Spark and returns a DataFrame
    :param input_file:
    :param row_tag:
    :return:
    """

    try:
        spark = get_spark_session()

        df = spark.read \
            .format('xml') \
            .options(rowTag=row_tag) \
            .option("inferSchema", "false") \
            .load(input_file)

        return df

    except Exception as e:
        logger.error(f"an error occured while reading the XML file: {e}")
        raise Exception(e)


def write_df_to_csv(df: DataFrame, filename: str):
    """
    Writes data from DF to csv file
    :param df:
    :param filename:
    """

    write_df_to_tmp_csv(df, filename)
    refactor_csv_file(filename)
    delete_folder(os.environ['TMP_DIR'])


def entrypoint():
    """
    Application entrypoint
    :return:
    """
    input_dir = os.environ['INPUT_DIR']
    processed_files = set()
    config = get_app_config()

    for filename in os.listdir(input_dir):
        if not filename.endswith('.xml'):
            continue

        full_path = os.path.abspath(os.path.join(input_dir, filename))
        if full_path not in processed_files:
            processed_files.add(full_path)
            df = read_xml_into_df(full_path, config.get('row_tag'))
            # TODO: it's better to add some schema validation here
            transformed_df = transform_df(df, config)
            write_df_to_csv(transformed_df, filename)


if __name__ == '__main__':
    entrypoint()
