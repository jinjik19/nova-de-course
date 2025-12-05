"""Main module for synthetic data generation."""

import argparse
import os
import shutil
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession

from src.services.logger import logger
from src.services.validator import DataValidator
from src.synthetic_data_generator import (
    SyntheticDataGenerator,
)


def get_spark_session(app_name: str) -> SparkSession:
    """Create and return a Spark session."""

    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    return spark


def save_dataframe_as_csv(df: DataFrame) -> None:
    """Save DataFrame as a single CSV file."""
    file_name = datetime.now().strftime("%Y-%m-%d-dev.csv")
    output_dir = "output"
    temp_dir = f"{output_dir}/temp_output"
    final_file_path = os.path.join(output_dir, file_name)

    try:
        single_df = df.coalesce(1)
        single_df.write.csv(temp_dir, header=True, mode="overwrite")

        part_file = None
        for file in os.listdir(temp_dir):
            if file.startswith("part-") and file.endswith(".csv"):
                part_file = file
                break

        if not part_file:
            raise FileNotFoundError("Spark didn't generate a CSV file part.")

        os.makedirs(output_dir, exist_ok=True)

        src_path = os.path.join(temp_dir, part_file)
        shutil.move(src_path, final_file_path)

        logger.info(f"Data saved to {final_file_path}")
    except Exception as e:
        logger.error(f"Error saving data: {e}")
    finally:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            logger.info("Cleaned up temporary files.")


def main(num_records: int = 1000):
    """Entrypoint for synthetic data generation."""
    spark = get_spark_session("SyntheticDataGenerator")
    generator = SyntheticDataGenerator(spark, num_records)
    df = generator.run()

    if df:
       validator = DataValidator(df, num_records)
       is_valid = validator.run()

       if is_valid:
           save_dataframe_as_csv(df)
           logger.info("Data generation and validation completed successfully.")
       else:
           logger.error("Data validation failed. CSV file not saved.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Synthetic Data Generator")

    parser.add_argument(
        "-n", "--count",
        type=int,
        required=True,
        help="Number of records to generate",
    )

    args = parser.parse_args()
    main(args.count)
