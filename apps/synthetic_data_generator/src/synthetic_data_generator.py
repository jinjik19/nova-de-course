"""Module for generating synthetic data."""
import functools
import os
import random
import uuid
from collections.abc import Iterator
from datetime import datetime, timedelta
from typing import Any, Iterable

import orjson
from pyspark import Broadcast
from pyspark.sql import DataFrame, SparkSession

from src.models import ROW_SCHEMA, Row
from src.services.logger import logger
from src.services.nullable import nullable
from src.utils.translit import transliterate


class GeneratorLogic:
    """Logic for generation row."""

    def __init__(self, allow_nulls: bool = True):
        self.allow_nulls = allow_nulls

    def generate_row(self, names: list[str], cities: list[str]) -> Row:
        """Generates a single data row."""
        age = self._generate_age()
        name = self._generate_name(names)

        return Row(
            id=self._generate_id(),
            name=name,
            email=self._generate_email(name),
            city=self._generate_city(cities),
            age=age,
            salary=self._generate_salary(),
            registration_date=self._generate_registration_date(age)
        )

    def _generate_id(self) -> str:
        """Generates a unique identifier."""
        return str(uuid.uuid4())

    @nullable()
    def _generate_name(self, names: list[str]):
        """Generates a random name."""
        return random.choice(names)

    def _generate_age(self):
        """Generates a random age."""
        return random.randint(18, 95)

    def _generate_email(self, name: str):
        """Generates a random email."""
        if not name:
            return None

        domain = random.choice(['gmail', 'yandex', 'mail', 'corp'])
        zone = random.choice(['com', 'ru'])
        latin_name = transliterate(name)

        return f"{latin_name}@{domain}.{zone}"

    @nullable()
    def _generate_city(self, cities: list[str]):
        """Generates a random city."""
        return random.choice(cities)

    @nullable()
    def _generate_salary(self):
        """Generates a random salary."""
        return round(random.uniform(15_000, 10_000_000), 2)

    def _generate_registration_date(self, age: int) -> str:
        """Generates a registration date based on age.

        Args:
            age (int): Age of the user.
        """
        current_date = datetime.now()
        birth_date = current_date - timedelta(days=age * 365)
        start_date = birth_date + timedelta(days=18 * 365)
        random_days = random.randint(0, (current_date - start_date).days)
        registration_date = start_date + timedelta(days=random_days)

        return registration_date.strftime("%Y-%m-%d")


class SyntheticDataGenerator:
    """Generator of synthetic data.

    Args:
        num_records (int): Number of records to generate.
    """
    def __init__(self, spark: SparkSession, num_records: int) -> None:
        self.spark = spark
        self.num_records = num_records
        self.logger = logger

        names_data = self._get_data("names")
        cities_data = self._get_data("cities")

        self.bc_names = self.spark.sparkContext.broadcast(names_data)
        self.bc_cities = self.spark.sparkContext.broadcast(cities_data)

    def _get_data(self, file_name: str) -> list:
        """Get data from json files."""
        current_dir = os.path.dirname(os.path.abspath(__file__))
        json_path = os.path.join(current_dir, "..", "data", f"{file_name}.json")
        json_path = os.path.normpath(json_path)

        with open(json_path, "rb") as file:
            return orjson.loads(file.read())

    def run(self) -> DataFrame:
        """Entrypoint for data generation."""
        rdd = self.spark.range(self.num_records).rdd.repartition(4)
        should_allow_nulls = self.num_records > 20

        partition_func = functools.partial(
            SyntheticDataGenerator._generate_partition,
            bc_names=self.bc_names,
            bc_cities=self.bc_cities,
            allow_nulls=should_allow_nulls
        )

        generated_rdd = rdd.mapPartitions(partition_func)

        try:
            df = self.spark.createDataFrame(generated_rdd, schema=ROW_SCHEMA)
            return df
        except Exception as e:
            self.logger.error(f"Error saving data: {e}")

    @staticmethod
    def _generate_partition(
        iterator: Iterable[Any],
        bc_names: Broadcast,
        bc_cities: Broadcast,
        allow_nulls: bool,
    ) -> Iterator[Row]:
        local_names = bc_names.value
        local_cities = bc_cities.value

        gen_logic = GeneratorLogic(allow_nulls)

        for _ in iterator:
            yield gen_logic.generate_row(local_names, local_cities)
