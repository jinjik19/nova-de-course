from dataclasses import dataclass
from uuid import UUID

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


@dataclass
class Row:
    id: UUID
    name: str
    email: str
    city: str
    age: int
    salary: float
    registration_date: str

ROW_SCHEMA = StructType([
    StructField("id", StringType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("email", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("age", IntegerType(), nullable=False),
    StructField("salary", DoubleType(), nullable=True),
    StructField("registration_date", StringType(), nullable=True),
])