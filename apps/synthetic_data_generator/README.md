# Synthetic Data Generator for DEV
---

Генерирует синтетические CSV-данные с использованием PySpark для тестового окружения (DEV)

---
[![Python](https://img.shields.io/badge/Python-3.13+-blue.svg)](https://www.python.org/downloads/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-4.0.1-orange.svg)](https://spark.apache.org/)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

---

## Tech Stack
- **Core:** Python 3.13+
- **Processing:** Apache Spark (PySpark)
- **Performance:** `orjson`
- **Code Quality:** `Ruff`
- **Dependency Manager:** `uv`
---

## Data Structure
**Up to 5% of the values ​​in each table can be `NULL` (simulating missing data).**
| Field | Type | Description |
| ----- | ---- | ----------- |
|  `id` | UUID | Unique identifier |
| `name` | String | Random name from data/names.json |
| `email` | String | Email created from name and domain with .ru/.com |
| `city` | String | Random city from data/cities.json |
| `age` | String | Age (from 18 to 85 лет) |
| `salary` | String | Random salary |
| `registration_date` | String | Registration date |

---

## Project Structure
```
.
├── Makefile
├── README.md
├── data                              # Dictonaries
│   ├── cities.json
│   └── names.json
├── main.py
├── output                            # Folder with generated data
├── pyproject.toml
├── src
|   ├── services
|   │   ├── logger.py                 # Configuration for looger
|   │   ├── nullable.py               # decorator for check nullable
|   │   └── validator.py              # Script for validation data
│   ├── models.py                     # Model
│   └── synthetic_data_generator.py   # Logic data is generation
└── uv.lock
```
---

## Example of data
| id | name | email | city | age | salary | registration_date |
| --- | --- | --- | --- | --- | --- | --- |
| 8fdbe4eb-e0f5-43d5-95d0-67c05ce6b2cb| Дарья | darya@corp.ru | Нижний Новгород | 95 | | 1974-11-22 |
| a98c2032-eece-4553-b5ad-d2f2db135db4 | Матвей | matvei@mail.ru | San Diego | 78 | 1950504.37 | 2021-02-05 |
| c5c880cb-7f9e-40b5-8d20-86c09f282be8 | Екатерина | ekaterina@corp.com | Chicago | 53 | 2656980.44 | 2020-03-08 |
| da56595-48fa-4f73-9b3e-18ad41610a3a | Jessica | jessica@mail.ru | Омск | 89 | 4905508.01 | 2016-08-08 |
| 62b821fa-b6b4-4428-9d30-3aab8442d77d | Jennifer | jennifer@mail.com | Самара | 57 | 7048447.82 | 2017-01-22 |
---

## Start project
1. Install uv
    ```sh
    # macOS / Linux
    curl -LsSf [https://astral.sh/uv/install.sh](https://astral.sh/uv/install.sh) | sh

    # Windows
    powershell -c "irm [https://astral.sh/uv/install.ps1](https://astral.sh/uv/install.ps1) | iex"
    ```

2. Create virtual environment
   ```sh
   cd apps/synthetic_data_generator

   uv venv

   source .venv/bin/activate
   ```

3. Install dependencies
   ```sh
   uv sync
   ```

4. Run generator
   ```sh
   # Generate 1000 rows(default)
   make run

   # Generate 1_000_000 rows
   make run count=1000000
   ```
