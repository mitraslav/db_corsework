from __future__ import annotations

import os
from dataclasses import dataclass
from dotenv import load_dotenv


@dataclass(frozen=True)
class PostgresConfig:
    """Конфигурация подключения к PostgreSQL, загружается из переменных окружения."""
    host: str
    port: int
    user: str
    password: str
    dbname: str


def load_config() -> PostgresConfig:
    """Загружает конфигурацию подключения из .env/переменных окружения."""
    load_dotenv()
    host = os.getenv("PG_HOST", "localhost")
    port = int(os.getenv("PG_PORT", "5432"))
    user = os.getenv("PG_USER", "postgres")
    password = os.getenv("PG_PASSWORD", "")
    dbname = os.getenv("PG_DB", "hh_vacancies")
    return PostgresConfig(host=host, port=port, user=user, password=password, dbname=dbname)
