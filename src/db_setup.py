from __future__ import annotations

from typing import Optional
import psycopg2
from psycopg2 import sql

from .config import PostgresConfig


def _connect(config: PostgresConfig, dbname: Optional[str] = None):
    """Создает подключение psycopg2 к указанной БД (или к БД по умолчанию из конфига)."""
    return psycopg2.connect(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        dbname=dbname or config.dbname,
    )


def create_database(config: PostgresConfig) -> None:
    """
    Создает базу данных, если она еще не существует.

    Для проверки/создания подключается к системной БД 'postgres' (типичный подход).
    """
    conn = _connect(config, dbname="postgres")
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (config.dbname,))
            exists = cur.fetchone() is not None
            if not exists:
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(config.dbname)))
    finally:
        conn.close()


def create_tables(config: PostgresConfig) -> None:
    """Создает таблицы companies и vacancies, а также связь FK между ними."""
    conn = _connect(config)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS companies (
                        id SERIAL PRIMARY KEY,
                        hh_id INTEGER UNIQUE NOT NULL,
                        name TEXT NOT NULL,
                        url TEXT
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS vacancies (
                        id SERIAL PRIMARY KEY,
                        hh_id INTEGER UNIQUE NOT NULL,
                        company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
                        title TEXT NOT NULL,
                        salary_from INTEGER,
                        salary_to INTEGER,
                        salary_currency TEXT,
                        alternate_url TEXT,
                        published_at TIMESTAMPTZ
                    );
                    """
                )
    finally:
        conn.close()
