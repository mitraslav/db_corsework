from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, Optional, Tuple, List

import psycopg2
from psycopg2.extras import execute_values

from .config import PostgresConfig
from .hh_api import HeadHunterAPI


@dataclass(frozen=True)
class EmployerSeed:
    """Описание выбранного работодателя: его hh_id и необязательная пометка."""
    hh_id: int
    note: str = ""


def _connect(config: PostgresConfig):
    """Создает подключение к PostgreSQL по параметрам конфига."""
    return psycopg2.connect(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        dbname=config.dbname,
    )


def _parse_salary(salary: Optional[Dict[str, Any]]) -> Tuple[Optional[int], Optional[int], Optional[str]]:
    """Преобразует salary-объект из API hh.ru в кортеж (from, to, currency)."""
    if not salary:
        return None, None, None
    return salary.get("from"), salary.get("to"), salary.get("currency")


def _parse_published_at(value: Optional[str]) -> Optional[datetime]:
    """
    Преобразует строку даты публикации из hh.ru в datetime.

    hh.ru часто возвращает ISO-строку с 'Z'; заменяем 'Z' на '+00:00' для совместимости.
    """
    if not value:
        return None
    v = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(v)
    except ValueError:
        return None


def upsert_companies(config: PostgresConfig, employers: Iterable[Dict[str, Any]]) -> None:
    """
    Записывает работодателей в таблицу companies.

    Если hh_id уже существует — обновляет name/url (UPSERT).
    """
    rows = []
    for emp in employers:
        rows.append((int(emp["id"]), emp.get("name", ""), emp.get("alternate_url") or emp.get("url")))

    if not rows:
        return

    conn = _connect(config)
    try:
        with conn:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO companies (hh_id, name, url)
                    VALUES %s
                    ON CONFLICT (hh_id) DO UPDATE
                    SET name = EXCLUDED.name,
                        url = EXCLUDED.url;
                    """,
                    rows,
                )
    finally:
        conn.close()


def insert_vacancies(config: PostgresConfig, vacancies: Iterable[Dict[str, Any]]) -> None:
    """
    Записывает вакансии в таблицу vacancies.

    Связывает вакансии с компаниями через company_id (FK), ищем его по employer.hh_id.
    При конфликте по hh_id — обновляет запись (UPSERT).
    """
    conn = _connect(config)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, hh_id FROM companies;")
                mapping = {hh_id: cid for (cid, hh_id) in cur.fetchall()}

                rows: List[tuple] = []
                for vac in vacancies:
                    hh_vac_id = int(vac["id"])
                    employer = vac.get("employer") or {}
                    emp_hh_id = employer.get("id")
                    if emp_hh_id is None:
                        continue
                    company_id = mapping.get(int(emp_hh_id))
                    if company_id is None:
                        continue

                    salary_from, salary_to, currency = _parse_salary(vac.get("salary"))
                    rows.append(
                        (
                            hh_vac_id,
                            company_id,
                            vac.get("name", ""),
                            salary_from,
                            salary_to,
                            currency,
                            vac.get("alternate_url"),
                            _parse_published_at(vac.get("published_at")),
                        )
                    )

                if not rows:
                    return

                execute_values(
                    cur,
                    """
                    INSERT INTO vacancies
                      (hh_id, company_id, title, salary_from, salary_to, salary_currency, alternate_url, published_at)
                    VALUES %s
                    ON CONFLICT (hh_id) DO UPDATE
                    SET company_id = EXCLUDED.company_id,
                        title = EXCLUDED.title,
                        salary_from = EXCLUDED.salary_from,
                        salary_to = EXCLUDED.salary_to,
                        salary_currency = EXCLUDED.salary_currency,
                        alternate_url = EXCLUDED.alternate_url,
                        published_at = EXCLUDED.published_at;
                    """,
                    rows,
                )
    finally:
        conn.close()


def load_hh_data_to_db(
    config: PostgresConfig,
    seeds: list[EmployerSeed],
    only_with_salary: bool = False,
    max_pages: int = 20,
) -> None:
    """
    Основной сценарий загрузки: получает данные из hh.ru и сохраняет их в PostgreSQL.

    1) Загружает работодателей (companies)
    2) Загружает вакансии этих работодателей (vacancies)
    """
    api = HeadHunterAPI()

    employers_payload = [api.get_employer(seed.hh_id) for seed in seeds]
    upsert_companies(config, employers_payload)

    all_vacancies: List[Dict[str, Any]] = []
    for seed in seeds:
        all_vacancies.extend(
            api.get_vacancies_by_employer(
                employer_id=seed.hh_id,
                only_with_salary=only_with_salary,
                max_pages=max_pages,
            )
        )

    insert_vacancies(config, all_vacancies)
