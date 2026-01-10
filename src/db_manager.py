from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple
import psycopg2

from .config import PostgresConfig


@dataclass(frozen=True)
class VacancyView:
    """Человекочитаемое представление вакансии для вывода в интерфейсе."""
    company_name: str
    vacancy_title: str
    salary_from: Optional[int]
    salary_to: Optional[int]
    currency: Optional[str]
    url: Optional[str]


class DBManager:
    """Менеджер запросов к PostgreSQL для работы с компаниями и вакансиями."""

    def __init__(self, config: PostgresConfig) -> None:
        """Сохраняет конфигурацию подключения для последующих запросов."""
        self._config = config

    def _connect(self):
        """Создает новое подключение к PostgreSQL."""
        return psycopg2.connect(
            host=self._config.host,
            port=self._config.port,
            user=self._config.user,
            password=self._config.password,
            dbname=self._config.dbname,
        )

    def get_companies_and_vacancies_count(self) -> List[Tuple[str, int]]:
        """Возвращает список всех компаний и количество вакансий у каждой компании."""
        query = """
            SELECT c.name, COUNT(v.id) AS вакансий
            FROM companies c
            LEFT JOIN vacancies v ON v.company_id = c.id
            GROUP BY c.name
            ORDER BY вакансий DESC, c.name;
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                return [(row[0], int(row[1])) for row in cur.fetchall()]

    def get_all_vacancies(self) -> List[VacancyView]:
        """Возвращает список всех вакансий с названием компании, зарплатой и ссылкой."""
        query = """
            SELECT c.name, v.title, v.salary_from, v.salary_to, v.salary_currency, v.alternate_url
            FROM vacancies v
            JOIN companies c ON c.id = v.company_id
            ORDER BY c.name, v.title;
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()

        return [
            VacancyView(
                company_name=r[0],
                vacancy_title=r[1],
                salary_from=r[2],
                salary_to=r[3],
                currency=r[4],
                url=r[5],
            )
            for r in rows
        ]

    def get_avg_salary(self) -> Optional[float]:
        """
        Возвращает среднюю зарплату по вакансиям.

        Зарплата рассчитывается так:
        - если указаны salary_from и salary_to: берется среднее (from+to)/2
        - если указано только from: берется from
        - если указано только to: берется to

        Примечание: валюта не учитывается (при желании можно фильтровать по currency).
        """
        query = """
            SELECT AVG(
                CASE
                    WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL THEN (salary_from + salary_to) / 2.0
                    WHEN salary_from IS NOT NULL THEN salary_from * 1.0
                    WHEN salary_to IS NOT NULL THEN salary_to * 1.0
                    ELSE NULL
                END
            ) AS avg_salary
            FROM vacancies;
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                (avg_salary,) = cur.fetchone()
                return float(avg_salary) if avg_salary is not None else None

    def get_vacancies_with_higher_salary(self) -> List[VacancyView]:
        """Возвращает вакансии, у которых рассчитанная зарплата выше средней по всем вакансиям."""
        query = """
            WITH s AS (
                SELECT AVG(
                    CASE
                        WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL THEN (salary_from + salary_to) / 2.0
                        WHEN salary_from IS NOT NULL THEN salary_from * 1.0
                        WHEN salary_to IS NOT NULL THEN salary_to * 1.0
                        ELSE NULL
                    END
                ) AS avg_salary
                FROM vacancies
            )
            SELECT c.name, v.title, v.salary_from, v.salary_to, v.salary_currency, v.alternate_url
            FROM vacancies v
            JOIN companies c ON c.id = v.company_id
            CROSS JOIN s
            WHERE
                CASE
                    WHEN v.salary_from IS NOT NULL AND v.salary_to IS NOT NULL THEN (v.salary_from + v.salary_to) / 2.0
                    WHEN v.salary_from IS NOT NULL THEN v.salary_from * 1.0
                    WHEN v.salary_to IS NOT NULL THEN v.salary_to * 1.0
                    ELSE NULL
                END > s.avg_salary
            ORDER BY c.name, v.title;
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()

        return [VacancyView(r[0], r[1], r[2], r[3], r[4], r[5]) for r in rows]

    def get_vacancies_with_keyword(self, keyword: str) -> List[VacancyView]:
        """
        Возвращает вакансии, в названии которых встречается ключевое слово (без учета регистра).

        Пример: keyword="python"
        """
        query = """
            SELECT c.name, v.title, v.salary_from, v.salary_to, v.salary_currency, v.alternate_url
            FROM vacancies v
            JOIN companies c ON c.id = v.company_id
            WHERE v.title ILIKE %s
            ORDER BY c.name, v.title;
        """
        pattern = f"%{keyword}%"
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (pattern,))
                rows = cur.fetchall()

        return [VacancyView(r[0], r[1], r[2], r[3], r[4], r[5]) for r in rows]
