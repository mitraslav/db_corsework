from __future__ import annotations

from typing import Iterable

from src.config import load_config
from src.db_manager import DBManager, VacancyView
from src.db_setup import create_database, create_tables
from src.loader import EmployerSeed, load_hh_data_to_db


def format_salary(v: VacancyView) -> str:
    """Преобразует поля зарплаты вакансии в человекочитаемую строку."""
    if v.salary_from is None and v.salary_to is None:
        return "зарплата не указана"
    cur = v.currency or ""
    if v.salary_from is not None and v.salary_to is not None:
        return f"{v.salary_from}–{v.salary_to} {cur}".strip()
    if v.salary_from is not None:
        return f"от {v.salary_from} {cur}".strip()
    return f"до {v.salary_to} {cur}".strip()


def print_vacancies(vacancies: Iterable[VacancyView], limit: int = 20) -> None:
    """Печатает список вакансий в понятном виде (с ограничением количества строк)."""
    shown = 0
    for v in vacancies:
        print(f"- {v.company_name} | {v.vacancy_title} | {format_salary(v)} | {v.url or ''}")
        shown += 1
        if shown >= limit:
            break
    if shown == 0:
        print("Ничего не найдено.")


def user_menu(db: DBManager) -> None:
    """Простой текстовый интерфейс взаимодействия с пользователем."""
    while True:
        print("\nВыберите действие:")
        print("1) Компании и количество вакансий")
        print("2) Все вакансии (первые 20)")
        print("3) Средняя зарплата")
        print("4) Вакансии с зарплатой выше средней (первые 20)")
        print("5) Поиск вакансий по ключевому слову")
        print("0) Выход")

        choice = input("Ваш выбор: ").strip()

        if choice == "1":
            rows = db.get_companies_and_vacancies_count()
            for name, cnt in rows:
                print(f"- {name}: {cnt}")
        elif choice == "2":
            print_vacancies(db.get_all_vacancies(), limit=20)
        elif choice == "3":
            avg = db.get_avg_salary()
            if avg is None:
                print("Средняя зарплата не вычисляется (нет данных по зарплатам).")
            else:
                print(f"Средняя зарплата по вакансиям: {avg:.2f} (без учета валют)")
        elif choice == "4":
            print_vacancies(db.get_vacancies_with_higher_salary(), limit=20)
        elif choice == "5":
            kw = input("Введите слово (например, python): ").strip()
            print_vacancies(db.get_vacancies_with_keyword(kw), limit=20)
        elif choice == "0":
            break
        else:
            print("Неизвестная команда.")


def main() -> None:
    """Точка входа: создает БД/таблицы, загружает данные с hh.ru и запускает меню."""
    config = load_config()

    create_database(config)
    create_tables(config)

    seeds = [
        EmployerSeed(1740, "Яндекс"),
        EmployerSeed(3529, "Сбер"),
        EmployerSeed(80, "Альфа-Банк"),
        EmployerSeed(78638, "Тинькофф"),
        EmployerSeed(4181, "VK"),
        EmployerSeed(15478, "Ozon"),
        EmployerSeed(2180, "Авито"),
        EmployerSeed(84585, "Kaspersky"),
        EmployerSeed(87021, "МТС"),
        EmployerSeed(3776, "Ростелеком"),
    ]

    load_hh_data_to_db(config, seeds=seeds, only_with_salary=False, max_pages=20)

    db = DBManager(config)
    user_menu(db)


if __name__ == "__main__":
    main()
