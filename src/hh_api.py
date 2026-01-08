from __future__ import annotations

from typing import Any, Dict, List, Optional
import requests

class HeadHunterAPI:

    BASE_URL = "https://api.hh.ru"

    def __init__(self, user_agent: str = "hh-postgres-project/1.0") -> None:
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": user_agent})

    def get_employer(self, employer_id: int) -> Dict[str, Any]:
        """Получает данные работника по его ID"""
        url = f"{self.BASE_URL}/employers/{employer_id}"
        resp = self._session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def search_employers(self, text: str, per_page: int = 20) -> List[Dict[str, Any]]:
        """Ищет работников"""
        url = f"{self.BASE_URL}/employers"
        params = {"text": text, "per_page": per_page}
        resp = self._session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("itmems", [])

    def get_vacancies_by_employer(
            self,
            employer_id: int,
            per_page: int = 100,
            max_pages: int = 20,
            area: Optional[int] = None,
            only_with_salary: bool = False
    ) -> List[Dict[str, Any]]:
        """"""
        url = f"{self.BASE_URL}/vacancies"
        all_items: List[Dict[str, Any]] = []

        for page in range(max_pages):
            params: Dict[str, Any] = {
                "employer_id": employer_id,
                "per_page": per_page,
                "page": page
            }
            if area is not None:
                params["area"] = area
            if only_with_salary:
                params["only_with_salary"] = True

            resp = self._session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            items = data.get("items", [])
            all_items.extend(items)

            pages = int(data.get("pages", 0))
            if page + 1 >= pages:
                break

        return all_items