import requests
from settings import config


class HHIntegration:

    @classmethod
    def get_vacancies(cls, employee_ids: list | None = None) -> dict:
        """
        Получает вакансии по идентификаторам работодателей.

        :param employee_ids: Список идентификаторов работодателей. Может быть None.
        :return: Словарь с данными о вакансиях.
        """
        params = {"employer_id": employee_ids}
        response = requests.get(config.hh_vacancies_url, params=params)
        response.raise_for_status()
        vacancies = response.json()
        return vacancies


def serialized_vacancies(vacancies: list[dict]):
    """
    Сериализует список вакансий.

    :param vacancies: Список словарей с данными о вакансиях.
    :return: Сериализованный список вакансий.
    """
    return list(map(serialized_vacancy, vacancies))


def serialized_vacancy(vacancy: dict) -> dict:
    """
    Сериализует данные о вакансии.

    :param vacancy: Словарь с данными о вакансии.
    :return: Сериализованный словарь с данными о вакансии.
    """

    return {
        "name": vacancy.get("name"),
        "url": vacancy.get("url"),
        "salary_from": vacancy.get("salary").get("from") if vacancy.get("salary") else None,
        "salary_to": vacancy.get("salary").get("to") if vacancy.get("salary") else None,
        "description": vacancy.get("description"),
        "requirements": vacancy.get("requirements"),
        "employer": vacancy.get("employer")
    }


def serialized_company(employer: dict) -> dict:
    """
    Сериализует данные о компании.

    :param employer: Словарь с данными о компании.
    :return: Сериализованный словарь с данными о компании.
    """
    return {
        "id": employer.get("id"),
        "name": employer.get("name"),
        "url": employer.get("url"),
        "accredited_it_employer": employer.get("accredited_it_employer")
    }


def transform_employer_to_id(vacancy) -> dict:
    """
    Преобразует данные о работодателе в идентификатор работодателя в данных о вакансии.

    :param vacancy: Словарь с данными о вакансии.
    :return: Словарь с обновленными данными о вакансии с идентификатором работодателя.
    """
    employer_id = vacancy.get("employer").get("id")
    vacancy["employer_id"] = employer_id
    vacancy.pop("employer")
    return vacancy
