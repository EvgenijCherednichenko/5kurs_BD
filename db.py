from settings import config
from psycopg2 import pool
from psycopg2.extras import execute_values


class DBManager:
    def __init__(self):
        self.connection_pool = pool.SimpleConnectionPool(
            1,
            10,
            user=config.db_user,
            password=config.db_password,
            host=config.db_host,
            port=5432,
            database=config.db_name,
        )
        self.connection = self.connection_pool.getconn()

    def create_tables(self) -> None:
        """
        Создает таблицы employer и vacancy в базе данных.
        """
        connection = self.connection_pool.getconn()
        cursor = connection.cursor()

        query = """
            create table employer 
            (id int primary key, name varchar(128), url varchar(256), accredited_it_employer boolean default false)
            """
        cursor.execute(query)
        connection.commit()

        query = """
            create table vacancy 
            (id serial primary key, name varchar(128), url varchar(256), 
            description text, requirements text, salary_from int, salary_to int, employer_id int,
            CONSTRAINT fk_employer FOREIGN KEY (employer_id) REFERENCES employer(id)
            )
            """
        cursor.execute(query)
        connection.commit()

    def drop_table(self) -> bool:
        """
        Удаляет таблицы employer и vacancy из базы данных.

        :return: Результат операции удаления таблиц.
        """
        cursor = self.connection.cursor()
        company_drop_query = """drop table if exists employer cascade"""
        cursor.execute(company_drop_query)
        self.connection.commit()
        vacancy_drop_query = """drop table if exists vacancy"""
        cursor.execute(vacancy_drop_query)
        self.connection.commit()
        return True

    def import_vacancies(self, vacancies: list[dict], employers: list[dict]) -> None:
        """
        Импортирует данные о вакансиях и работодателях в базу данных.

        :param vacancies: Список словарей с данными о вакансиях.
        :param employers: Список словарей с данными о компаниях.
        """
        cursor = self.connection.cursor()
        values = [tuple(vacancy.values()) for vacancy in vacancies]
        self.import_companies(employers)
        execute_values(
            cursor,
            'INSERT INTO vacancy (name, url, salary_from, salary_to, description, requirements, employer_id) VALUES %s',
            values,
        )
        self.connection.commit()

    def import_companies(self, employers: list) -> None:
        """
        Импортирует данные о компаниях в базу данных.

        :param employers: Список словарей с данными о компаниях.
        """
        employers = [
            dict(sorted(s))
            for s in set(frozenset(d.items()) for d in employers)
        ]
        connection = self.connection_pool.getconn()
        cursor = connection.cursor()
        values = [tuple(project.values()) for project in employers]
        query = f'INSERT INTO employer (accredited_it_employer, id, name, url) VALUES %s'.format(
            values
        )
        try:
            execute_values(cursor, query, values)
            connection.commit()
        except Exception as e:
            raise e

    def get_company_vacancy_counts(self) -> dict:
        """
        Возвращает наименование компании и количество вакансий для каждой компании.

        :return: Словарь, где ключи - наименования компаний, значения - количество вакансий.
        """
        cursor = self.connection.cursor()

        query_company_vacancy_count = """
        SELECT e.name, COUNT(v.*) AS vacancy_count
        FROM employer e
        LEFT JOIN vacancy v ON v.employer_id = e.id
        GROUP BY e.name
        """

        cursor.execute(query_company_vacancy_count)
        company_vacancy_counts = cursor.fetchall()

        result = {row[0]: row[1] for row in company_vacancy_counts}

        return result

    def get_all_vacancies(self) -> list:
        """
        Получает все вакансии из базы данных.

        :return: Список всех вакансий.
        """
        cursor = self.connection.cursor()
        query = 'SELECT * from vacancy'
        cursor.execute(query)
        vacancies = cursor.fetchall()
        return vacancies

    def get_avg_salary(self) -> float:
        """
        Возвращает среднюю зарплату по всем вакансиям.

        :return: Средняя зарплата.
        """
        cursor = self.connection.cursor()
        query = """
            SELECT AVG(salary) AS average_salary
            FROM (
                SELECT (COALESCE(salary_from, 0) + COALESCE(salary_to, 0)) / 2 AS salary
                FROM vacancy
                WHERE (salary_from IS NOT NULL OR salary_to IS NOT NULL)
            ) AS combined_salaries
            """
        cursor.execute(query)
        average_salary = round(cursor.fetchone()[0], 2)
        return average_salary

    def get_vacancies_with_higher_salary(self) -> list:
        """
        Возвращает список вакансий с самой высокой средней зарплатой.

        :return: Список вакансий с высокой зарплатой.
        """
        cursor = self.connection.cursor()
        query = """
                SELECT name, (salary_from + salary_to) / 2 AS average_salary
                FROM vacancy
                ORDER BY average_salary DESC
                LIMIT 5
                """
        cursor.execute(query)
        higher_salary_vacancies = cursor.fetchall()
        return higher_salary_vacancies

    def get_vacancies_with_keyword(self, keyword):
        """
        Возвращает список вакансий, содержащих ключевое слово.

        :param keyword: Ключевое слово для поиска.
        :return: Список вакансий соответствующих ключевому слову.
        """
        cursor = self.connection.cursor()

        query = f"""
                SELECT name, url, description, requirements, salary_from, salary_to
                FROM vacancy
                WHERE name LIKE '%{keyword}%'
                """
        cursor.execute(query)
        keyword_vacancies = cursor.fetchall()
        return keyword_vacancies
