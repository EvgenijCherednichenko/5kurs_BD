from db import DBManager
from hh import HHIntegration, serialized_vacancies, serialized_company, transform_employer_to_id

if __name__ == "__main__":
    db_manager = DBManager()

    if db_manager.drop_table():
        db_manager.create_tables()

    company_lst = []
    employee_ids = [93]

    unparsed_vacancies = HHIntegration.get_vacancies(employee_ids=employee_ids)
    parsed_vacancies = serialized_vacancies(unparsed_vacancies.get("items"))
    employers = [serialized_company(vacancy.get("employer")) for vacancy in parsed_vacancies]
    transformed_vacancies = [transform_employer_to_id(vacancy) for vacancy in parsed_vacancies]
    db_manager.import_vacancies(transformed_vacancies, employers)
    print(db_manager.vacancies_and_companies_count())
    print(db_manager.get_all_vacancies())
    print(db_manager.get_avg_salary())
    print(db_manager.get_vacancies_with_higher_salary())
    print(db_manager.get_vacancies_with_keyword("Кладовщик-комплектовщик,"))
    db_manager.connection.close()
