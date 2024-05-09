from db import DBManager
from hh import HHIntegration, serialized_vacancies, serialized_company, transform_employer_to_id

if __name__ == "__main__":
    db_manager = DBManager()

    if db_manager.drop_table():
        db_manager.create_tables()

    company_lst = []
    employee_ids = [93, 1060821, 9161401, 10260977, 827187, 3878807, 10725937, 10056971, 3711736, 2700516]
    unparsed_vacancies = HHIntegration.get_vacancies(employee_ids=employee_ids)
    parsed_vacancies = serialized_vacancies(unparsed_vacancies.get("items"))
    employers = [serialized_company(vacancy.get("employer")) for vacancy in parsed_vacancies]
    transformed_vacancies = [transform_employer_to_id(vacancy) for vacancy in parsed_vacancies]
    db_manager.import_vacancies(transformed_vacancies, employers)
    print(db_manager.get_company_vacancy_counts())
    print(db_manager.get_all_vacancies())
    print(db_manager.get_avg_salary())
    print(db_manager.get_vacancies_with_higher_salary())
    print(db_manager.get_vacancies_with_keyword("Менеджер отдела продаж"))
    db_manager.connection.close()
