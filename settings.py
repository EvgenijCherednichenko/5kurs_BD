import os


class Config:
    hh_vacancies_url = os.getenv("VACANCIES_URL", "https://api.hh.ru/vacancies")
    db_host = os.getenv("DB_HOST", "localhost")
    db_password = os.getenv("DB_PASSWORD", "postgres")
    db_name = os.getenv("DB_NAME", "hh_db")
    db_user = os.getenv("DB_USER", "postgres")
    db_uri = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}/{db_name}"


config = Config()
