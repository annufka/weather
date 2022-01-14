import sqlite3
from datetime import datetime


class Database:
    def __init__(self, path_to_db="main.db"):
        self.path_to_db = path_to_db

    @property
    def connection(self):
        return sqlite3.connect(self.path_to_db)

    def execute(self, sql: str, parameters: tuple = None, fetchone=False, fetchall=False, commit=False):
        if not parameters:
            parameters = ()
        connection = self.connection
        cursor = connection.cursor()
        data = None
        cursor.execute(sql, parameters)

        if commit:
            connection.commit()
        if fetchall:
            data = cursor.fetchall()
        if fetchone:
            data = cursor.fetchone()
        connection.close()
        return data

    def create_table_cities(self):
        sql = """
        CREATE TABLE IF NOT EXISTS Cities (
            city_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name_of_city text NOT NULL
            );
        """
        self.execute(sql, commit=True)

    def create_table_weather(self):
        sql = """
        CREATE TABLE IF NOT EXISTS Weather (
            city_id int NOT NULL,
            dt datetime NOT NULL,
            temp float NOT NULL,
            pop float NOT NULL,
            clouds int NOT NULL, 
            pressure int NOT NULL, 
            humidity int NOT NULL,
            wind_speed float NOT NULL,
            FOREIGN KEY (city_id) REFERENCES Cities (city_id) 
            );
        """
        self.execute(sql, commit=True)

    @staticmethod
    def format_args(sql, parameters: dict):
        sql += " AND ".join([
            f"{item} = ?" for item in parameters
        ])
        return sql, tuple(parameters.values())

    def select_city(self, name):
        sql = """
                SELECT * FROM Cities WHERE name_of_city=?
                """
        return self.execute(sql, parameters=(name,), fetchone=True)

    def add_city(self, name):
        sql = """
                 INSERT INTO Cities(name_of_city) VALUES(?)
                 """
        self.execute(sql, parameters=(name,), commit=True)

    def add_weather(self, city, dt, temp, pop, clouds, pressure, humidity, wind_speed):
        sql_for_city_id = """
                SELECT city_id FROM Cities WHERE name_of_city=?
                """
        city_id = self.execute(sql_for_city_id, parameters=(city,), fetchone=True)
        sql = """
                  INSERT INTO Weather(city_id, dt, temp, pop, clouds, pressure, humidity, wind_speed) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                  """
        self.execute(sql, parameters=(
        city_id[0], dt.strftime("%Y-%m-%d"), temp, pop, clouds, pressure, humidity, wind_speed), commit=True)

    def select_all_cities(self):
        sql = """
                 SELECT name_of_city FROM Cities
                 """
        return self.execute(sql, fetchall=True)

    def select_params(self, param, city):
        sql_for_city_id = """
                        SELECT city_id FROM Cities WHERE name_of_city=?
                        """
        city_id = self.execute(sql_for_city_id, parameters=(city,), fetchone=True)
        sql = """
                 SELECT {} FROM Weather WHERE city_id=?
                 """
        return self.execute(sql.format(param), parameters=(city_id[0],), fetchall=True)

    def select_params_by_city_date(self, start_dt, end_dt, city):
        sql_for_city_id = """
                                SELECT city_id FROM Cities WHERE name_of_city=?
                                """
        city_id = self.execute(sql_for_city_id, parameters=(city,), fetchone=True)
        sql = f"""
                SELECT * FROM Weather WHERE city_id=? AND strftime('%Y-%m-%d', dt) BETWEEN strftime('%Y-%m-%d', '{start_dt}')AND strftime('%Y-%m-%d', '{end_dt}');
                """
        return self.execute(sql, parameters=(city_id[0], ), fetchall=True)
