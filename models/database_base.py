from dataclasses import fields

import mysql.connector


class DataBaseBase:
    @classmethod
    def connect_to_database(cls, config):
        try:
            return mysql.connector.connect(
                host=config.mysql_host,
                user=config.mysql_user,
                password=config.mysql_password,
                database=config.mysql_database,
            )
        except mysql.connector.errors.ProgrammingError:
            print("MySQL authentication issue")
        except mysql.connector.errors.InterfaceError:
            print("MySQL connection issue")

    @classmethod
    def select_from(cls, config):
        db = cls.connect_to_database(config)
        cursor = db.cursor(dictionary=True)
        cursor.execute(f"""SELECT {cls.fields()} FROM {cls.table_name()}""")
        results = cursor.fetchall()
        cursor.close()
        db.close()
        return results

    @classmethod
    def table_name(cls):
        pass

    @classmethod
    def extra_fields(cls):
        return []

    @classmethod
    def fields(cls):
        dataclass_fields = [field.name for field in fields(cls)]
        return ", ".join(dataclass_fields + cls.extra_fields())
