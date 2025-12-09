import abc
from abc import ABC
from dataclasses import dataclass, fields

import mysql.connector


class DatabaseBase(ABC):
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
        return cls.query(config, f"""SELECT {cls.fields()} FROM {cls.table_name()}""")

    @classmethod
    def query(cls, config, query, params=None):
        db = cls.connect_to_database(config)
        cursor = db.cursor(dictionary=True)
        if params is not None:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        db.close()
        return results

    @classmethod
    @abc.abstractmethod
    def table_name(cls):
        pass

    @classmethod
    def extra_fields(cls):
        return []

    @classmethod
    def fields(cls):
        dataclass_fields = [field.name for field in fields(cls)]
        return ", ".join(dataclass_fields + cls.extra_fields())
