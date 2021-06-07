import mysql.connector


def connect_to_db(config):
    try:
        return mysql.connector.connect(
            host=config.mysql_host,
            user=config.mysql_user,
            password=config.mysql_password,
            database=config.mysql_database,
        )
    except mysql.connector.errors.ProgrammingError:
        print("MySQL Authentication issue")
    except mysql.connector.errors.InterfaceError:
        print("MySQL Connection Issue")


def select_from(config, table_name, fields):
    db = connect_to_db(config)
    cursor = db.cursor()

    cursor.execute(f"""SELECT {fields} FROM {table_name}""")

    results = cursor.fetchall()
    cursor.close()
    db.close()

    return results


def get_call_history(config):
    fields_to_get = ("InstrumentId , "
                     "PrimaryKeyValue , "
                     "CallNumber , "
                     "DialNumber , "
                     "BusyDials , "
                     "StartTime , "
                     "EndTime , "
                     "ABS(TIME_TO_SEC(TIMEDIFF(EndTime, StartTime))) as dialsecs, "
                     "Status , "
                     "Interviewer , "
                     "DialResult , "
                     "UpdateInfo , "
                     "AppointmentInfo")
    results = select_from(config, "cati.DialHistory", fields_to_get)

    return results


def get_mi_call_history(config):
    fields_to_get = ("InstrumentId , "
                     "PrimaryKeyValue , "
                     "Id , "
                     "StartTime , "
                     "CallNumber , "
                     "DialNumber , "
                     "Interviewer , "
                     "DialResult , "
                     "DialedNumber , "
                     "AppointmentInfo, "
                     "EndTime, "
                     "ABS(TIME_TO_SEC(TIMEDIFF(EndTime, StartTime))) as dialsecs ")
    results = select_from(config, "cati.DialHistory", fields_to_get)

    return results


def get_events(config):
    result = select_from(config, "cati.Events", "*")
    print(f"Results {len(result)}")

    return result


