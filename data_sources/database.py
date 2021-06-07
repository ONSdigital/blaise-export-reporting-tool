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


def query_dial_history_table(config, fields):
    db = connect_to_db(config)
    cursor = db.cursor()

    query_str = (
        f"SELECT {fields} FROM cati.DialHistory "
    )

    cursor.execute(query_str)
    results = cursor.fetchall()
    cursor.close()

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
    results = query_dial_history_table(config, fields_to_get)

    return results


def get_events(config):
    db = connect_to_db(config)
    print("Starting get_events ")
    cursor = db.cursor()

    cursor.execute("SELECT * FROM Events")
    result = cursor.fetchall()
    print(f"Results {len(result)}")
    cursor.close()

    return result
