import mysql.connector


def get_call_history(config):
    db = mysql.connector.connect(
        host=config.mysql_host,
        user=config.mysql_user,
        password=config.mysql_password,
        database=config.mysql_database,
    )
    cursor = db.cursor()

    query_str = (
        "SELECT InstrumentId , "
        "PrimaryKeyValue , "
        "CallNumber , "
        "DialNumber , "
        "BusyDials , "
        "StartTime , "
        "EndTime , "
        "TIME_FORMAT(TIMEDIFF(EndTime, StartTime), '%H hours %i minutes %s seconds') as dialsecs, "
        "Status , "
        "Interviewer , "
        "DialResult , "
        "UpdateInfo , "
        "AppointmentInfo "
        "FROM cati.DialHistory "
    )

    cursor.execute(query_str)
    results = cursor.fetchall()
    cursor.close()

    return results


def get_events(config):
    db = mysql.connector.connect(
        host=config.mysql_host,
        user=config.mysql_user,
        password=config.mysql_password,
        database=config.mysql_database,
    )
    print("Starting get_events ")
    cursor = db.cursor()

    cursor.execute("SELECT * FROM Events")
    result = cursor.fetchall()
    print(f"Results {len(result)}")

    return result
