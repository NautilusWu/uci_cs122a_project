import mysql.connector
import csv

from mysql.connector import Error


def conn_db():
    """Connect to database"""
    connection = mysql.connector.connect(
        host="localhost",  # IP address of the MySQL server
        user="test",  # username
        password="password",  # password
        database="cs122a",  # database name
    )
    return connection


def close_db_conn(connection):
    """close db connection"""
    if connection.is_connected():
        connection.close()


def execute_query(query):
    """execute query"""
    rtn_code, rtn_value = -1, None
    modify_keywords = {
        "insert",
        "update",
        "delete",
        "create",
        "alter",
        "drop",
    }
    conn = conn_db()
    if not conn:
        print("Database connection failed.")
        return rtn_code, rtn_value

    conn.autocommit = False
    cursor = None
    try:
        # execute query
        cursor = conn.cursor()
        # print(query)
        cursor.execute(query)
        affected_rows = cursor.rowcount
        # print(f"Number of rows affected 1: {affected_rows}")

        lower_query = query.lower().strip()
        is_modify = any(
            keyword in lower_query for keyword in modify_keywords
        )
        if is_modify:
            conn.commit()
        result = cursor.fetchall()
        # print(result)
        rtn_code = affected_rows
        rtn_value = result
    except Error as e:
        print(f"Error: {e}")
        rtn_code = -1
        rtn_value = None
    finally:
        if cursor != None:
            cursor.close()
        close_db_conn(conn)
        return rtn_code, rtn_value


def drop_table(table_name):
    rtn_code, rtn_value = -1, None
    conn = conn_db()
    if not conn:
        print("Database connection failed.")
        return rtn_code, rtn_value
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.commit()
        rtn_code, rtn_value = 0, "Ok"
    except Error as e:
        print(f"Error: {e}")
    finally:
        if cursor != None:
            cursor.close()
        close_db_conn(conn)
        return rtn_code, rtn_value


def del_existing_tables():
    rtn_code, rtn_value = -1, None
    tables = [
        "sessions",
        "reviews",
        "videos",
        "movies",
        "series",
        "releases",
        "viewers",
        "producers",
        "users",
    ]
    for table in tables:
        rtn_code, _ = drop_table(table)
        if rtn_code != 0:
            # print(f"Error dropping table {table}.")
            rtn_code, rtn_value = -1, None
            break
        else:
            rtn_code, rtn_value = 0, "Ok"
            # print(f"Table {table} dropped.")
    return rtn_code, rtn_value


def create_table(table_name, columns):
    rtn_code, rtn_value = -1, None
    conn = conn_db()
    if not conn:
        print("Database connection failed.")
        return rtn_code, rtn_value

    cursor = None
    try:
        # print(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})")
        cursor = conn.cursor()
        cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
        )
        conn.commit()
        rtn_code, rtn_value = 0, "Success"
    except Error as e:
        print(f"Error: {e}")
    finally:
        if cursor != None:
            cursor.close()
        close_db_conn(conn)
        return rtn_code, rtn_value


def create_new_tables():
    rtn_code, rtn_value = -1, None
    tables = [
        "users",
        "producers",
        "viewers",
        "releases",
        "movies",
        "series",
        "videos",
        "sessions",
        "reviews",
    ]
    users_columns = [
        "uid INT PRIMARY KEY AUTO_INCREMENT, ",
        "email VARCHAR(255) NOT NULL, ",
        "joined_date DATE NOT NULL, ",
        "nickname VARCHAR(255) NOT NULL, ",
        "street VARCHAR(255) NOT NULL, ",
        "city VARCHAR(255) NOT NULL, ",
        "state VARCHAR(255) NOT NULL, ",
        "zip CHAR(5) NOT NULL, ",
        "genres VARCHAR(255) NOT NULL, ",
        "UNIQUE(email)", # This is what I do to let us verify a user without using uid. It can be deleted.
    ]
    producers_columns = [
        "uid INT NOT NULL UNIQUE, ",
        "bio VARCHAR(255) NOT NULL, ",
        "company VARCHAR(255) NOT NULL, ",
        "FOREIGN KEY (uid) REFERENCES users(uid) ON DELETE CASCADE",
    ]
    viewers_columns = [
        "uid INT NOT NULL UNIQUE, ",
        "subscription VARCHAR(255) NOT NULL, ",
        "first_name VARCHAR(255) NOT NULL, ",
        "last_name VARCHAR(255) NOT NULL, ",
        "FOREIGN KEY (uid) REFERENCES users(uid) ON DELETE CASCADE",
    ]
    releases_columns = [
        "rid INT PRIMARY KEY AUTO_INCREMENT, ",
        "producer_uid INT NOT NULL, ",
        "title VARCHAR(255) NOT NULL, ",
        "genre VARCHAR(255) NOT NULL, ",
        "release_date DATE NOT NULL, ",
        "FOREIGN KEY (producer_uid) REFERENCES users(uid) ON DELETE CASCADE",
    ]
    movies_columns = [
        "rid INT NOT NULL UNIQUE, ",
        "website_url VARCHAR(255) NOT NULL, ",
        "FOREIGN KEY (rid) REFERENCES releases(rid) ON DELETE CASCADE",
    ]
    series_columns = [
        "rid INT NOT NULL UNIQUE, ",
        "introduction VARCHAR(2048) NOT NULL, ",
        "FOREIGN KEY (rid) REFERENCES releases(rid) ON DELETE CASCADE",
    ]
    videos_columns = [
        "rid INT NOT NULL, ",
        "ep_num INT DEFAULT NULL, ",
        "title VARCHAR(255) NOT NULL, ",
        "length INT NOT NULL, ",
        "FOREIGN KEY (rid) REFERENCES releases(rid) ON DELETE CASCADE, ",
        "PRIMARY KEY (rid, title), ", # Not necessary. We should ask TA
        "CONSTRAINT chk_ep_num CHECK (ep_num >= 0), ", # making sure ep_num > 0
        "CONSTRAINT chk_length CHECK (length > 0), ",
        "UNIQUE (rid, ep_num)", # Not necessary. Not really sure
    ]
    sessions_columns = [
        "sid INT PRIMARY KEY AUTO_INCREMENT, ",
        "uid INT NOT NULL, ",
        "rid INT NOT NULL, ",
        "ep_num INT DEFAULT NULL, ",
        "initiate_at DATETIME NOT NULL, ",
        "leave_at DATETIME NOT NULL, ",
        "quality VARCHAR(255) NOT NULL, ",
        "device VARCHAR(255) NOT NULL, ",
        "FOREIGN KEY (uid) REFERENCES viewers(uid) ON DELETE CASCADE, ",
        "FOREIGN KEY (rid) REFERENCES releases(rid) ON DELETE CASCADE",
        # "CONSTRAINT chk_init_leave CHECK (initiate_at <= leave_at)", # Making sure the logic. Haven't learn in class
    ]
    reviews_columns = [
        "rvid INT PRIMARY KEY AUTO_INCREMENT, ",
        "uid INT NOT NULL, ",
        "rid INT NOT NULL, ",
        "rating INT NOT NULL, ",
        "body VARCHAR(255) NOT NULL, ",
        "posted_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, ",  # Default with current time in calendar
        "FOREIGN KEY (uid) REFERENCES viewers(uid) ON DELETE CASCADE, ",
        "FOREIGN KEY (rid) REFERENCES releases(rid) ON DELETE CASCADE",
    ]

    columns = [
        users_columns,
        producers_columns,
        viewers_columns,
        releases_columns,
        movies_columns,
        series_columns,
        videos_columns,
        sessions_columns,
        reviews_columns,
    ]

    for table, column in zip(tables, columns):
        tb_columns = "".join(column)
        # print(table)
        # print(tb_columns)
        rtn_code, _ = create_table(table, tb_columns)
        if rtn_code != 0:
            print(f"Error creating table {table}.")
            rtn_code, rtn_value = -1, None
            break
        else:
            rtn_code, rtn_value = 0, "Success"
            # print(f"Table {table} created.")
    return rtn_code, rtn_value


def insert_one_row(table_name, columns, values):
    rtn_code, rtn_value = -1, None
    conn = conn_db()
    if not conn:
        print("Database connection failed.")
        return rtn_code, rtn_value

    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"INSERT INTO {table_name} ({columns}) VALUES {values};"
        )
        conn.commit()
        rtn_code, rtn_value = 0, "Ok"
    except Error as e:
        print(f"Error: {e}")
    finally:
        if cursor != None:
            cursor.close()
        close_db_conn(conn)
        return rtn_code, rtn_value


def insert_from_csv_auto_id(table_name, csv_name):
    rtn_code, rtn_value = -1, "Fail"
    conn = conn_db()
    if not conn:
        print("Database connection failed.")
        return rtn_code, rtn_value

    cursor = None
    try:
        cursor = conn.cursor()
        conn.autocommit = False

        with open(csv_name, "r", encoding="utf-8") as csvfile:
            csv_reader = csv.reader(csvfile)
            header = next(csv_reader)  # First line is the header

            # Skip the first column which is the ID
            fields = header[1:]  # header[0] is the ID
            placeholders = ", ".join(["%s"] * len(fields))
            insert_query = f"INSERT INTO "
            insert_query += f"{table_name} ({', '.join(fields)}) "
            insert_query += f"VALUES ({placeholders});"

            data_to_insert = []
            for row in csv_reader:
                values = row[1:]  # row[0] is the ID column
                data_to_insert.append(values)
            cursor.executemany(insert_query, data_to_insert)
            conn.commit()
            # print("Data inserted successfully.")
            rtn_code, rtn_value = 0, "Success"
    except Error as e:
        print(f"Error: {e}")
        rtn_code, rtn_value = -1, "Fail"
        if conn.is_connected():
            conn.rollback()
            print("Transaction rolled back.")
    finally:
        if cursor != None:
            cursor.close()
        close_db_conn(conn)
        return rtn_code, rtn_value


def insert_from_csv(table_name, csv_name):
    rtn_code, rtn_value = -1, "Fail"
    conn = conn_db()
    if not conn:
        print("Database connection failed.")
        return rtn_code, rtn_value

    cursor = None
    try:
        cursor = conn.cursor()
        conn.autocommit = False

        with open(csv_name, "r", encoding="utf-8") as csvfile:
            csv_reader = csv.reader(csvfile)
            header = next(csv_reader)  # First line is the header

            fields = header[0:]
            placeholders = ", ".join(["%s"] * len(fields))
            insert_query = f"INSERT INTO "
            insert_query += f"{table_name} ({', '.join(fields)}) "
            insert_query += f"VALUES ({placeholders});"

            data_to_insert = []
            for row in csv_reader:
                values = row[0:]
                data_to_insert.append(values)
            cursor.executemany(insert_query, data_to_insert)
            conn.commit()
            # print("Data inserted successfully.")
            rtn_code, rtn_value = 0, "Success"
    except Error as e:
        print(f"Error: {e}")
        rtn_code, rtn_value = -1, "Fail"
        if conn.is_connected():
            conn.rollback()
            print("Transaction rolled back.")
    finally:
        if cursor != None:
            cursor.close()
        close_db_conn(conn)
        return rtn_code, rtn_value


def is_exist(table_name, field_name, value):
    query = f"SELECT {field_name} "
    query += f"FROM {table_name} "
    query += f"WHERE {field_name} = {value};"
    rtn_code, records_list = execute_query(query)
    if rtn_code == 0 and len(records_list) > 0:
        return True
    else:
        # Not find the record or
        # table or field does not exist
        return False


def insert_new_viewer(
        uid,
        email,
        nickname,
        street,
        city,
        state,
        zip,
        genres,
        joined_date,
        first_name,
        last_name,
        subscription,
):
    users_columns = [
        "uid",
        "email",
        "joined_date",
        "nickname",
        "street",
        "city",
        "state",
        "zip",
        "genres",
    ]
    users_columns_str = ", ".join(users_columns)
    query_users = "INSERT INTO users ("
    query_users += users_columns_str
    query_users += ") VALUES ("
    query_users += ", ".join(["%s"] * len(users_columns))
    query_users += ");"
    data_users = (
        int(uid),
        email,
        joined_date,
        nickname,
        street,
        city,
        state,
        zip,
        genres,
    )
    # print(query_users)
    # print(data_users)

    query_viewers_columns = [
        "uid",
        "subscription",
        "first_name",
        "last_name",
    ]
    query_viewers_columns_str = ", ".join(query_viewers_columns)
    query_viewers = "INSERT INTO viewers ("
    query_viewers += query_viewers_columns_str
    query_viewers += ") VALUES ("
    query_viewers += ", ".join(["%s"] * len(query_viewers_columns))
    query_viewers += ")"
    data_viewers = (
        int(uid),
        subscription,
        first_name,
        last_name,
    )
    # print(query_viewers)
    # print(data_viewers)

    rtn_code, rtn_value = -1, "Fail"
    rows_affected = 0
    conn = conn_db()
    if not conn:
        print("Database connection failed.")
        return rtn_code, rtn_value

    cursor = None
    try:
        cursor = conn.cursor()
        conn.autocommit = False
        if is_exist("viewers", "uid", uid):
            # print("Viewer already exists. Do not insert it.")
            rtn_code, rtn_value = 0, None
        else:
            if is_exist("users", "uid", uid):
                # print("User already exists, do not update table users.")
                # Only insert into table viewers
                cursor.execute(query_viewers, data_viewers)
                rows_affected += cursor.rowcount
            else:
                # Insert into table users and table viewers
                cursor.execute(query_users, data_users)
                rows_affected += cursor.rowcount
                cursor.execute(query_viewers, data_viewers)
                rows_affected += cursor.rowcount
        conn.commit()
        rtn_code, rtn_value = rows_affected, "Success"
    except Error as e:
        print(f"Error: {e}")
        rtn_code, rtn_value = -1, "Fail"
        if conn.is_connected():
            conn.rollback()
            print("Can not insert viewer. Transaction rolled back.")
    finally:
        if cursor != None:
            cursor.close()
        close_db_conn(conn)
        return rtn_code, rtn_value


def add_genre(uid, genres_str):
    rows_affected = 0
    if not is_exist("users", "uid", uid):
        return -1, "Fail"
    conn = conn_db()
    if not conn:
        print("Database connection failed.")
        return rtn_code, rtn_value

    cursor = None
    try:
        cursor = conn.cursor()
        conn.autocommit = False

        genre_list = genres_str.strip().split(";")
        query = "UPDATE users SET genres = "
        query += "CONCAT(COALESCE(genres, ''), %s) "
        query += "WHERE uid = %s AND "
        query += "(LOWER(genres) NOT LIKE %s);"

        for genre in genre_list:
            genre_s = ";" + genre.strip()
            genre_lower = f"%{genre.lower()}%"
            data = (genre_s, uid, genre_lower)
            cursor.execute(query, data)
            rows_affected += cursor.rowcount

        conn.commit()
        rtn_code, rtn_value = rows_affected, "Success" if rows_affected > 0 else "Fail"
    except Error as e:
        print(f"Error: {e}")
        rtn_code, rtn_value = -1, "Fail"
        if conn.is_connected():
            conn.rollback()
            print("Can not add genre. Transaction rolled back.")
    finally:
        if cursor != None:
            cursor.close()
        close_db_conn(conn)
        return rtn_code, rtn_value


def delete_viewer(uid_str):
    rtn_code, rtn_value = -1, None
    uid = int(uid_str)
    rows_affected = 0

    if not is_exist("users", "uid", uid):
        return -1, None  # User id not found
    if not is_exist("viewers", "uid", uid):
        return -1, None  # User id not found

    conn = conn_db()
    if not conn:
        print("Database connection failed.")
        return rtn_code, rtn_value

    cursor = None
    try:
        cursor = conn.cursor()
        conn.autocommit = False

        if is_exist("producers", "uid", uid):
            # The id is in producers table, so only delete from viewers.
            query_viewers = f"DELETE FROM viewers WHERE uid = %s;"
            cursor.execute(query_viewers, (uid,))
            rows_affected += cursor.rowcount
        else:
            # The id is not in producers table, so delete from users.
            query_users = f"DELETE FROM users WHERE uid = %s;"
            cursor.execute(query_users, (uid,))
            rows_affected += cursor.rowcount

        conn.commit()
        rtn_code, rtn_value = rows_affected, "Ok"
    except Error as e:
        print(f"Error: {e}")
        rtn_code, rtn_value = -1, None
        if conn.is_connected():
            conn.rollback()
            print("Can not insert viewer. Transaction rolled back.")
    finally:
        if cursor != None:
            cursor.close()
        close_db_conn(conn)
        return rtn_code, rtn_value


def insert_movie(rid, website_url):
    rtn_code, rtn_value = -1, "Fail"
    conn = conn_db()
    if not conn:
        print("Database connection failed.")
        return -1, "Fail"

    # if not is_exist("releases", "rid", rid):
    #     err_info = f"Release id {rid} is not exists. "
    #     err_info += f"Can not insert a movie with this id."
    #     # print(err_info)
    #     return 0, "Fail"
    # if is_exist("series", "rid", rid):
    #     # The release id is in series table, so do not insert it to movies
    #     err_info = f"Release id {rid} is in series table. "
    #     err_info += f"Do not insert it to movies."
    #     # print(err_info)
    #     return 0, "Fail"
    if is_exist("movies", "rid", rid):
        # The release id is in movies table, so do not insert it again
        err_info = f"Release id {rid} is in movies table. "
        err_info += f"Do not insert it again."
        # print(err_info)
        return 0, "Fail"

    cursor = None
    try:
        cursor = conn.cursor()
        conn.autocommit = False
        query = f"INSERT INTO movies (rid, website_url) VALUES (%s, %s);"
        cursor.execute(query, (rid, website_url))
        rows_affected = cursor.rowcount
        conn.commit()
        rtn_code, rtn_value = rows_affected, "Success"
    except Error as e:
        # print(f"Error: {e}")
        rtn_code, rtn_value = -1, "Fail"
        if conn.is_connected():
            conn.rollback()
            # print("Can not insert movie. Transaction rolled back.")
    finally:
        if cursor != None:
            cursor.close()
        close_db_conn(conn)
        return rtn_code, rtn_value


def is_exist_video(rid, ep_num):
    ep_num = ep_num.strip().lower()
    if ep_num == "null":
        query = f"SELECT title FROM videos WHERE rid = %s AND ep_num is NULL;"
        data = (rid,)
    else:
        query = f"SELECT title FROM videos WHERE rid = %s AND ep_num = %s;"
        data = (rid, ep_num)
    rtn_code, rtn_value = execute_select(query, data)
    if rtn_code == 0:
        return len(rtn_value) > 0
    else:
        return False


def insert_session(
        sid, uid, rid, ep_num, initiate_at, leave_at, quality, device
):
    if is_exist("sessions", "sid", sid):
        print(f"Session id {sid} already exists. Do not insert it.")
        return 0, "Fail"
    if not is_exist("viewers", "uid", uid):
        print(
            "Viewer id is not exists, can not insert a session with this id."
        )
        return 0, "Fail"
    if not is_exist("releases", "rid", rid):
        print(
            f"Release id {rid} is not exists, can not insert a session with this id."
        )
        return 0, "Fail"

    ep_num = ep_num.strip().lower()

    if not (is_exist("movies", "rid", rid) or is_exist("series", "rid", rid)):
        error_str = f"There is no a movie or a series with this rid {rid}. "
        error_str += "Can not insert a session with this id."
        print(error_str)
        return 0, "Fail"
    if (is_exist("movies", "rid", rid) and is_exist("series", "rid", rid)):
        error_str = f"There is both a movie and a series with this rid {rid}. "
        error_str += "Something is wrong. Can not insert a session with this id."
        print(error_str)
        return 0, "Fail"
    if is_exist("movies", "rid", rid) and ep_num != "null":
        print(f"This rid {rid} is a movie, episode number must be NULL.")
        return 0, "Fail"
    if is_exist("series", "rid", rid) and ep_num == "null":
        print(f"This rid {rid} is a series, episode number must be a number.")
        return 0, "Fail"
    if not is_exist_video(rid, ep_num):
        print(f"rid {rid} and ep_num {ep_num} is not exists in table videos.")
        return 0, "Fail"

    if ep_num == "null":
        ep_num = None
    else:
        ep_num = int(ep_num)

    rtn_code, rtn_value = -1, "Fail"
    conn = conn_db()
    if not conn:
        print("Database connection failed.")
        return rtn_code, rtn_value

    cursor = None
    try:
        cursor = conn.cursor()
        conn.autocommit = False
        query = f"INSERT INTO sessions ("
        query += f"sid, uid, rid, ep_num, initiate_at, "
        query += f"leave_at, quality, device) "
        query += f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s); "
        data = (sid, uid, rid, ep_num, initiate_at,
                leave_at, quality, device,)
        # print(query)
        # print(data)
        cursor.execute(query, data)
        rows_affected = cursor.rowcount
        conn.commit()
        rtn_code, rtn_value = rows_affected, "Success"
    except Error as e:
        print(f"Error: {e}")
        rtn_code, rtn_value = -1, "Fail"
        if conn.is_connected():
            conn.rollback()
            print("Can not insert session. Transaction rolled back.")
    finally:
        if cursor != None:
            cursor.close()
        close_db_conn(conn)
        return rtn_code, rtn_value


def update_release(rid, title):
    if not is_exist("releases", "rid", rid):
        print(f"Release id {rid} is not exists, can not update it.")
        return 0, "Fail"

    rtn_code, rtn_value = -1, "Fail"
    conn = conn_db()
    if not conn:
        print("Database connection failed.")
        return -1, "Fail"

    cursor = None
    try:
        cursor = conn.cursor()
        conn.autocommit = False
        query = f"UPDATE releases SET title = %s WHERE rid = %s; "
        data = (title, rid,)
        cursor.execute(query, data)
        rows_affected = cursor.rowcount
        conn.commit()
        rtn_code, rtn_value = rows_affected, "Success"
    except Error as e:
        print(f"Error: {e}")
        rtn_code, rtn_value = -1, "Fail"
        if conn.is_connected():
            conn.rollback()
            print("Can not update table releases. Transaction rolled back.")
    finally:
        if cursor != None:
            cursor.close()
        close_db_conn(conn)
        return rtn_code, rtn_value


def execute_select(query, data=None):
    rtn_code, rtn_value = -1, None
    conn = conn_db()
    if not conn:
        print("Database connection failed.")
        return -1, None
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(query, data)
        results = cursor.fetchall()
        return 0, results
    except Error as e:
        print(f"Error: {e}")
        return -1, None
    finally:
        if cursor != None:
            cursor.close()
        close_db_conn(conn)


def list_release(uid):
    # if not is_exist("viewers", "uid", uid):
    #     print(f"Viewer id {uid} is not exists, can not list it.")
    #     return -1, None
    query = f"SELECT rid, genre, title FROM releases "
    query += f"WHERE rid in "
    query += f"(select distinct rid from reviews where uid = %s) "
    query += f"ORDER BY title; "
    data = (uid,)
    rtn_code, rtn_value = execute_select(query, data)
    return rtn_code, rtn_value


def popular_release(top_n):
    query = f"SELECT tb1.rid, tb1.title, tb2.reviewCount "
    query += f"FROM releases as tb1 JOIN "
    query += f"(SELECT rid, count(rvid) as reviewCount "
    query += f"FROM reviews GROUP BY rid) as tb2 "
    query += f"ON tb1.rid = tb2.rid "
    # query += f"ORDER BY tb2.reviewCount DESC, tb1.title "
    query += f"ORDER BY tb2.reviewCount DESC, tb1.rid DESC "  # This modification ensures the order on question 9
    query += f"LIMIT %s; "
    # print(query)
    data = (top_n,)
    rtn_code, rtn_value = execute_select(query, data)
    return rtn_code, rtn_value


# def release_title(sid):
#     # query = f"SELECT tb1.sid, tb1.rid,tb3.title, tb3.genre, "
#     query = f"SELECT tb1.sid, tb1.rid, tb3.title, tb3.genre, tb1.ep_num, tb2.title, tb2.length, "
#     query += f"tb1.ep_num, tb2.title, tb1.ep_num, tb2.length "
#     query += f"FROM sessions as tb1 JOIN videos as tb2 "
#     query += f"ON tb1.rid = tb2.rid "
#     query += f"AND COALESCE(tb1.ep_num, tb1.rid) = COALESCE(tb2.ep_num, tb2.rid) "
#     query += f"JOIN releases as tb3 "
#     query += f"ON tb1.rid = tb3.rid "
#     query += f"WHERE tb1.sid = %s "
#     query += f"ORDER BY tb3.title; "
#     data = (sid,)
#     rtn_code, rtn_value = execute_select(query, data)
#     return rtn_code, rtn_value

def release_title(sid):
    # Modified SELECT clause: Removed tb1.sid and renamed columns for clarity
    query = f"SELECT tb1.rid, tb3.title AS release_title, tb3.genre, tb2.title AS video_title, tb1.ep_num, tb2.length "
    query += f"FROM sessions AS tb1 JOIN videos AS tb2 "
    query += f"ON tb1.rid = tb2.rid "
    query += f"AND COALESCE(tb1.ep_num, tb1.rid) = COALESCE(tb2.ep_num, tb2.rid) "
    query += f"JOIN releases AS tb3 "
    query += f"ON tb1.rid = tb3.rid "
    query += f"WHERE tb1.sid = %s "
    query += f"ORDER BY tb3.title; "  # Ordering in ascending order on release title
    data = (sid,)
    rtn_code, rtn_value = execute_select(query, data)
    return rtn_code, rtn_value



def active_viewer(n, start_date, end_date):
    query = f"SELECT tb1.uid, tb2.first_name, tb2.last_name "
    query += f"FROM "
    query += f"(SELECT uid, count(sid) as num "
    query += f"FROM sessions "
    query += f"WHERE initiate_at >= %s AND initiate_at <= %s "
    query += f"GROUP BY uid "
    query += f"HAVING num >= %s ) as tb1 "
    query += f"JOIN viewers as tb2 ON tb1.uid = tb2.uid "
    query += f"ORDER BY tb1.uid, num DESC; "
    # data = (start_date + " 00:00:00", end_date + " 23:59:59", n)
    data = (start_date, end_date, n)
    return execute_select(query, data)


# def videos_viewed(rid):  # This one is calcauted based on pairs. By combining with this function. We can verify whether our output is correct or not
#     if not is_exist("videos", "rid", rid):
#         print(f"rid {rid} is not exist in table videos.")
#         return -1, None
#     # query = f"SELECT tb1.rid, tb1.ep_num, tb1.title, tb1.length, tb2.count_uid "
#     query = f"SELECT tb1.rid, tb1.ep_num, tb1.title, tb1.length, COALESCE(tb2.count_uid, 0) AS count_uid "
#     query += f"FROM videos as tb1 "
#     query += f"LEFT JOIN "
#     query += f"(SELECT rid, COALESCE(ep_num, rid) AS new_ep_num, count(distinct uid) as count_uid "
#     query += f"FROM sessions "
#     query += f"GROUP BY rid, COALESCE(ep_num, rid)) as tb2 "
#     # query += f"ON tb1.rid = tb2.rid and COALESCE(tb1.ep_num, tb1.rid) = tb2.new_ep_num "
#     # query += f"GROUP BY rid "
#     query += f"ON tb1.rid = tb2.rid "
#     query += f"WHERE tb1.rid = %s "
#     # query += f"ORDER BY rid DESC, count_uid DESC;"
#     query += f"ORDER BY tb1.rid DESC;"

#     data = (rid,)
#     return execute_select(query, data)


def videos_viewed(rid):
    if not is_exist("videos", "rid", rid):
        print(f"rid {rid} is not exist in table videos.")
        return -1, None
    query = f"SELECT tb1.rid, tb1.ep_num, tb1.title, tb1.length, COALESCE(tb2.count_uid, 0) AS count_uid "
    query += f"FROM videos as tb1 "
    query += f"LEFT JOIN "
    query += f"(SELECT rid, COUNT(DISTINCT uid) as count_uid "
    query += f"FROM sessions "
    query += f"GROUP BY rid) as tb2 "
    query += f"ON tb1.rid = tb2.rid "
    query += f"WHERE tb1.rid = %s "
    query += f"ORDER BY tb1.rid DESC, tb1.ep_num ASC;"
    data = (rid,)
    return execute_select(query, data)

