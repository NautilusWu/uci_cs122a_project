from datetime import datetime
import os

import mysql_do

def get_csv_files(path):
    """Get all .csv file names in a path"""
    csv_files_name = []
    if os.path.isdir(path):
        for filename in os.listdir(path):
            if filename.endswith(".csv"):
                csv_files_name.append(filename)
    return csv_files_name


def f_import(folder_path):
    # Delete existing tables
    rtn_code, _ = mysql_do.del_existing_tables()
    if rtn_code != 0:
        # print("Error deleting existing tables.")
        return False
    # print("All existing tables deleted successfully.\n")

    # Create new tables
    rtn_code, _ = mysql_do.create_new_tables()
    if rtn_code != 0:
        # print("Error creating new tables.")
        return False
    # print("All new tables created successfully.\n")

    # Import files
    csv_files = get_csv_files(folder_path)
    if len(csv_files) == 0:
        return False
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
    tables_csv = [tb_name + ".csv" for tb_name in tables]
    if not all(file in csv_files for file in tables_csv):
        return False
    for i in range(len(tables)):
        # print(f"Importing {tables[i]}")
        rtn_code, _ = mysql_do.insert_from_csv(
            tables[i], os.path.join(folder_path, tables[i] + ".csv")
        )
        if rtn_code != 0:
            # print(f"Error importing {csv_files[i]}")
            return False
    # print("All files imported successfully.\n")
    return True


def check_id(id):
    try:
        num = int(id)
        return num >= 0
    except ValueError:
        return False


def check_email(email):
    if not (("@" in email) and ("." in email)):
        return False
    return True


def check_zip(zip):
    if len(zip) != 5:
        return False
    if not zip.isdigit():
        return False
    return True


def check_date(joined_date):
    try:
        datetime.strptime(joined_date, "%Y-%m-%d")
        return True
    except ValueError:
        return False

def check_datetime(datetime_str):
    try:
        datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
        return True
    except ValueError:
        return False

def check_subscription(subscription):
    subscriptions_list = ["free", "monthly", "yearly"]
    if subscription.lower() not in subscriptions_list:
        return False
    return True


def convert_genres(genres_str):
    genres_list = genres_str.split(",")
    genres_list = [g.strip() for g in genres_list]
    keep_unique_genres = set()
    result = []
    for item in genres_list:
        lower_item = item.lower()
        if lower_item not in keep_unique_genres:
            keep_unique_genres.add(lower_item)
            result.append(item)

    genres = ";".join(result)
    return genres


def check_website_url(website_url):
    url = website_url.strip().lower()
    if not url.startswith("http://") and not url.startswith("https://"):
        return False
    return True


def f_insertviewer(params_list):
    if len(params_list) < 12:
        # print("Not enough parameters.")
        return False
    uid = params_list[0]
    email = params_list[1]
    nickname = params_list[2]
    street = params_list[3]
    city = params_list[4]
    state = params_list[5]
    zip = params_list[6]
    genres = params_list[7]
    joined_date = params_list[8]
    first_name = params_list[9]
    last_name = params_list[10]
    subscription = params_list[11]

    user_id_valid = check_id(uid)
    if not user_id_valid:
        # print(f"User id {uid} is not valid.")
        return False
    email_valid = check_email(email)
    if not email_valid:
        # print(f"Email {email} is not valid.")
        return False
    zip_valid = check_zip(zip)
    if not zip_valid:
        # print(f"Zip {zip} is not valid.")
        return False
    genres = convert_genres(genres)
    joined_date_valid = check_date(joined_date)
    if not joined_date_valid:
        # print(f"Joined date {joined_date} is not valid.")
        return False
    subscription_valid = check_subscription(subscription)
    if not subscription_valid:
        # print(f"Subscription {subscription} is not valid.")
        return False
    rtn, _ = mysql_do.insert_new_viewer(
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
        subscription.lower(),
    )
    if rtn > 0:
        # print(f"Viewer {uid} {email} {nickname} inserted successfully.\n")
        return True
    else:
        # print("Error inserting new viewer.\n")
        return False


def f_addgenre(params_list):
    if len(params_list) < 2:
        # print("Not enough parameters.")
        return False

    uid = params_list[0]
    genre = params_list[1]

    user_id_valid = check_id(uid)

    if not user_id_valid:
        # print(f"User id {uid} is not valid.")
        return False
    genre = convert_genres(genre)

    rtn, _ = mysql_do.add_genre(uid, genre)
    if rtn > 0:
        # print(f"Genre {genre} added successfully.\n")
        return True
    else:
        # print(
        #     "Error adding genre. Maybe user id is not exist or genre already exists.\n"
        # )
        return False


def f_deleteviewer(params_list):
    if len(params_list) < 1:
        # print("Not enough parameters.")
        return False

    uid = params_list[0]

    user_id_valid = check_id(uid)
    if not user_id_valid:
        # print(f"User id {uid} is not valid.")
        return False

    rtn, _ = mysql_do.delete_viewer(uid)
    if rtn > 0:
        # print(f"Viewer ID {uid} deleted successfully.\n")
        return True
    else:
        # print(f"Error deleting viewer ID {uid}.\n")
        return False


def f_insertmovie(params):
    if len(params) < 2:
        # print("Not enough parameters.")
        return False

    rid = params[0]
    website_url = params[1]

    if not check_id(rid):
        # print(f"Release id {rid} is not valid.")
        return False
    # if not check_website_url(website_url):
    #     # print(f"Website url {website_url} is not valid.")
    #     return False
    website_url = website_url.strip().lower()

    rtn, _ = mysql_do.insert_movie(int(rid), website_url)
    if rtn > 0:
        # print(f"Movie {rid} inserted successfully.\n")
        return True
    else:
        # print("Error inserting new movie.\n")
        return False


def f_insertsession(params):
    if len(params) < 8:
        # print("Not enough parameters.")
        return False

    sid = params[0]
    uid = params[1]
    rid = params[2]
    ep_num = params[3].strip().lower()
    initiate_at = params[4].strip().lower()
    leave_at = params[5].strip().lower()
    quality = params[6].strip().lower()
    device = params[7].strip().lower()

    if not check_id(sid):
        # print(f"Session id {sid} is not valid.")
        return False
    if not check_id(uid):
        # print(f"User id {uid} is not valid.")
        return False
    if not check_id(rid):
        # print(f"Release id {rid} is not valid.")
        return False
    if ep_num != "null":
        if not (check_id(ep_num) and int(ep_num) > 0):
            # print(f"Episode number {ep_num} is not valid.")
            return False
    if not check_datetime(initiate_at):
        # print(f"Initiate at {initiate_at} is not valid.")
        return False
    if not check_datetime(leave_at):
        # print(f"Leave at {leave_at} is not valid.")
        return False
    rtn, _ = mysql_do.insert_session(
        int(sid), int(uid), int(rid), ep_num,
        initiate_at, leave_at, quality, device)
    if rtn > 0:
        # print(f"Session {sid} inserted successfully.\n")
        return True
    else:
        # print("Error inserting new session.\n")
        return False

def f_updaterelease(params):
    if len(params) < 2:
        # print("Not enough parameters.")
        return False

    rid = params[0]
    title = params[1]

    if not check_id(rid):
        # print(f"Release id {rid} is not valid.")
        return False

    rtn, result = mysql_do.update_release(int(rid), title)
    if rtn > 0:
        # print(f"Release {rid} updated successfully.\n")
        return True
    else:
        if result == "Success":
            # print(f"Release title is same as before, nothing changed.\n")
            return True
        # print("Error updating release.\n")
        return False

def f_listrelease(params):
    if len(params) < 1:
        # print("Not enough parameters.")
        return False

    uid = params[0]

    if not check_id(uid):
        # print(f"User id {uid} is not valid.")
        return False

    rtn, result = mysql_do.list_release(int(uid))
    if rtn == 0:
        print(f"rid{' '*2}genre{' '*17}title")
        for row in result:
            print(f"{row[0]:>3}  {row[1]:<20}  {row[2]:<8}") 
        return True
    else:
        return False

def f_popularrelease(params):
    if len(params) < 1:
        # print("Not enough parameters.")
        return False

    top_n = params[0]

    if not check_id(top_n):
        # print(f"Top {top_n} is not valid.")
        return False
    if int(top_n) <= 0:
        # print(f"Top {top_n} must be greater than 0.\n")
        return False

    rtn, result = mysql_do.popular_release(int(top_n))
    if rtn == 0:
        if len(result) == 0:
            print(f"There is no any reviewed release.\n")
            return True
        # print(f"The most popular {top_n} releases are: ")
        print(f"rid{' '*2}title{' '*17}reviewCount")
        for row in result:
            print(f"{row[0]:>3}  {row[1]:<20}  {row[2]:<8}") 
        return True
    else:
        return False

def f_releasetitle(params):
    if len(params) < 1:
        # print("Not enough parameters.")
        return False
    sid = params[0]
    if not check_id(sid):
        # print(f"Release id {sid} is not valid.")
        return False

    rtn, result = mysql_do.release_title(int(sid))
    print(f"rid{' '*2}release_title{' '*9}genre{' '*17}video_title{' '*8}ep_num{' '*2}length")
    for row in result:
        if row[4] is None:
            ep_num = ""
        else:
            ep_num = row[4]
        print(f"{row[0]:>3}  {row[1]:<20}  {row[2]:<20}  {row[3]:<17}  {ep_num:<6}  {row[5]:<8}") 

def f_activeviewer(params):
    # print('; '.join(params))
    if len(params) < 3:
        print("Not enough parameters.")
        return False

    n = params[0]
    start_date = params[1]
    end_date = params[2]
    if not check_id(n) or int(n) < 1:
        print(f"N is not valid.")
        return False        
    # if not check_date(start_date) or not check_date(end_date):
    #     # print(f"Start date or end date is not valid.")
    #     return False
    rtn, result = mysql_do.active_viewer(int(n), start_date, end_date)
    if rtn == 0 :
        # print(f"UID{' '*2}first name{' '*3}last name")
        for row in result:
            print(f"{row[0]:>3}  {row[1]:<11}  {row[2]}") 
        return True
    else:
        return False
    return False
 
def f_videosviewed(params):
    if len(params) < 1:
        # print("Not enough parameters.")
        return False

    rid = params[0]
    rtn, result = mysql_do.videos_viewed(int(rid))
    if rtn >=0 :
        print(f"RID{' '*2}ep_num{' '*2}title{' '*20}length{' '*2}COUNT")
        for rec in result:
            count = 0 if rec[4] is None else  int(rec[4])
            ep_num = " " if rec[1] is None else  int(rec[1])
            print(f"{int(rec[0]):>3}{' '*2}{ep_num:>4}{' '*4}{rec[2]:<20}{' '*5}{rec[3]:<5}{' '*3}{count}")
    else:
        return False
