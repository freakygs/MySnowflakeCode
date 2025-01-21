import snowflake.connector
import os
import re
import requests
import io

stage_name = 'stg_deployment'
teams_channel = '<teams webhook>'


##

def check_for_double_quotes(param):
    buf = io.StringIO(param)
    for i in range(len(str(param).split('\n'))):
        original_line = str(buf.readline())
        new_line = original_line.strip().upper()
        if ("CREATE TABLE" in new_line or "CREATE PROCEDURE" in new_line):
            if '"' in new_line:
                new_line = new_line.replace('"', '')
                param = param.replace(original_line, new_line)
                print(new_line)

    return param


def replacing_string(complete_str):
    compiled_table = re.compile(re.escape("create or replace table"), re.IGNORECASE)
    compiled_procedure = re.compile(re.escape("create or replace procedure"), re.IGNORECASE)
    res_table = compiled_table.sub("create table ", complete_str)
    res = compiled_procedure.sub("create procedure ", res_table)

    normalized_string = re.sub(r"\s+", "", complete_str.lower())
    normalized_pattern = re.sub(r"\s+", "", "create or replace procedure".lower())

    if re.search(normalized_pattern, normalized_string):
        return False
    else:
        return check_for_double_quotes(complete_str)

    final_query = str(res)
    final_query = check_for_double_quotes(final_query)
    return final_query


def check_before_deployment(s, file_name):
    try:
        validate_string = ' '.join(' '.join(map(str, s)).strip().split()).lower()
        if validate_string.find('alter table') != -1:
            ## Exception
            raise ("Please Check the file -- " + file_name)
        RuntimeError
        return 1
    except Exception as err:
        send_message_to_teams(teams_channel, str(err) + str(
            err) + " Found alter command, Please make changes to script to use drop and create")


def create_views(cs, last_updated_timestamp, file_name):
    table_list = []
    try:
        cs.execute(
            "select TABLE_NAME from INFORMATION_SCHEMA.TABLES where  TABLE_TYPE='BASE TABLE' and LAST_ALTERED > '" + str(
                last_updated_timestamp) + "' order by 1;")
        table_list = list(cs.fetchall())
        for e in table_list:
            ##call procedure to create views
            cs.execute("CALL <schema>.SP_ETL_VIEWS_REFRESH('" + e[0] + "')")
    except Exception as err:
        error_message = str(err) + " "
        if len(table_list) == 0:
            error_message = error_message + "Issue while creating views for the file - " + file_name
        else:
            error_message = error_message + "Issue while creating views for tables are "
            for e in table_list:
                error_message = error_message + e[0] + ", "
        send_message_to_teams(teams_channel, error_message)


def send_message_to_teams(webhook_url, message):
    headers = {
        'Content-Type': 'application/json'
    }

    data = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "summary": "Notification from Python",
        "themeColor": "0078D7",
        "title": "Notification from Python",
        "text": message
    }

    response = requests.post(webhook_url, headers=headers, json=data)

    if response.status_code == 200:
        print("Message sent successfully.")
    else:
        print("Failed to send message.")


# stage_name = 'automation'
PASSWORD = os.getenv('SNOWSQL_PWD')
UserID = '<SnowflakeUser>'
# UserID = os.getenv('Snowflake_user')
# PASSWORD = os.getenv('Snowflake_password')
# print(UserID, PASSWORD)
PASSWORD = '<password>'

ctx = snowflake.connector.connect(
    user=UserID,
    password=PASSWORD,
    account='<account>',
    role='SYSADMIN',
    database='<database>',
    schema='<Schema>',
    warehouse='<warehouse>'
)
cs = ctx.cursor()

try:
    # Get last updated Timestamp
    cs.execute(
        "select max(FILE_LOADED_TIMESTAMP) from <database>.<schema>.DEPLOYMENT_TIMESTAMP where EXECUTED = True")
    last_updated_timestamp = cs.fetchone()

    cs.execute("list @" + stage_name)
    stage_list_query_id = cs.sfqid
    cs.execute("SELECT $1 as name, $4 as timestamp FROM TABLE(RESULT_SCAN('" + str(
        stage_list_query_id) + "')) where to_varchar(to_timestamp(timestamp,'DY, DD MON YYYY HH24:MI:SS TZD') ,'YYYY-MM-DD HH24:MI:SS') > '" + str(
        last_updated_timestamp[0]) + "' order by timestamp;")
    file_lst = list(cs.fetchall())
    result_set = []

    for e in file_lst:
        cs.execute(
            "select $1 from  @" + str(e[0]) + "(file_format => '<schema>.FF_deployment') where $1 is not null")
        result_set = cs.fetchall()
        s = [" ".join(x) for x in result_set]
        # query_to_run = ' '.join(' '.join(map(str, s)).strip().split())
        complete_str = ''.join(s)
        # re.sub(r"^\s+|\s+$", "", s), sep='')
        # complete_str_for_check = ' '.join(' '.join(map(str, s)).strip().split()).lower()
        query_to_run = re.sub(' +', ' ', complete_str)
        comments = ""
        try:
            if check_before_deployment(s, str(e[0])):
                #if (query_to_run.lower().find('create or replace table') != -1) or query_to_run.lower().find(
               #         'create or replace procedure') != -1 or query_to_run.lower().find(''):
                query_to_run = replacing_string(query_to_run)
                query_to_run = 'Begin ' + query_to_run + 'End;'
                cs.execute(query_to_run)
                comments = "files was successfully deployed"
                cs.execute("insert into <database>.<schema>.DEPLOYMENT_TIMESTAMP values('" + e[
                    0] + "' , true, to_varchar(to_timestamp( '" + e[
                               1] + "','DY, DD MON YYYY HH24:MI:SS TZD') ,'YYYY-MM-DD HH24:MI:SS'), '" + comments + "');")
                # create_views(cs, last_updated_timestamp[0], str(e[0]))
            else:
                ### Exception
                comments = str("Failed at checks before deployment, Please check - " + e[0]).replace("'", "\"")
                cs.execute("insert into <database>.<schema>.DEPLOYMENT_TIMESTAMP values('" + e[
                    0] + "' , false, to_varchar(to_timestamp( '" + e[
                               1] + "','DY, DD MON YYYY HH24:MI:SS TZD') ,'YYYY-MM-DD HH24:MI:SS'), '" + comments + "');")
                send_message_to_teams(teams_channel, comments)
        except Exception as err:
            comments = str(err).replace("'", "\"") + " --- Error while executing the file - " + e[0]
            cs.execute("insert into <database>.<schema>.DEPLOYMENT_TIMESTAMP values('" + e[
                0] + "' , false, to_varchar(to_timestamp( '" + e[
                           1] + "','DY, DD MON YYYY HH24:MI:SS TZD') ,'YYYY-MM-DD HH24:MI:SS'), '" + comments + "');")
            send_message_to_teams(teams_channel, comments)
except Exception as err:
    send_message_to_teams(teams_channel, str(err) + " --- Error while getting file for execution")

finally:
    cs.close()
ctx.close()
