from flask import Flask, render_template,request,redirect
import mysql.connector
from cassandra.cluster import Cluster
import json
import redis

cluster = Cluster()
session = cluster.connect('samplemeta')

app = Flask(__name__)

@app.route('/', methods=['GET'])
def hello_world():
    return render_template('index.html')

@app.route('/check_database', methods=['POST'])
def check_db_existence():
    partition_key_name = request.form.get('db_name')
    query = "select databasename from relationalmetadata group by databasename"
    result = session.execute(query)
    print("in check_db ", "\n ", result)
    for row in result:
        if row.databasename == partition_key_name:
            query_index = menuMaker(partition_key_name)
            return render_template('display_views.html', data=query_index, databasename=partition_key_name)
    return render_template('Nodb_filler.html')


@app.route('/display_views', methods=['POST'])
def display_data_view():
    table_data = json.loads(request.form.get('selectedData'))
    databasename = request.form.get('dataBaseName')
    print(databasename)
    query_index = dict()
    columns_info = []
    for key,columns in table_data.items():
        table_name = key.replace(":","")
        query_index[table_name] = columns
        for element in columns:
            columns_info.append(element)

    # print(columns_info)
    fetched_data = DataRetriver(databasename,query_index)
    # print(fetched_data)
    redis_sql(fetched_data)
    return render_template('fetched_data.html',columns=columns_info,rows=fetched_data)

def redis_sql(data):
    try:
        r = redis.Redis(host='localhost', port=6379,
                        decode_responses=True)
        print(r)
        for row in data:
            value = row[0:2] + row[3:]
            r.hset(name='name', key=str(row[2]), value=str(value), mapping=None, items=None)
            print(row)
        print('Cached data to redis...')
    except Exception as e:
        print('something went wrong!', e)

@app.route("/process_form", methods=['POST'])
def SaveDBMetaData():
    query_index = dict()
    host = request.form.get('hostname')
    user = request.form.get('username')
    password = request.form.get('password')
    database = request.form.get('database')

    db_config  = {
        "host" :  f"{host}",
        "user" : f"{user}",
        "password": f"{password}",
        "database": f"{database}"
    }

    db_config_string = f"host : '{host}', user  : '{user}', password : '{password}', database : '{database}'"
    # print(db_config_string)
    table_to_dataFields = dict()
    # Connect to the MySQL database
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    # Replace with the desired table name
    query = "SHOW TABLES"

    # Execute the query
    cursor.execute(query)

    # Fetch the table names
    tables = cursor.fetchall()

    # Extract table names from the fetched data
    table_names = [table[0] for table in tables]


    for table_name in table_names:
        try:
            # Construct the query to retrieve column information
            query = f"SHOW COLUMNS FROM {table_name}"

            # Execute the query
            cursor.execute(query)

            # Fetch the column information
            columns = cursor.fetchall()
            column_names_to_display = [column[0] for column in columns]
            query_index[table_name] = column_names_to_display

            column_names = [f"'{column[0]}'" for column in columns]
            column_names_string = ','.join(map(str, column_names))
            table_to_dataFields[table_name] = column_names

            cassandra_query = f"insert into relationalmetadata(databasename,tablename,db_config,datafields) values(" \
                              f"'{database}'," \
                              f"'{table_name}'," \
                              "{" + f"{ db_config_string }" + "}," \
                              f"[{column_names_string}]" \
                              f");"
            print(cassandra_query, "\n")
            session.execute(cassandra_query)
        except mysql.connector.Error as err:
            print("Error:", err)
    print("Database metadata successfully saved")
    # Close the cursor and connection
    cursor.close()
    connection.close()
    return render_template('display_views.html', data=query_index, databasename=database)

def menuMaker(database) -> dict():
    selected_columns_from_tables = dict()
    cassandra_query = f"select * from relationalmetadata where databasename='{database}'"
    result = session.execute(cassandra_query)
    for row in result:
        selected_columns_from_tables[row.tablename] = row.datafields
    print(selected_columns_from_tables)
    return selected_columns_from_tables

def DataRetriver(database,query_index):
    rows = []
    cassandra_query = f"select * from relationalmetadata where databasename='{database}'"
    result = session.execute(cassandra_query)[0]
    db_config = {
        "host": f"{result.db_config.host}",
        "user": f"{result.db_config.user}",
        "password": f"{result.db_config.password}",
        "database": f"{result.db_config.database}"
    }
    print(db_config)

    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    columns_info = ""

    for i, (key,value) in enumerate(query_index.items()):
        table_name = key
        for index,element in enumerate(value):
            columns_info += f"{table_name}.{element}"
            if index == len(value) - 1:
                continue
            columns_info += ","
        if i == len(query_index.items()) - 1:
            continue
        columns_info += ","

    table_names = []
    for (key,value) in query_index.items():
        table_names.append(key)

    query = ""

    no_of_tables = len(table_names)
    if no_of_tables == 2:
        query = f"select  {columns_info} from {table_names[0]} inner join {table_names[1]} on {table_names[0]}.id = {table_names[1]}.id"
    else:
        query = f"select {columns_info} from {table_names[0]} inner join {table_names[1]} on {table_names[0]}.id = {table_names[1]}.id " \
            f"inner join {table_names[2]} on {table_names[1]}.id = {table_names[2]}.id"
    print(query)
    try:
        # Execute the query
        cursor.execute(query)

        # Fetch all the rows
        rows = cursor.fetchall()

        # Print the fetched rows
        # print(rows)
        for row in rows:
            print(row)
        # final = rows
    except mysql.connector.Error as err:
        print("Error:", err)

    cursor.close()
    connection.close()
    return rows

if __name__ == '__main__':
    if __name__ == "__main__":
        app.run(debug=True, port=8000)