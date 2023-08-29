from flask import Flask, render_template,request
import mysql.connector
from cassandra.cluster import Cluster
import json
import redis
import time

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

@app.route("/process_form", methods=['POST'])
def DBMiddleWare():
    query_index = dict()
    host = request.form.get('hostname')
    user = request.form.get('username')
    password = request.form.get('password')
    database = request.form.get('database')


    return render_template('table_primary_key.html',data=query_index,host=host,user=user,password=password, databasename=database)


@app.route("/fulltabledata", methods=['POST'])
def SaveMetaData():
    databasename = request.form.get('dataBaseName')
    host = request.form.get('host')
    user = request.form.get('user')
    password = request.form.get('password')
    tableName_primaryKey = json.loads(request.form.get('primary_key_details'))

    db_config = {
        "host": f"{host}",
        "user": f"{user}",
        "password": f"{password}",
        "database": f"{databasename}"
    }

    db_config_string = f"host : '{host}', user  : '{user}', password : '{password}', database : '{databasename}'"
    print(tableName_primaryKey, "\n", db_config_string)
    return "successfully got the data"

    # table_to_dataFields = dict()
    # # Connect to the MySQL database
    # connection = mysql.connector.connect(**db_config)
    # cursor = connection.cursor()
    #
    # # Replace with the desired table name
    # query = "SHOW TABLES"
    #
    # # Execute the query
    # cursor.execute(query)
    #
    # # Fetch the table names
    # tables = cursor.fetchall()
    #
    # # Extract table names from the fetched data
    # table_names = [table[0] for table in tables]
    # for table_name in table_names:
    #     try:
    #         # Construct the query to retrieve column information
    #         query = f"SHOW COLUMNS FROM {table_name}"
    #
    #         # Execute the query
    #         cursor.execute(query)
    #
    #         # Fetch the column information
    #         columns = cursor.fetchall()
    #
    #         # this goes up for being displayed in the front-end
    #         column_names_to_display = [column[0] for column in columns]
    #         query_index[table_name] = column_names_to_display
    #
    #         # this is for making the cassandra query
    #         column_names = [f"'{column[0]}'" for column in columns]
    #         column_names_string = ','.join(map(str, column_names))
    #         table_to_dataFields[table_name] = column_names
    #
            # cassandra_query = f"insert into relationalmetadata(databasename,primary_key,tablename,db_config,datafields) values(" \
            #                   f"'{database}'," \
            #                   f"'{tableName_primaryKey[table_name]}'"
            #                   f"'{table_name}'," \
            #                   "{" + f"{db_config_string}" + "}," \
            #                                                 f"[{column_names_string}]" \
            #                                                 f");"
    #         print(cassandra_query, "\n", "Saving Table metadata to cassandra using the abobe query \n")
    #         session.execute(cassandra_query)
    #     except mysql.connector.Error as err:
    #         print("Error:", err)
    #
    # # Close the cursor and connection
    # cursor.close()
    # connection.close()


@app.route('/display_views', methods=['POST'])
def display_data_view():
    table_data = json.loads(request.form.get('selectedData'))
    databasename = request.form.get('dataBaseName')
    selectedForeignKeys = json.loads(request.form.get('selectedForeignKeydData'))

    print(selectedForeignKeys)
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
        # print(r)
        print('connected to redis...')
        for row in data:
            value = row[0:2] + row[3:]
            r.hset(name='name', key=str(row[2]), value=str(value), mapping=None, items=None)
            # print(row)
        print('Cached data to redis...')
    except Exception as e:
        print('something went wrong!', e)






def menuMaker(database) -> dict():
    selected_columns_from_tables = dict()
    cassandra_query = f"select * from relationalmetadata where databasename='{database}'"
    result = session.execute(cassandra_query)
    for row in result:
        selected_columns_from_tables[row.tablename] = row.datafields
    # print(selected_columns_from_tables)
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
    # print(db_config)

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

    query = f"select  {columns_info} from {table_names[0]}"


    no_of_tables = len(table_names)
    for i in range(1,no_of_tables):
        query += f" inner join {table_names[i]} on {table_names[i-1]}.id = {table_names[i]}.id"

    print(query)
    try:
        # Execute the query
        start_time = time.time()
        cursor.execute(query)
        end_time = time.time()

        print("time taken for sql db query ", end_time - start_time)
        # Fetch all the rows
        rows = cursor.fetchall()
        # print the rows or do whatever you want with the rows
    except mysql.connector.Error as err:
        print("Error:", err)

    cursor.close()
    connection.close()
    return rows

if __name__ == '__main__':
    if __name__ == "__main__":
        app.run(debug=True, port=8000)
