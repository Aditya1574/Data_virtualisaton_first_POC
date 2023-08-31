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
    for row in result:
        if row.databasename == partition_key_name:
            query_index = menuMaker(partition_key_name)
            return render_template('display_views.html', data=query_index, databasename=partition_key_name)
    return render_template('Nodb_filler.html')

@app.route("/process_form", methods=['POST'])
def DBMiddleWareBeforeSavingMetaData():
    query_index = dict()
    host = request.form.get('hostname')
    user = request.form.get('username')
    password = request.form.get('password')
    database = request.form.get('database')

    db_config = {
        "host": f"{host}",
        "user": f"{user}",
        "password": f"{password}",
        "database": f"{database}"
    }

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

            # this goes up for being displayed in the front-end
            column_names_to_display = [column[0] for column in columns]
            query_index[table_name] = column_names_to_display

        except mysql.connector.Error as err:
            print("Error:", err)

    # Close the cursor and connection
    cursor.close()
    connection.close()
    return render_template('table_primary_key.html',data=query_index,host=host,user=user,password=password, databasename=database)


@app.route("/fulltabledata", methods=['POST'])
def SaveMetaDataToCassandra():
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
    db_config_string = f"host: '{host}',user : '{user}',password: '{password}', database : '{databasename}'"
    # Connect to the MySQL database
    print(db_config_string, "\n", tableName_primaryKey)

    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    for table_name, primary_key in tableName_primaryKey.items():
        print(type(table_name),type(primary_key))
        print(table_name,primary_key)


    for table_name,primary_key in tableName_primaryKey.items():
        try:
            query = f"SHOW COLUMNS FROM {table_name}"

            cursor.execute(query)

            # Fetch the column information
            columns = cursor.fetchall()
            column_names = [f"'{column[0]}'" for column in columns]
            column_names_string = ','.join(map(str, column_names))

            cassandra_query = f"insert into relationalmetadata(databasename,primary_key,tablename,db_config,datafields) values(" \
                              f"'{databasename}'," \
                              f"'{primary_key}'," \
                              f"'{table_name}'," \
                              "{" + f"{db_config_string}" + "}," \
                              f"[{column_names_string}]" \
                              f");"
            print(cassandra_query, "\n", "Saving Table metadata to cassandra using the abobe query \n")
            session.execute(cassandra_query)

        except mysql.connector.Error as err:
            print("Error:", err)

    # Close the cursor and connection
    cursor.close()
    connection.close()
    return render_template('options.html',databasename=databasename)

@app.route('/options',methods=['POST'])
def option_selection_middleware():
    selected_option = request.form.get('selected_option')
    database_name = request.form.get('db_name')

    print("inside post request on /options  : ", database_name)
    if selected_option == 'add_tables':
        # logic for checking which table names to proceed with tables not present in cassandra

        # 1. get the table names from cassandra along with db_config details using the variable database_name : dictionary
        cassandra_query = f"select db_config from relationalmetadata where databasename='{database_name}' limit 1"
        result = session.execute(cassandra_query)[0]
        print(result)
        db_config = {
            "host": f"{result.db_config.host}",
            "user": f"{result.db_config.user}",
            "password": f"{result.db_config.password}",
            "database": f"{result.db_config.database}"
        }
        cassandra_query = f"select tablename from relationalmetadata where databasename='{database_name}'"
        result = session.execute(cassandra_query)
        tables_in_cassandra = dict()
        for element in result:
            tables_in_cassandra[element.tablename] = True
        # 2. use db_config detials to show return all the table names from the mysql database : list
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        query = "SHOW TABLES"

        # Execute the query
        cursor.execute(query)

        # Fetch the table names
        tables = cursor.fetchall()

        # Extract table names from the fetched data
        table_names = [table[0] for table in tables]
        # 3. those false are to be dislpayed : do something if empty list else something else

        tables_not_present = []
        for table_name in table_names:
            if tables_in_cassandra[table_name] == False:
                tables_not_present.append(table_name)

        if len(tables_not_present) == 0:
            return f"All tables already selected in {database_name}"
        return render_template('add_tables.html',db_name=database_name, data=tables_not_present)
    elif selected_option == 'generate_report':
        # Handle generating report

        # 1. using database_name get table names and column names  : make query_index

        # 2. render a page  [ display_views.html ]
        return "Generating report"

@app.route('/addTableHandler',methods=['POST'])
def SaveAddedtableMetaData():
    selected_tables = request.form.getlist('selected_tables[]')
    db_name = request.form.get('db_name')

    print(selected_tables)
    print("Database name : ",db_name)

    return "data collected"

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
