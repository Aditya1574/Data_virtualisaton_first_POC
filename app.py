from flask import Flask, render_template,request
import mysql.connector
from cassandra.cluster import Cluster
import json
import redis
import time
from collections import OrderedDict
import pandas as pd
import xml.etree.ElementTree as ET

cluster = Cluster()
session = cluster.connect('samplemeta')

app = Flask(__name__)

def get_filename(file_path):
    start = file_path.rfind('\\')
    end = file_path.rfind('.')
    file_name = file_path[start + 1:end]
    return file_name

def JSONflatten_dict(d, parent_key='', sep='.'):
    items = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(JSONflatten_dict(v, new_key, sep=sep))
        elif isinstance(v, list):
            if v and isinstance(v[0], dict):
                # Handle nested dictionaries within the array
                for i, item in enumerate(v):
                    items.update(JSONflatten_dict(item, new_key, sep=sep))
            else:
                # Convert array values to a single string with comma separation
                items[new_key] = ', '.join(map(str, v))
        else:
            items[new_key] = v

    return items


def flatten_xml(element, parent_key='', separator='.'):
    flattened_data = {}

    for child_element in element:
        child_key = parent_key + separator + child_element.tag
        if len(child_element) > 0:
            flattened_data.update(flatten_xml(child_element, child_key, separator))
        else:
            flattened_data[child_key] = child_element.text

    return flattened_data

@app.route('/', methods=['GET'])
def hello_world():
    return render_template('index.html')


@app.route('/options_selected', methods=['POST'])
def options_selected():
    selected_options = request.form.getlist('options[]')
    # print(selected_options)
    return render_template('data_collection.html',selected_options=selected_options)

@app.route('/data_collected',methods=['POST'])
def collected_data_processing():

    relational_data = dict()
    json_data = dict()
    xml_data = dict()
    excel_data = dict()

    if request.form.get('relational-data'):
        relational_data = json.loads(request.form.get('relational-data'))
        print(relational_data)
    if request.form.get('json-data'):
        json_data = json.loads(request.form.get('json-data'))
    if request.form.get('xml-data'):
        xml_data = json.loads(request.form.get('xml-data'))
    if request.form.get('excel-data'):
        excel_data = json.loads(request.form.get('excel-data'))
        print(excel_data)

    table_names_send = []
    relational_present = False
    databasename=""
    if relational_data:

        db_config={
            "host": relational_data['hostname'],
            "user": relational_data['user'],
            "password": relational_data['password'],
            "database": relational_data['database']
        }
        databasename=relational_data['database']
        # Connect to the MySQL database
        # right now fetching from sql server will fetch from cassandra
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Replace with the desired table name
        query = "SHOW TABLES"

        # Execute the query
        cursor.execute(query)

        # Fetch the table names
        tables = cursor.fetchall()

        # Extract table names from the fetched data
        table_names_send = [table[0] for table in tables]
        relational_present = True

        print(table_names_send)

        cursor.close()
        connection.close()

    excel_present = False
    sheet_names_send = []
    excel_filepath = ""
    if excel_data:
        # Replace 'your_file.xlsx' with the path to your Excel file
        excel_filepath = excel_data['excel_filepath']

        # Using ExcelFile object
        xls = pd.ExcelFile(excel_filepath)
        sheet_names = xls.sheet_names

        # Using pd.read_excel
        sheet_names_send = pd.ExcelFile(excel_filepath).sheet_names
        excel_present = True

        print(sheet_names_send)

    json_present = False
    json_filename = ""
    json_filepath = ""
    if json_data:
        json_filepath = json_data['json_filepath']
        json_filename = get_filename(json_filepath)
        json_present = True
        print(json_filename)

    xml_present = False
    xml_filename = ""
    xml_filepath = ""
    if xml_data:
        xml_filepath = xml_data['xml_filepath']
        xml_filename = get_filename(xml_filepath)
        xml_present = True
        print(xml_filename)


    return render_template('view_selection.html',relational_present=relational_present,tables=table_names_send,databasename=databasename,
                           excel_present=excel_present,sheet_names=sheet_names_send,excel_filepath=excel_filepath,
                           json_present=json_present,json_filename=json_filename,json_filepath=json_filepath,
                           xml_present=xml_present,xml_filename=xml_filename,xml_filepath=xml_filepath)


@app.route('/data_selected',methods=['POST'])
def generate_columns():

    # 1. relational database
    selected_tables = request.form.getlist('selected_tables[]')
    relational_present = False
    table_columns = dict()
    databasename = ""
    if selected_tables:
        relational_present = True
        databasename = request.form.get('databasename')

        cassandra_query = f"select db_config from relationalmetadata where databasename='{databasename}' limit 1"
        result = session.execute(cassandra_query)[0]
        print(result)
        db_config = {
            "host": f"{result.db_config.host}",
            "user": f"{result.db_config.user}",
            "password": f"{result.db_config.password}",
            "database": f"{result.db_config.database}"
        }

        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()


        for table_name in selected_tables:
            try:
                query = f"SHOW COLUMNS FROM {table_name}"

                cursor.execute(query)

                # Fetch the column information
                columns = cursor.fetchall()

                table_columns[table_name] = [column[0] for column in columns]
            except mysql.connector.Error as err:
                print("Error:", err)


    # 2. excel database
    selected_sheets = request.form.getlist('selected_sheets[]')
    excel_filepath = request.form.get('excel_filepath')

    excel_present = False
    sheet_columns = dict()
    if selected_sheets:
        excel_present = True
        for sheet_name in selected_sheets:
            df = pd.read_excel(excel_filepath, sheet_name=sheet_name)

            # Get the column names as a list
            column_names = df.columns.tolist()

            sheet_columns[sheet_name] = column_names

    # 3. json database
    json_filepath = request.form.get('json_filepath')
    json_filename = request.form.get('json_filename')
    JSONfilename_attr_names = dict()
    json_present = False
    if json_filepath:

        json_present = True
        # Open and load the JSON file
        with open(json_filepath, 'r') as file:
            data = json.load(file)

        # Access the first object (usually the first item in a JSON array)
        first_object = data[0]

        JSONfilename_attr_names[json_filename] = JSONflatten_dict(first_object).keys()


    # 4. xml database
    xml_filepath = request.form.get('xml_filepath')
    xml_filename = request.form.get('xml_filename')
    xml_present = False
    XMLfilename_attr_names = dict()
    if xml_filepath:
            xml_present = True

            # Parse the XML file
            with open(xml_filepath, "r") as xml_file:
                xml_content = xml_file.read()

            # Step 2: Parse the XML content into an XML tree
            root = ET.fromstring(xml_content)

            XMLfilename_attr_names[xml_filename] = [attr_name[1:] for attr_name in flatten_xml(root).keys()]
            # print(XMLfilename_attr_names)
            # print(XMLfilename_attr_names[xml_filename], type(XMLfilename_attr_names[xml_filename]))


    final_tables_data = dict()

    if relational_present:
        print(table_columns)
        for key,value in table_columns.items():
            final_tables_data[key + ":relational"] = value

    if excel_present:
        print(sheet_columns)
        for key,value in sheet_columns.items():
            final_tables_data[key + ":excel"] = value
    # if json_present:
    #     print(JSONfilename_attr_names)
    #     final_tables_data[json_filename + ":JSON"] = JSONfilename_attr_names

    if xml_present:
        print(XMLfilename_attr_names)
        final_tables_data[xml_filename + ":XML"] = XMLfilename_attr_names[xml_filename]

    return render_template('multiple_ds_select_join_data.html', data=final_tables_data,databasename=databasename)

@app.route('/get_join_data', methods=['POST'])
def Data_joiner():
    db_name = request.form.get('databasename')
    table_column = json.loads(request.form.get('displayColumnsData'))
    table_join_conditions = json.loads(request.form.get('joinConditionsData'))
    table_joining_keys = json.loads(request.form.get('joinedColumnsData'))

    print("inside /get_join_data : \n")
    print(db_name)
    print("displayColumnsdata : ", table_column)
    print("joinConditionsData : ", table_joining_keys)
    print("joinedColumnsData : ", table_join_conditions)



    return "joining data got successfully!"

@app.route('/check_database', methods=['POST'])
def check_db_existence():
    partition_key_name = request.form.get('db_name')
    query = "select databasename from relationalmetadata group by databasename"
    result = session.execute(query)
    for row in result:
        if row.databasename == partition_key_name:
            query_index = menuMaker(partition_key_name)
            return render_template('options.html', databasename=partition_key_name)
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

    # for table_name, primary_key in tableName_primaryKey.items():
    #     print(type(table_name),type(primary_key))
    #     print(table_name,primary_key)


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

        print("tables in cassandra, inside /options route : add new tables : ", tables_in_cassandra)
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
            if table_name not in tables_in_cassandra:
                tables_not_present.append(table_name)

        if len(tables_not_present) == 0:
            return f"All tables already selected in {database_name}"

        query_index = dict()
        for table_name in tables_not_present:
            try:
                # Construct the query to retrieve column information
                query = f"SHOW COLUMNS FROM {table_name}"

                # Execute the query
                cursor.execute(query)

                # Fetch the column information
                columns = cursor.fetchall()

                # this goes up for being displayed in the front-end
                column_names = [column[0] for column in columns]

                query_index[table_name] = column_names

            except mysql.connector.Error as err:
                print("Error:", err)

        # Close the cursor and connection
        cursor.close()
        connection.close()

        return render_template('add_tables.html',db_name=database_name, data=query_index)
    elif selected_option == 'generate_report':
        # Handle generating report

        # 1. using database_name get table names and column names  : make query_index
        cassandra_query = f"select tablename from relationalmetadata where databasename='{database_name}'"

        result = session.execute(cassandra_query)
        tables = []
        for element in result:
            tables.append(element.tablename)
        return render_template('Select_tables_for_report.html', databasename=database_name,tables=tables)
    elif selected_option == 'view_generated_report':
        #1. fetch generated report data using db name
        cassandra_query = f"select report_name from generated_report_data where databasename='{database_name}'"
        result = session.execute(cassandra_query)
        database_reports = dict()
        reports = []
        for element in result:
            reports.append(element.report_name)
        database_reports[database_name] = reports

        return render_template('prev_generated_report.html',data=database_reports)

@app.route('/refresh_route', methods=['POST'])
def correct_Cassandra():
    database_name = request.form.get('databasename')
    cassandra_query = f"select db_config from relationalmetadata where databasename='{database_name}' limit 1"
    result = session.execute(cassandra_query)[0]
    # print(result)
    db_config = {
        "host": f"{result.db_config.host}",
        "user": f"{result.db_config.user}",
        "password": f"{result.db_config.password}",
        "database": f"{result.db_config.database}"
    }
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    # All tables from mysql server
    query = "SHOW TABLES"

    # Execute the query
    cursor.execute(query)

    # Fetch the table names
    tables = cursor.fetchall()

    tables_from_db = dict()
    # make a hash-map for comparing
    for table in tables:
        tables_from_db[table[0]] = True

    cassandra_query_for_tables = f"select tablename from relationalmetadata where databasename='{database_name}'"

    table_result = session.execute(cassandra_query_for_tables)
    # checking which
    tables_not_present_in_cassandra = []
    correct_tables = []
    for element in table_result:
        if element.tablename not in tables_from_db:
            tables_not_present_in_cassandra.append(element.tablename)
        else:
            correct_tables.append(element.tablename)

    for table_name in tables_not_present_in_cassandra:
        query = f"delete from relationalmetadata where databasename='{database_name}' and tablename='{table_name}'"
        session.execute(query)

    return render_template('Select_tables_for_report.html',databasename=database_name,tables=correct_tables)

@app.route('/view_gen_report', methods=['POST'])
def View_Report():
    report_name = request.form.get('report_name')
    database_name = request.form.get('database_name')

    cassandra_query = f"select query_for_report,columns from generated_report_data where databasename='{database_name}' and report_name='{report_name}'"
    result = session.execute(cassandra_query)[0]

    query = result.query_for_report
    # print(result.query_for_report)
    # print(result.columns, type(result.columns))


    res = session.execute(f"select db_config from relationalmetadata where databasename='{database_name}' limit 1")[0]

    db_config = {
        "host": f"{res.db_config.host}",
        "user": f"{res.db_config.user}",
        "password": f"{res.db_config.password}",
        "database": f"{res.db_config.database}"
    }

    fetched_data = []
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        # Execute the query
        start_time = time.time()
        cursor.execute(query)
        end_time = time.time()

        print("time taken for sql db query ", end_time - start_time)
        # Fetch all the rows
        fetched_data = cursor.fetchall()
        # print("in the data /view_gen_report route : ", fetched_data)
        # print the rows or do whatever you want wit)h the rows
    except mysql.connector.Error as err:
        print("Error:", err)

    cursor.close()
    connection.close()

    # return "got the query for report"
    return render_template('fetched_data.html', columns=result.columns, rows=fetched_data, databasename=database_name,report_not_present=False)

@app.route('/selected_tables', methods=['POST'])
def collect_tables():
    selected_tables = json.loads(request.form.get('selected_tables'))
    databasename = request.form.get('dataBaseName')


    query_index = OrderedDict()

    for table_name in selected_tables:
        cassandra_query = f"select datafields from relationalmetadata where databasename='{databasename}' and tablename='{table_name}'"
        result = session.execute(cassandra_query)[0]
        query_index[table_name] = result.datafields

    print(selected_tables)
    print(query_index)
    # return f"data retrieved successfullly from {databasename}"
    return render_template('collect_column_jk.html', data=query_index,databasename=databasename)

@app.route('/addTableHandler',methods=['POST'])
def SaveAddedtableMetaData():
    tables_primary_keys = json.loads(request.form.get('selected_primary_keys'))
    db_name = request.form.get('db_name')

    # print("Tables against primary keys : ",tables_primary_keys)
    # print("Database name : ",db_name)

    # 1. taking configration information for existing tables of same database  : db_name
    cassandra_query = f"select db_config from relationalmetadata where databasename='{db_name}' limit 1"
    result = session.execute(cassandra_query)[0]
    print(result)
    db_config = {
        "host": f"{result.db_config.host}",
        "user": f"{result.db_config.user}",
        "password": f"{result.db_config.password}",
        "database": f"{result.db_config.database}"
    }
    db_config_string = f"host: '{result.db_config.host}',user : '{result.db_config.user}',password: '{result.db_config.password}', database : '{result.db_config.database}'"
    # 2. making connection using the same
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    # 3. taking out the column names for "Adding tables to the POC" : inserting data to cassandra
    for table_name,primary_key in tables_primary_keys.items():
        try:
            # Construct the query to retrieve column information
            query = f"SHOW COLUMNS FROM {table_name}"

            # Execute the query
            cursor.execute(query)

            # Fetch the column information
            columns = cursor.fetchall()

            # this goes up for being displayed in the front-end
            column_names = [f"'{column[0]}'" for column in columns]
            column_names_string = ','.join(map(str, column_names))

            cassandra_query = f"insert into relationalmetadata(databasename,primary_key,tablename,db_config,datafields) values(" \
                              f"'{db_name}'," \
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

    return render_template('options.html', databasename=db_name)

@app.route('/process_selection', methods=['POST'])
def display_data_view():
    db_name = request.form.get('dataBaseName')
    table_column = json.loads(request.form.get('displayColumnsData'))
    table_join_conditions = json.loads(request.form.get('joinConditionsData'))
    table_joining_keys = json.loads(request.form.get('joinedColumnsData'))
    column_info = []
    print("inside /process_selection : \n")
    print("displayColumnsdata : ", table_column)
    print("joinConditionsData : ",table_joining_keys)
    print("joinedColumnsData : ",table_join_conditions)

    for key,value in table_column.items():
        column_info = column_info + value

    print(column_info)


    # return f"successfully recieved data for generating report {databasename}"
    # print(columns_info)
    fetched_data,query,columns_to_save = DataRetriver(db_name,table_column,table_join_conditions,table_joining_keys)
    # print(fetched_data)
    # redis_sql(fetched_data)
    # return f"Data retrived successfully from the {databasename} db!"

    return render_template('fetched_data.html',columns=column_info,rows=fetched_data,databasename=db_name,query_generated=query,columns_to_save=columns_to_save,report_not_present=True)

@app.route('/save_report',methods=['POST'])
def save_report():
    report_name = request.form.get('report_name')
    databasename = request.form.get('dataBaseName')
    query = request.form.get('query_generated')
    columns_to_save = request.form.get('columns_to_save')
    # print("data for generated report primary key  : ", request.form['selected_pk_report'])
    pk_report =  request.form['selected_pk_report']
    cassandra_query = f"insert into generated_report_data(databasename,report_name,query_for_report,columns,pk_report) values('{databasename}','{report_name}','{query}',[{columns_to_save}],'{pk_report}')"

    session.execute(cassandra_query)

    return render_template('options.html',databasename=databasename)



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
    # print(selected_columns_from_tables) #debugging statements
    return selected_columns_from_tables

def afterON(table1,table2,join1,join2):
    query = f"{table1}.{join1[0]} = {table2}.{join2[0]}"
    if len(join1) == 1:
        return query
    for i in range(1,len(join1)):
        query += f" and {table1}.{join1[i]} = {table2}.{join2[i]}"
    return query

def DataRetriver(database,query_index,table_joining_conditions,joining_keys):
    rows = []
    cassandra_query = f"select db_config from relationalmetadata where databasename='{database}'"
    result = session.execute(cassandra_query)[0]
    db_config = {
        "host": f"{result.db_config.host}",
        "user": f"{result.db_config.user}",
        "password": f"{result.db_config.password}",
        "database": f"{result.db_config.database}"
    }

    columns_info = ""
    colunms_to_save = []
    for i, (key,value) in enumerate(query_index.items()):
        table_name = key
        for index,element in enumerate(value):
            colunms_to_save.append(f"'{element}'")
            columns_info += f"{table_name}.{element}"
            if index == len(value) - 1:
                continue
            columns_info += ","
        if i == len(query_index.items()) - 1:
            continue
        columns_info += ","
    colunms_to_save_string = ",".join(colunms_to_save)
    table_names = []
    for (key,value) in query_index.items():
        table_names.append(key)

    query = f"select  {columns_info} from {table_names[0]}"


    no_of_tables = len(table_names)
    for i in range(1,no_of_tables):
        table1 = table_names[i-1]
        table2 = table_joining_conditions[table_names[i-1]]['join_table']
        join_conditions = afterON(table1,table2,joining_keys[table1],table_joining_conditions[table1]['join_columns'])
        query += f" inner join {table_names[i]} on {join_conditions}"

    print(query)

    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        # Execute the query
        start_time = time.time()
        cursor.execute(query)
        end_time = time.time()

        print("time taken for sql db query ", end_time - start_time)
        # Fetch all the rows
        rows = cursor.fetchall()
        # print("inside data retriever function : ", rows)
        # print the rows or do whatever you want wit)h the rows
    except mysql.connector.Error as err:
        print("Error:", err)

    cursor.close()
    connection.close()
    return rows,query,colunms_to_save_string



if __name__ == '__main__':
    if __name__ == "__main__":
        app.run(debug=True, port=8000)
