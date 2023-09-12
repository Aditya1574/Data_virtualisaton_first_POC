import mysql.connector
from cassandra.cluster import Cluster
import json
import pandas as pd
import xml.etree.ElementTree as ET
import time
from flask import Flask, render_template,request
from collections import OrderedDict
import redis

cluster = Cluster()
session = cluster.connect('samplemeta')

app = Flask(__name__)

def is_integer(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


def get_filename(file_path):
    start = file_path.rfind('\\')
    end = file_path.rfind('.')
    file_name = file_path[start + 1:end]
    return file_name

# function for JSON retrieving data

def flatten_json(json_obj,flattened_data, separator='_', parent_key=''):
    for key, value in json_obj.items():
        new_key = f"{parent_key}{separator}{key}" if parent_key else key
        if isinstance(value, dict):
            flattened_data.update(flatten_json(value,flattened_data, separator, new_key))
        elif isinstance(value, list):
            sub_dict = {}
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    sub_dict.update(flatten_json(item,flattened_data, separator, f"{new_key}{separator}{i}"))
                else:
                    sub_key = f"{new_key}{separator}{key}"
                    if sub_key not in sub_dict:
                        sub_dict[sub_key] = []
                    sub_dict[sub_key].append(item)
            flattened_data.update(sub_dict)
        else:
            if new_key not in flattened_data:
                flattened_data[new_key] = []
            flattened_data[new_key].append(value)
    return flattened_data

""""

# usage 

myjson_data = [
json docs
]
# Flatten the JSON data
flattened_data = {}
for json_data in myjson_data:
    flattened_data = flatten_json(json_data,flattened_data)

flattened_data contains the data flattened keys against list of values, index --> record

"""


# fucntion for column names

def JSONflatten_dict(d, parent_key='', sep='_'):
    items = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(JSONflatten_dict(v, new_key, sep=sep))
        elif isinstance(v, list):
            if v and isinstance(v[0], dict):
                for i, item in enumerate(v):
                    items.update(JSONflatten_dict(item, f"{new_key}_{i}", sep=sep))
            else:
                items[new_key] = v
        else:
            items[new_key] = v
    return items


"""
# Your first JSON record (replace with your data)
json_data = {
    "employee": {
        "id": 12345,
        "name": "John Doe",
        "phones": [{"type": "home", "number": "555-1234"}, {"type": "work", "number": "555-5678"}]
    }
}
# Flatten the JSON data
flattened_data = flatten_dict(json_data)

# Print the flattened data
for key, value in flattened_data.items():
    print(f"{key}: {value}")
"""

# function for extracting data from xml

def get_xml_data(element, flattened_data, parent_key='', separator='.'):
    for i, child_element in enumerate(element):
        child_key = parent_key + separator + child_element.tag
        if len(child_element) > 0:
            if len(child_element) == 2:
                child_key = child_key + f"[{i}]"
            child_data = get_xml_data(child_element, flattened_data, child_key, separator)
            if child_key in flattened_data:
                if not isinstance(flattened_data[child_key], list):
                    flattened_data[child_key] = [flattened_data[child_key]]
                flattened_data[child_key].append(child_data[child_key])
            else:
                flattened_data.update(child_data)
        else:
            child_key = child_key
            if child_key in flattened_data:
                if not isinstance(flattened_data[child_key], list):
                    flattened_data[child_key] = [flattened_data[child_key]]
                flattened_data[child_key].append(child_element.text)
            else:
                flattened_data[child_key] = [child_element.text]

    return flattened_data

"""

# usage

xml_data = # enclosed inside triple quotes also inside root element

# Parse the XML data
root = ET.fromstring(xml_data)

# Flatten the XML data
flattened_data = {}
flattened_data = get_xml_data(root, flattened_data)

# Print the flattened data
for key, value in flattened_data.items():
    print(f"{key}: {value}")
"""

# for extractiing the column names for xml data using the first record

def flatten_xml(element, flattened_keys, parent_key='', separator='.'):
    for i, child_element in enumerate(element):
        child_key = parent_key + separator + child_element.tag
        if len(child_element) > 0:
            if len(child_element) == 2:
                child_key = child_key + f".{i}"
            flatten_xml(child_element, flattened_keys, child_key, separator)
        else:
            child_key = child_key
            flattened_keys.append(child_key)
    return flattened_keys

"""

xml_data = # first xml record enclosed inside triple quotes

# Parse the XML data
root = ET.fromstring(xml_data)

# Flatten the XML data
flattened_data = []
flattened_data = flatten_xml(root, flattened_data)

# Print the flattened data
for element in flattened_data:
    print(element)
"""

def check_relationaldatabase(databasename):
    # partition_key_name = request.form.get('db_name')
    query = "select databasename from relationalmetadata group by databasename"
    result = session.execute(query)
    for row in result:
        if row.databasename == databasename:
            return True
    return False


def check_exceldatabase(filename):

    query = "select filename from excelmetadata group by filename"
    result = session.execute(query)
    for row in result:
        if row.filename == filename:
            return True
    return False


def check_jsondatabase(filename):
    query = "select filename from jsonmetadata group by filename"
    result = session.execute(query)
    for row in result:
        if row.filename == filename:
            return True
    return False

def check_xmldatabase(filename):
    query = "select filename from xmlmetadata group by filename"
    result = session.execute(query)
    for row in result:
        if row.filename == filename:
            return True
    return False


@app.route('/', methods=['GET'])
def hello_world():
    return render_template('index.html')


@app.route('/options_selected', methods=['POST'])
def options_selected():
    selected_options = request.form.getlist('options[]')
    # print(selected_options)
    return render_template('data_collection.html',data_sources=selected_options)

@app.route('/data_collected',methods=['POST'])
def collected_data_processing():

    relational_present = True
    xml_present = True
    json_present = True
    excel_present = True

    db_name = request.form.get('db_name')
    if db_name: # has come ?
        relational_present = check_relationaldatabase(db_name)
        if not relational_present: # if not present then dont sent the name
            db_name = ""
    xml_file = request.form.get('xml_file')
    if xml_file:
        xml_present = check_xmldatabase(xml_file)
        if not xml_present:
            xml_file = ""
    json_file = request.form.get('json_file')
    if json_file:
        json_present = check_jsondatabase(json_file)
        if not json_present:
            json_file = ""
    excel_file = request.form.get('excel_file')
    if excel_file:
        excel_present = check_exceldatabase(excel_file)
        if not excel_present:
            excel_file = ""


    # just Debegging statements
    # print(db_name, " : ", relational_present)
    # print(xml_file, " : ", xml_present)
    # print(json_file, " : ", json_present)
    # print(excel_file, " : ", excel_present)

    # True : present in cassandra or not selected for report generation

    if relational_present and xml_present and json_present and excel_present:
        final_tables = dict()
        excel_filepath = ""
        xml_filepath = ""
        json_filepath = ""
        if db_name: # checking if the name was entered
            cassandra_query = f"select tablename,datafields from relationalmetadata where databasename='{db_name}'"
            result = session.execute(cassandra_query)
            for element in result:
                final_tables[element.tablename + ":relational" ] = element.datafields

        if excel_file:
            cassandra_query = f"select sheetname,datafields from excelmetadata where filename='{excel_file}'"
            result = session.execute(cassandra_query)
            for element in result:
                final_tables[element.sheetname + ":excel"] = element.datafields
            cassandra_query = f"select filepath from excelmetadata where filename='{excel_file}' limit 1"
            result = session.execute(cassandra_query)[0]
            excel_filepath = result.filepath

        if json_file:
            cassandra_query = f"select datafields from jsonmetadata where filename='{json_file}'"
            result = session.execute(cassandra_query)[0]
            final_tables[json_file + ":JSON"] = result.datafields
            cassandra_query = f"select filepath from jsonmetadata where filename='{json_file}' limit 1"
            result = session.execute(cassandra_query)[0]
            json_filepath = result.filepath

        if xml_file:
            cassandra_query = f"select datafield from xmlmetadata where filename='{xml_file}'"
            result = session.execute(cassandra_query)[0]
            final_tables[xml_file + ":XML"] = result.datafield
            cassandra_query = f"select filepath from xmlmetadata where filename='{xml_file}' limit 1"
            result = session.execute(cassandra_query)[0]
            xml_filepath = result.filepath

        return render_template('view_selection.html',tables=final_tables,databasename=db_name,
                               excel_filepath=excel_filepath,json_filepath=json_filepath,xml_filepath=xml_filepath)

    return render_template('data_entry.html',databasename=db_name,xml_filename=xml_file,excel_filename=excel_file,
                           json_filename=json_file,relational_present=relational_present,xml_present=xml_present,
                           json_present=json_present,excel_present=excel_present)

@app.route('/final_data_collected', methods=['POST'])
def send_to_tableSelection():
    # databasename for relational database :it must come only for those who exit in cassandra
    databasename = request.form.get('databasename')
    # file-names : they must come only for those who exit in cassandra
    excel_filename = request.form.get('excel_filename')
    xml_filename = request.form.get('xml_filename')
    json_filename = request.form.get('json_filename')
    # file-paths
    json_filepath = ""
    xml_filepath = ""
    excel_filepath = ""
    # RELATIONAL
    relational_present = request.form.get('relational-data')
    print("relational_present : ", relational_present, " ", type(relational_present))
    print("database name : ", databasename," ", type(databasename))
    relational_query_index = dict()
    # database requirements
    host=""
    user=""
    password=""
    # EXCEL
    excel_present = request.form.get('excel-data')
    print("excel_present : ", excel_present, " ", type(excel_present))
    print("excel_filename : ", excel_filename, " ", type(excel_filename))
    excel_query_index = dict()
    # JSON
    json_present = request.form.get('json-data')
    print("json_present : ", json_present, " ", type(json_present))
    print("json_filename : ", json_filename, " ", type(json_filename))
    json_columns = []
    #   XML
    xml_present = request.form.get('xml-data')
    print("xml_present : ", xml_present, " ", type(xml_present))
    print("xml_filename : ", xml_filename, " ", type(xml_filename))
    xml_columns = []

    if relational_present is not None: # if the data for this was entered >> was chosen but not in meta
        relational_data = json.loads(request.form.get('relational-data'))
        db_config={
            "host": relational_data['hostname'],
            "user": relational_data['user'],
            "password": relational_data['password'],
            "database": relational_data['database']
        }
        host=relational_data['hostname']
        user=relational_data['user']
        password=relational_data['password']
        databasename = relational_data['database']

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
                query = f"SHOW COLUMNS FROM {table_name}"

                cursor.execute(query)

                # Fetch the column information
                columns = cursor.fetchall()

                relational_query_index[table_name] = [column[0] for column in columns]
            except mysql.connector.Error as err:
                print("Error:", err)
    elif databasename != "None": #
        cassandra_query = f"select db_config from relationalmetadata where databasename='{databasename}' limit 1;"
        result = session.execute(cassandra_query)[0]
        host = result.db_config.host
        user = result.db_config.user
        password = result.db_config.password
        databasename = result.db_config.database

    if excel_present is not None:
        # Replace 'your_file.xlsx' with the path to your Excel file
        excel_data = json.loads(request.form.get('excel-data'))
        excel_filepath = excel_data['excel_filepath']
        excel_filename = get_filename(excel_filepath)
        # Using ExcelFile object
        xls = pd.ExcelFile(excel_filepath)
        sheet_names = xls.sheet_names
        # Using pd.read_excel
        sheet_names = pd.ExcelFile(excel_filepath).sheet_names
        for sheet_name in sheet_names:
            excel_query_index[sheet_name] = GetColumnNamesFromSheetName(excel_filepath,sheet_name)
    elif excel_filename != "None":
        # required for getting the path of the corrosponding file which is existing in cassandra and send it from here on!
        cassandra_query = f"select filepath from excelmetadata where filename='{excel_filename}' limit 1"
        result = session.execute(cassandra_query)[0]
        excel_filepath = result.filepath


    if xml_present is not None:
        xml_data = json.loads(request.form.get('xml-data'))
        xml_filepath = xml_data['xml_filepath']
        xml_filename = get_filename(xml_filepath)
        xml_columns = GetXMLFlattenAttrNames(xml_filepath)
    elif xml_filename != "None":
        cassandra_query = f"select filepath from xmlmetadata where filename='{xml_filename}' limit 1"
        result = session.execute(cassandra_query)[0]
        xml_filepath = result.filepath


    if json_present is not None:
        json_data = json.loads(request.form.get('json-data'))
        json_filepath = json_data['json_filepath']
        json_filename = get_filename(json_filepath)
        json_columns = [key for key in GetJSONFlattenAttrNames(json_filepath)]
    elif json_filename != "None":
        cassandra_query = f"select filepath from jsonmetadata where filename='{json_filename}' limit 1"
        result = session.execute(cassandra_query)[0]
        json_filepath = result.filepath



    return render_template('add_tables_sheets.html',
                           table_data=relational_query_index,excel_data=excel_query_index,
                           json_columns=json_columns,xml_columns=xml_columns,
                           excel_filename=excel_filename,json_filename=json_filename,xml_filename=xml_filename,
                           excel_filepath=excel_filepath,xml_filepath=xml_filepath,json_filepath=json_filepath,
                           db_name=databasename,host=host,user=user,password=password)



def GetColumnNames(databasename,table_name):
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
    column_names = []
    try:
        query = f"SHOW COLUMNS FROM {table_name}"

        cursor.execute(query)

        # Fetch the column information
        columns = cursor.fetchall()

        column_names = [column[0] for column in columns]
    except mysql.connector.Error as err:
        print("Error:", err)

    return column_names

def GetColumnNamesFromSheetName(excel_filepath,sheet_name):
    df = pd.read_excel(excel_filepath, sheet_name=sheet_name)

    # Get the column names as a list
    column_names = df.columns.tolist()

    return column_names

def GetXMLFlattenAttrNames(xml_filepath):
    tree = ET.parse(xml_filepath)
    root = tree.getroot()

    # Find the first child element of the root
    first_child = list(root)[0]

    # Get the name of the first child element
    first_child_name = first_child.tag

    first_record = root.find(first_child_name)  # Change 'record' to the actual tag name

    # Create a new XML element tree containing only the root and the first record
    new_root = ET.Element(root.tag)
    new_root.append(first_record)

    # Create a new XML tree with the new root
    new_tree = ET.ElementTree(new_root)

    # Serialize the new tree to a string
    first_record_xml = ET.tostring(new_tree.getroot(), encoding='utf-8').decode('utf-8')

    first_element = ET.fromstring(first_record_xml)
    flattened_data = []
    return flatten_xml(first_element,flattened_data)

def GetJSONFlattenAttrNames(json_filepath):

    with open(json_filepath, 'r') as file:
        data = json.load(file)

    # Access the first object (usually the first item in a JSON array)
    first_object = data[0]

    return JSONflatten_dict(first_object).keys()



@app.route('/data_selected',methods=['POST'])
def generate_columns():
    final_tables_data = dict()
    excel_filepath = request.form.get('excel_filepath')
    json_filepath = request.form.get('json_filepath')
    xml_filepath = request.form.get('xml_filepath')
    databasename = request.form.get('databasename')
    selected_data = json.loads(request.form.get('selected_tables'))

    # Looping through the selected_data thing getting table,sheets,json and xml files in order
    for element_name in selected_data:
        start = element_name.rfind(':')
        end = element_name.find(':')
        db_type = element_name[start + 1:]
        entity_name = element_name[:end]
        # 1. relational database
        if db_type == 'relational':
            final_tables_data[element_name] = GetColumnNames(databasename,entity_name)
        # 2. excel database
        elif db_type == 'excel':
            final_tables_data[element_name] = GetColumnNamesFromSheetName(excel_filepath,entity_name)
        # 3. json database
        elif db_type == 'JSON':
            final_tables_data[element_name] = [key for key in GetJSONFlattenAttrNames(json_filepath)]
            # doing this as what is returned from the function  is dict_key object
        # 4. xml database
        else:
            final_tables_data[element_name] = GetXMLFlattenAttrNames(xml_filepath)

    print(final_tables_data)
    return render_template('multiple_ds_select_join_data.html', data=final_tables_data,databasename=databasename,
                           excel_filepath=excel_filepath,json_filepath=json_filepath,xml_filepath=xml_filepath)


def GetRelationalDataAsDataFrame(databasename, table_name, column_names):
    cassandra_query = f"select db_config from relationalmetadata where databasename='{databasename}' limit 1"
    result = session.execute(cassandra_query)[0]
    # print(result)
    db_config = {
        "host": f"{result.db_config.host}",
        "user": f"{result.db_config.user}",
        "password": f"{result.db_config.password}",
        "database": f"{result.db_config.database}"
    }
    # 2. connect to mysql
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    # 3. make a query to mysql server and fetch the data
    columns_for_query = ",".join(column_names)
    query = f"select {columns_for_query} from {table_name}"
    cursor.execute(query)
    rows = cursor.fetchall()
    # 4. convert the data into polars dataframe
    relational_df = pd.DataFrame(rows,columns=column_names )

    # print("Relational Dataframe \n", relational_df)

    return relational_df

def GetJSONDataAsDataFrame(json_filepath,attr_names):
    # reading json data and gettig it as a dictionary
    myjson_data = []
    try:
        with open(json_filepath, 'r') as json_file:
            myjson_data = json_file.read()
        # Now, json_data contains the entire JSON content as a string
        # print(myjson_data)
        myjson_data = json.loads(myjson_data)
    except FileNotFoundError:
        print(f"JSON File not found: {json_filepath}")

    json_flattened_data = {}
    for json_data in myjson_data:
        json_flattened_data = flatten_json(json_data, json_flattened_data)

    # DF_JSON
    json_df = pd.DataFrame(json_flattened_data)
    # print("JSON Dataframe \n", json_df)

    return json_df

def GetXMLDataAsDataFrame(xml_filepath,attr_names):
    xml_data = ""
    try:
        with open(xml_filepath, 'r') as xml_file:
            xml_data = xml_file.read()
            # Now, xml_data contains the entire XML document as a string
            # print(xml_data)
    except FileNotFoundError:
        print(f"XML File not found: {xml_filepath}")

    xml_flattened_data = {}
    root = ET.fromstring(xml_data)

    # Flatten  XML data

    xml_flattened_data = get_xml_data(root, xml_flattened_data)

    # print(xml_flattened_data)

    # DF_XML
    xml_df = pd.DataFrame(xml_flattened_data)

    return xml_df

def GetExcelDataAsDataFrame(excel_filepath,sheet_name,colunms):
    try:
        # Read the Excel file into a Pandas DataFrame
        df = pd.read_excel(excel_filepath, sheet_name=sheet_name)

        # Select only the required columns from the DataFrame
        df = df[colunms]

        return df
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None

def BifervateElement(element):
    start = element.rfind(':')
    end = element.find(':')

    return element[:end],element[start + 1:]


@app.route('/get_join_data', methods=['POST'])
def DataJoiner():
    databasename = request.form.get('databasename')
    excel_filepath = request.form.get('excel_filepath')
    json_filepath = request.form.get('json_filepath')
    xml_filepath = request.form.get('xml_filepath')

    table_column = json.loads(request.form.get('displayColumnsData'))
    table_join_conditions = json.loads(request.form.get('joinConditionsData'))
    table_joining_keys = json.loads(request.form.get('joinedColumnsData'))

    print(table_column)
    print(table_joining_keys)
    print(table_join_conditions)


    print("inside /get_join_data : \n")

    final_df = pd.DataFrame()

    element, values = next(iter(table_column.items()))
    element_name, element_type = BifervateElement(element)
    columns_required = values
    print(element_name, " : ",element_type )
    # joining columns for the current column
    joining_keys_for_element = table_joining_keys[element]
    # getting all the required columns : (columns requested by the user UNION columns required for joining)
    values = list(set(values).union(set(joining_keys_for_element)))

    if element_type == 'relational':
        final_df = GetRelationalDataAsDataFrame(databasename,element_name,values)
    elif element_type == 'excel':
        final_df = GetExcelDataAsDataFrame(excel_filepath,element_name,values)
    elif element_type == 'JSON':
        final_df = GetJSONDataAsDataFrame(json_filepath,values)
    elif element_type == 'XML':
        final_df = GetXMLDataAsDataFrame(xml_filepath,values)

    for i,(table_name,columns) in enumerate(table_column.items()):

        if i == len(table_column)-1:
            continue

        joining_to = table_join_conditions[table_name]['join_table']

        element_name,element_type = BifervateElement(joining_to)

        values = list(set(table_column[joining_to]).union(table_join_conditions[table_name]['join_columns']))
        values = list(set(values).union(table_joining_keys[joining_to]))
        joining_df = pd.DataFrame()

        print(f"joining {table_name} to {joining_to} with {values}")
        print(f"columns of final_df  : {i} ", final_df.columns)

        joining_from_columns_test = final_df.columns

        if element_type == 'relational':
            joining_df = GetRelationalDataAsDataFrame(databasename, element_name, values)
        elif element_type == 'excel':
            joining_df = GetExcelDataAsDataFrame(excel_filepath, element_name, values)
        elif element_type == 'JSON':
            joining_df = GetJSONDataAsDataFrame(json_filepath, values)
        elif element_type == 'XML':
            joining_df = GetXMLDataAsDataFrame(xml_filepath, values)

        # print(joining_df)
        joining_from_columns = table_joining_keys[table_name]
        joining_to_columns = table_join_conditions[table_name]['join_columns']

        print("Dislaying data types of from : \n")
        for col in joining_from_columns:
            print(col, " : " , final_df[col].dtype)

        print("Dislaying data types of to : \n")
        for col in joining_to_columns:
            print(col, " : ", joining_df[col].dtype)


        print(table_name , " left : ", joining_from_columns, f" {len(joining_from_columns)}")
        print(table_join_conditions[table_name]['join_table']," right : ",joining_to_columns,f" {len(joining_to_columns)}")


        for index in range(len(joining_to_columns)):
            joining_df[joining_to_columns[index]] = joining_df[joining_to_columns[index]].astype(final_df[joining_from_columns[index]].dtype)

        # logic for preventing any wrong comparisons due to renaming using _delme

        # values list elements elements occuring in from df will get renamed hence un-renamed already existing may get
        # compared in the next join

        common_from_to = list(set(values) & set(joining_from_columns_test))

        # hence renaming in the table_joining_keys for ensuring correct join next time too

        for common_item in common_from_to:
            if common_item in table_joining_keys[joining_to] and common_item not in table_join_conditions[table_name]['join_columns']:
                i = table_joining_keys[joining_to].index(common_item)
                table_joining_keys[joining_to][i] = common_item + '_delme'
            if common_item in table_column[joining_to] and common_item not in table_join_conditions[table_name]['join_columns']:
                i = table_column[joining_to].index(common_item)
                table_column[joining_to][i] = common_item + '_delme'


        print("changed in joinnig_to whihch will be next joining from  : ", table_joining_keys[joining_to])
        final_df = final_df.merge(
            joining_df,
            left_on=joining_from_columns,
            right_on=joining_to_columns,
            how='inner',
            suffixes=('', '_delme')
        )
        columns_required = columns_required + table_column[joining_to]


    print("Complete data Frame : \n",final_df)
    print("Trimmed data Frame : \n",final_df[columns_required])
    # final_df[columns_required].to_csv("C:\\Users\\Dell\\Desktop\\generated_test_data\\data.csv", header=True, index=False)

    # final_df[columns_required] :  all the data
    # columns_required : required_columns

    return render_template('combined_result.html',dataframe=final_df[columns_required],columns=columns_required)

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

        # print("tables in cassandra, inside /options route : add new tables : ", tables_in_cassandra)
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
    return render_template('fetched_data.html', columns=result.columns, rows=fetched_data,
                           databasename=database_name,report_not_present=False)

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
    # SAVING METADATA FOR RELATIONAL DATABASE
    db_name = request.form.get('db_name')
    excel_filepath = request.form.get('excel_filepath')
    json_filepath = request.form.get('json_filepath')
    xml_filepath = request.form.get('xml_filepath')
    final_tables = dict()
    # print("Relational : ", request.form.get('selected_primary_keys_forTables'), bool(json.loads(request.form.get('selected_primary_keys_forTables'))))
    print("databasename : " , db_name, " ", type(db_name))

    if request.form.get('selected_primary_keys_forTables') != "{}" and request.form.get('selected_primary_keys_forTables') != "":
        tables_primary_keys = json.loads(request.form.get('selected_primary_keys_forTables'))
        db_name = request.form.get('db_name')
        db_config = {
            "host" : request.form.get('host'),
            "user" : request.form.get('user'),
            "password" : request.form.get('password'),
            "database" : request.form.get('db_name')
        }
        db_config_string = f"host: '{request.form.get('host')}'," \
                           f"user : '{request.form.get('user')}'," \
                           f"password: '{request.form.get('password')}', " \
                           f"database : '{request.form.get('db_name')}'"
        # 3. taking out the column names for "Adding tables to the POC" : inserting data to cassandra
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        for table_name,primary_key in tables_primary_keys.items():
            try:
                query = f"SHOW COLUMNS FROM {table_name}"
                cursor.execute(query)

                # Fetch the column information
                columns = cursor.fetchall()
                # this goes up for being displayed in the front-end
                final_tables[table_name + ":relational"] = columns
                column_names = [f"'{column[0]}'" for column in columns]
                column_names_string = ','.join(map(str, column_names))

                cassandra_query = f"insert into relationalmetadata(databasename,primary_key,tablename," \
                                  f"db_config,datafields) values(" \
                                  f"'{db_name}'," \
                                  f"'{primary_key}'," \
                                  f"'{table_name}'," \
                            "{" + f"{db_config_string}" + "}," \
                                  f"[{column_names_string}]" \
                                  f");"
                print(cassandra_query, "\n", "Saved Table metadata to cassandra using the abobe query \n")
                session.execute(cassandra_query)
            except mysql.connector.Error as err:
                print("Error:", err)
    elif db_name != "None"and db_name != "": # means this exists in the meta data repo
        cassandra_query = f"select tablename,datafields from relationalmetadata where databasename='{db_name}'"
        result = session.execute(cassandra_query)
        for element in result:
            final_tables[element.tablename + ":relational"] = element.datafields

    print("Excel : ", request.form.get('selected_primary_keys_forSheets'))
    print("excel_filepath : " , excel_filepath, " ", type(excel_filepath))

    if request.form.get('selected_primary_keys_forSheets') != "{}" and request.form.get('selected_primary_keys_forSheets') != "":
        sheet_primary_keys = json.loads(request.form.get('selected_primary_keys_forSheets'))
        excel_filepath = request.form.get('excel_filepath')
        excel_filename = get_filename(excel_filepath)
        for sheet_name,primary_key in sheet_primary_keys.items():
            columns = GetColumnNamesFromSheetName(excel_filepath,sheet_name)
            final_tables[sheet_name + ":excel"] = columns
            column_names = [f"'{column}'" for column in columns]
            column_names_string = ','.join(map(str,column_names))

            cassandra_query = f"insert into excelmetadata(filename,sheetname,datafields,filepath,primary_key)" \
                              f"values('{excel_filename}'," \
                              f"'{sheet_name}'," \
                              f"[{column_names_string}]," \
                              f"'{excel_filepath}'," \
                              f"'{primary_key}')"
            session.execute(cassandra_query)
            print(cassandra_query, "\n", "Saved excel metadata to cassandra using the abobe query \n")
    elif excel_filepath != "None" and excel_filepath != "":
        excel_file = get_filename(excel_filepath)
        cassandra_query = f"select sheetname,datafields from excelmetadata where filename='{excel_file}'"
        result = session.execute(cassandra_query)
        for element in result:
            final_tables[element.sheetname + ":excel"] = element.datafields

    print("JSON : ", request.form.get('selected_primary_keys_forJSON'))
    print("json_filepath : " , json_filepath, " ", type(json_filepath))

    if request.form.get('selected_primary_keys_forJSON') != "{}" and request.form.get('selected_primary_keys_forJSON') != "":
        json_filepath = request.form.get('json_filepath')
        filename_pk = json.loads(request.form.get('selected_primary_keys_forJSON'))
        filename, primary_key = next(iter(filename_pk.items()))
        flattned_columns = [key for key in GetJSONFlattenAttrNames(json_filepath)] # little hack for getting correct data [type]
        final_tables[filename + ":JSON"] = flattned_columns
        flattned_columns_string = ','.join(map(str,[f"'{col}'" for col in flattned_columns]))
        cassandra_query = f"insert into jsonmetadata(filename,filepath,datafields,primary_key) values(" \
                          f"'{filename}'," \
                          f"'{json_filepath}'," \
                          f"[{flattned_columns_string}]," \
                          f"'{primary_key}'" \
                          f")"
        session.execute(cassandra_query)
        print(cassandra_query, "\n", "Saved JSON metadata to cassandra using the abobe query \n")
    elif json_filepath != "None"and json_filepath != "":
        json_file = get_filename(json_filepath)
        cassandra_query = f"select datafields from jsonmetadata where filename='{json_file}'"
        result = session.execute(cassandra_query)[0]
        final_tables[json_file + ":JSON"] = result.datafields

    print("XML : ", request.form.get('selected_primary_keys_forXML'))
    print("xml_filepath : " , xml_filepath, " ", type(xml_filepath))

    if request.form.get('selected_primary_keys_forXML') != "{}" and request.form.get('selected_primary_keys_forXML') != "":
        xml_filepath = request.form.get('xml_filepath')
        filename_pk = json.loads(request.form.get('selected_primary_keys_forXML'))
        filename, primary_key = next(iter(filename_pk.items()))
        flattened_columns = GetXMLFlattenAttrNames(xml_filepath)
        final_tables[filename + ":XML"] = flattened_columns
        flattned_columns_string = ','.join(map(str,[f"'{col}'" for col in flattened_columns]))
        cassandra_query = f"insert into xmlmetadata(filename,filepath,datafield,primary_key) values(" \
                          f"'{filename}'," \
                          f"'{xml_filepath}'," \
                          f"[{flattned_columns_string}]," \
                          f"'{primary_key}'" \
                          f")"
        session.execute(cassandra_query)
        print(cassandra_query, "\n", "Saved XML metadata to cassandra using the abobe query \n")
    elif xml_filepath != "None" and xml_filepath != "":
        xml_file = get_filename(xml_filepath)
        cassandra_query = f"select datafield from xmlmetadata where filename='{xml_file}'"
        result = session.execute(cassandra_query)[0]
        final_tables[xml_file + ":XML"] = result.datafield
    # return "Saved data into meta data repo successfully!"

    return render_template('view_selection.html',tables=final_tables,databasename=db_name,json_filepath=json_filepath,
                           xml_filepath=xml_filepath,excel_filepath=excel_filepath)
    # return render_template('options.html', databasename=db_name)

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

    return render_template('fetched_data.html',columns=column_info,rows=fetched_data,databasename=db_name,
                           query_generated=query,columns_to_save=columns_to_save,report_not_present=True)

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
    cassandra_query = f"select db_config from relationalmetadata where databasename='{database}' limit 1"
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
