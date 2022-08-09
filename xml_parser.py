import sqlite3
import xml.etree.ElementTree as ET
mytree = ET.parse('feed-example.xml')
myroot = mytree.getroot()

def replacing(column_name):
    if '-' in column_name:

        return column_name.replace('-', '_', 3)
    else:
        return column_name


cnt = 1
for x in myroot.findall('offer'):
     cnt += 1
print(cnt)

try:
    sqliteConnection = sqlite3.connect('xml_parser.db')
    sqlite_drop_table_query = """DROP TABLE IF EXISTS objects"""
    sqlite_create_table_query = '''CREATE TABLE objects (
                                    id INTEGER PRIMARY KEY,
                                    type CHAR,
                                    property_type CHAR,
                                    category CHAR,
                                    creation_date DATETIME,
                                    last_update_date DATETIME,
                                    manually_added INT,
                                    mortgage BOOL,
                                    haggle BOOL,
                                    renovation CHAR,
                                    description TEXT, 
                                    new_flat INT,
                                    rooms INT,
                                    balcony CHAR,
                                    bathroom_unit INT,
                                    floor INT,
                                    floors_total INT,
                                    building_name CHAR,
                                    building_type CHAR,
                                    building_phase CHAR,
                                    building_section CHAR,
                                    built_year INT,
                                    ready_quarter INT,
                                    lift INT,
                                    parking INT,
                                    ceiling_height DOUBLE,
                                    nmarket_complex_id INT,
                                    nmarket_building_id INT,
                                    location CHAR,
                                    country CHAR,
                                    region CHAR,
                                    district CHAR,
                                    locality_name CHAR,
                                    sub_locality_name CHAR,
                                    non_admin_sub_locality CHAR,
                                    address CHAR,
                                    apartment INT,
                                    latitude DOUBLE,
                                    longitude DOUBLE,
                                    metro CHAR,
                                    time_on_foot INT,
                                    price INT,
                                    area DOUBLE,
                                    living_space DOUBLE,
                                    kitchen_space DOUBLE
                                    );'''
    cursor = sqliteConnection.cursor()
    print("Database Successfully Connected to SQLite")
    cursor.execute(sqlite_drop_table_query)
    cursor.execute(sqlite_create_table_query)
    print("SQLite table created")
    sqliteConnection.commit()
    #columns names
    names = list(map(lambda x: x[0], sqliteConnection.execute('select * from objects').description))

    for i in range(1, cnt):
        print(i)
        values = []
        columns = []
        for x in myroot[i]:
            if replacing(x.tag) in names:
                if replacing(x.tag) == 'location':
                    for j in range(0, 9):
                        values.append(x[j].text)
                        columns.append(replacing(x[j].tag))

                if replacing(x.tag) == 'price':
                    values.append(x[0].text)
                    columns.append(replacing(x.tag))
                if replacing(x.tag) == 'area':
                    values.append(x[0].text)
                    columns.append(replacing(x.tag))
                if replacing(x.tag) == 'living_space':
                    values.append(x[0].text)
                    columns.append(replacing(x.tag))
                if replacing(x.tag) == 'kitchen_space':
                    values.append(x[0].text)
                    columns.append(replacing(x.tag))
                else:
                    values.append(x.text)
                    columns.append(replacing(x.tag))

        sqlite_insert_query = "INSERT INTO objects(" + ', '.join(columns) + \
        ") VALUES(" + ', '.join(str(values).replace('[', '', 2).replace(']', '', 2).split(sep=',')) + ")"

        entiries = values
        print(sqlite_insert_query)
        count = cursor.execute(sqlite_insert_query)
        sqliteConnection.commit()
        print("Record inserted successfully into SqliteDb_developers table ", cursor.rowcount)

    cursor.close()

except sqlite3.Error as error:
    print("Error while connecting to sqlite", error)
finally:
    if sqliteConnection:
        sqliteConnection.close()
        print("The SQLite connection is closed")



