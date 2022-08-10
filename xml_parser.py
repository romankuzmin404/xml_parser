import sqlite3
import xml.etree.ElementTree as ET
from loguru import logger

mytree = ET.parse('feed-example.xml')
myroot = mytree.getroot()

logger.add("special.log", filter=lambda record: "special" in record["extra"])
logger.bind(special=True).info("------------------------------------------------------------------------------------")


def replacing(column_name):
    return column_name.replace('-', '_', 3)


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
                                    name CHAR,
                                    time_on_foot INT,
                                    time_on_transport INT,
                                    price INT,
                                    area DOUBLE,
                                    living_space DOUBLE,
                                    kitchen_space DOUBLE
                                    );'''
    cursor = sqliteConnection.cursor()
    logger.bind(special=True).info("Database Successfully Connected to SQLite")
    cursor.execute(sqlite_drop_table_query)
    cursor.execute(sqlite_create_table_query)
    logger.bind(special=True).info("SQLite table created")
    sqliteConnection.commit()
    # columns names
    names = list(map(lambda x: x[0], sqliteConnection.execute('select * from objects').description))
    for i in range(1, len(myroot)):
        logger.debug(i)
        values = []
        columns = []
        for x in myroot[i]:
            _tag = replacing(x.tag)
            if _tag in names:
                if _tag in ['price', 'area', 'living_space', 'kitchen_space']:
                    values.append(x[0].text)
                    columns.append(_tag)
                if _tag == 'location':
                    for j in range(0, 10):
                        if x[j].tag == 'metro':
                            for metro_info in x[j]:
                                values.append(metro_info.text)
                                columns.append(replacing(metro_info.tag))
                        else:
                            values.append(x[j].text)
                            columns.append(replacing(x[j].tag))
                else:
                    values.append(x.text)
                    columns.append(_tag)

        sqlite_insert_query = "INSERT INTO objects(" + ', '.join(columns) + \
                              ") VALUES(" + ', '.join(
            str(values).replace('[', '', 2).replace(']', '', 2).split(sep=',')) + ")"

        logger.bind(special=True).info("insert query {}".format(sqlite_insert_query))
        count = cursor.execute(sqlite_insert_query)
        sqliteConnection.commit()
        logger.bind(special=True).info(
            "Record inserted successfully into SqliteDb_developers table {}".format(cursor.rowcount))

    cursor.close()

except sqlite3.Error as error:
    logger.bind(special=True).error("Error while connecting to sqlite{}".format(error))
finally:
    if sqliteConnection:
        sqliteConnection.close()
        logger.bind(special=True).info("The SQLite connection is closed")
