import sqlite3
import xml.etree.ElementTree as ET
from loguru import logger

class CustomList(list):
    def __init__(self, seq=(), need_replacing: bool = False):
        self.need_replacing = need_replacing
        super().__init__(seq)
    def append(self, obj):
        obj = obj if not self.need_replacing else obj.replace('-', '_', 3)
        super().append(obj)

class ParserXMLtoSQlite():
    """Class parser from XML to SQlite"""
    target_db = ''
    source_xml = ''

    def __init__(self, source_xml, target_db):
        self.source_xml = source_xml
        self.target_db = target_db
        self.table_name = ''
    # connection to db
    def connection_db(self):
        sqliteConnection = sqlite3.connect(self.target_db)
        logger.bind(special=True).info("Database Successfully Connected to SQLite")
        return sqliteConnection

    def connection_close(self, sqliteConnection):
        if sqliteConnection:
            sqliteConnection.close()
            logger.bind(special=True).info("The SQLite connection is closed")

    # create table
    def create_db(self, sqliteConnection, table_name):
        self.table_name = table_name
        cursor = sqliteConnection.cursor()
        sqlite_drop_table_query = "DROP TABLE IF EXISTS {}".format(table_name)
        sqlite_create_table_query = '''CREATE TABLE {} (
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
                                                    name CHAR,
                                                    time_on_foot INT,
                                                    time_on_transport INT,
                                                    price INT,
                                                    area DOUBLE,
                                                    living_space DOUBLE,
                                                    kitchen_space DOUBLE
                                                    );'''.format(table_name)
        cursor.execute(sqlite_drop_table_query)
        cursor.execute(sqlite_create_table_query)
        logger.bind(special=True).info("SQLite table created")
        sqliteConnection.commit()
        cursor.close()

    def root_init(self):
        mytree = ET.parse(self.source_xml)
        myroot = mytree.getroot()
        return myroot

    def get_names(self, sqliteConnection):
        names = list(map(lambda x: x[0], sqliteConnection.execute('select * from {}'.format(self.table_name)).description))
        return names

    def tag_parsing(self, root, column_names):
        record_cnt = 1
        for offer in root[1:]:
            # parse
            values = CustomList()
            columns = CustomList(need_replacing=True)
            for attribute in offer:
                _attribute = attribute.tag.replace('-', '_', 3)
                if _attribute in 'location'.join(column_names):
                    if _attribute in ['price', 'area', 'living_space', 'kitchen_space']:
                        values.append(attribute[0].text)
                        columns.append(attribute.tag)
                    if _attribute == 'location':
                        for location_atribute in attribute:
                            if location_atribute.tag == 'metro':
                                for metro in location_atribute:
                                    values.append(metro.text)
                                    columns.append(metro.tag)
                            else:
                                values.append(location_atribute.text)
                                columns.append(location_atribute.tag)
                    else:
                        values.append(attribute.text)
                        columns.append(_attribute)
            cursor = sqliteConnection.cursor()
            sqlite_insert_query = "INSERT INTO objects(" + ', '.join(columns) + \
                                  ") VALUES(" + ', '.join(str(values).replace('[', '', 2).replace(']', '', 2).split(sep=',')) + ")"
            logger.bind(special=True).info("insert query {}".format(sqlite_insert_query))
            cursor = cursor.execute(sqlite_insert_query)
            sqliteConnection.commit()

            logger.bind(special=True).info("Record inserted successfully into SqliteDb_developers table {}".format(record_cnt))
            record_cnt += 1
        cursor.close()

# logger
logger.add("special.log", filter=lambda record: "special" in record["extra"])
logger.bind(special=True).info("------------------------------------------------------------------------------------")
# create an object of class ParserXMLtoSQlite with name source xml and target db
parser = ParserXMLtoSQlite('feed-example.xml', "xml_parser.db")
try:
    # connection to db
    sqliteConnection = parser.connection_db()
    # create table with name
    parser.create_db(sqliteConnection, 'objects')
    # init root
    myroot = parser.root_init()
    # names of columns
    names = parser.get_names(sqliteConnection)
    # parse from root
    parser.tag_parsing(myroot, names)




except sqlite3.Error as error:
    logger.bind(special=True).error("Error while connecting to sqlite{}".format(error))
finally:
    parser.connection_close(sqliteConnection)


