import logging
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass
from typing import Any

import connect_sql
import requests
import xmltodict
from config import AA, AA_KEY, AA_PROCEDURE, AA_TABLE, URL, xml_template
from traceback_logger import traceback_logger


# The class `ProcessingError` is a custom exception class in Python.
class ProcessingError(Exception):
    pass


# The class Continue is a custom exception in Python.
class Continue(Exception):
    pass


# The `EventLogger` class provides methods to log debug, info, and error messages using the Python
# logging module.
class EventLogger:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

    def log_debug(self, message):
        self.logger.debug(message)

    def log_info(self, message):
        self.logger.info(message)

    def log_error(self, message):
        self.logger.error(message)


# The `ConnectionHolder` class initializes a session and establishes SQL connections.
class ConnectionHolder:
    def __init__(self):
        self.session = requests.Session()
        self.cnxn_cur, self.conn = (connect_sql.connection(),
                                    connect_sql.pymssql_conn())


# This Python code defines a `DataHolder` class using the `dataclass` decorator. The class has three
# private attributes `_data`, `_updates`, and `_update_ids` with default values of `None`.
@dataclass
class DataHolder:
    _data: Any = None
    _updates: Any = None
    _update_ids: Any = None

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, new_data):
        self._data = new_data

    @property
    def updates(self):
        return self._updates

    @updates.setter
    def updates(self, new_updates):
        self._updates = new_updates

    @property
    def update_ids(self):
        return self._update_ids

    @update_ids.setter
    def update_ids(self, new_update_ids):
        self._update_ids = new_update_ids


# The `HolderFactory` class in Python provides a way to create different types of holders based on the
# specified holder type.
class HolderFactory:
    holder_dict = {
        DataHolder: DataHolder(),
        ConnectionHolder: ConnectionHolder()
        # extend holders here
    }

    @staticmethod
    def create_holder(holder_type):
        if holder_type in HolderFactory.holder_dict:
            return HolderFactory.holder_dict[holder_type]
        else:
            raise ValueError("Invalid holder type")


# The `Utils` class in Python provides static and class methods for various utility functions such as
# converting values to lists, manipulating dictionaries, and generating SQL strings.
class Utils:
    @staticmethod
    def to_list(v):
        return [v] if not isinstance(v, list) else v

    @staticmethod
    def lowercase_keys_and_stringify_values(dictionary):
        return {k.lower(): str(v) for k, v in dictionary.items()}

    @staticmethod
    def compare_sets(set_a, set_b):
        set_a_sorted = set(list(sorted(set_a.items())))
        set_b_sorted = set(list(sorted(set_b.items())))
        return dict(set_a_sorted - set_b_sorted)

    @classmethod
    def create_sql_procedure_str(cls, d, proc):
        x_dkv = cls._dict_to_sql_zip(d)
        sql_str = cls._create_sql_str(x_dkv)
        if '\\r\\n' in sql_str:
            sql_str = str(sql_str.replace(
                '\\r\\n', '\r\n').encode('utf-8'))[2:-1]
        sql = f"""EXEC {proc} \n{sql_str}"""
        return sql

    @classmethod
    def create_sql_update_str(cls, d, table, db_key, id_value):
        sql_zip = cls._dict_to_sql_zip(d)
        sql_str = cls._create_sql_str(sql_zip)
        sql_str = str(sql_str.replace(
            '@', '').replace('\\r\\n', '\r\n').encode('utf-8'))[2:-1]
        sql = f"""\
        set ANSI_WARNINGS OFF
        UPDATE [SBRS].[dbo].{table}
        SET
        {sql_str}
        WHERE {db_key}{
            f'={id_value}' if not isinstance(id_value, list) else
            f' in {tuple(id_value)}' if len(id_value)>1 else
            f'={id_value[0]}'
        }
        set ANSI_WARNINGS ON"""
        return sql

    def _dict_to_sql_zip(d):
        """
        The function `_dict_to_sql_zip` converts a dictionary into a list of tuples where keys are prefixed
        with '@'.

        :param d: Takes a dictionary `d` as input and converts it into a
        list of tuples where each tuple contains a key from the dictionary prefixed with '@' and its
        corresponding value.
        :return: Returns a list of tuples where each tuple contains a key-value pair from the input
        dictionary `d`. The keys are prefixed with '@' and the values are unchanged.
        """
        d_keys = d.keys()
        d_keys = deque(f'@{k}' for k in d_keys)
        d_values = d.values()
        return list(zip(d_keys, d_values))

    def _create_sql_str(sql_zip, sql_str=''):
        """
        The function `_create_sql_str` generates an SQL string by combining key-value pairs from a list of
        tuples.

        :param sql_zip: Creates an SQL string from a list of tuples `sql_zip`. The function iterates over
        the tuples in `sql_zip`, concatenates the elements with an equal sign and a comma, and then
        replaces any occurrences of `'NULL'` :param sql_str: The `sql_str` parameter is a string that
        represents an SQL query. It is used to build the SQL query by concatenating key-value pairs from
        the `sql_zip` list. Each key-value pair is formatted as `key='value'` in the SQL query string.
        :return: Returns a SQL string with key-value pairs formatted as column_name='value'. The
        function replaces single quotes in the values with double single quotes and also replaces the
        string 'NULL' with NULL.
        """
        v_last = sql_zip[-1]
        for v in sql_zip[:-1]:
            sql_str += "=".join(
                [str(v[0]), "'" + str(v[1]).replace("'", "''") + "'"]) + ","
        sql_str += "=".join(
            [str(v_last[0]), "'" + str(v_last[1]).replace("'", "''") + "'"])
        return sql_str.replace("'NULL'", 'NULL')


# The `XmlGetter` class retrieves XML data from a 3rd party database using a specified constant value.
class XmlGetter:
    xml = xml_template % '<ns:get{const}/>'

    def __init__(self, data_holder, const, conn_holder):
        self.data_holder = data_holder
        self.const = const
        self.session = conn_holder.session
        self.logger = EventLogger()

    @property
    def data(self):
        return self.data_holder.data

    @data.setter
    def data(self, new_data):
        self.data_holder.data = new_data

    def process(self):
        xml = self._get_xml()
        xml_dict = xmltodict.parse(self.session.post(URL, data=xml).text)
        self.data = xml_dict

    def _get_xml(self):
        self.logger.log_info("Getting xml from 3rd party database...")
        return self.xml.format(const=self.const)


# The `XmlProcessor` class is designed to process XML data, extract specific content, handle faults,
# and extract data based on specified keys.
class XmlProcessor:
    SOAP_ENV = "SOAP-ENV:Envelope"
    SOAP_BODY = "SOAP-ENV:Body"
    SOAP_FAULT = "SOAP-ENV:Fault"
    FAULT_STRING = "faultstring"
    NS1_RESPONSE = "ns1:get{const}Response"
    RETURN = "return"

    def __init__(self, data_holder, const, keys):
        self.data_holder = data_holder
        self.const = const
        self.keys = keys

    @property
    def data(self):
        return self.data_holder.data

    @data.setter
    def data(self, new_data):
        self.data_holder.data = new_data

    def process(self):
        try:
            modified_data = self._extract_xml_body_content()
            self.result_data = modified_data
            self.result_data = self._extract_data_by_keys()
        except Exception as e:
            raise ProcessingError(f'{self._extract_xml_fault_string(e)}')

        if not self.result_data:
            raise ProcessingError('Empty XML Response')

        self.data = self.result_data

    def _extract_xml_body_content(self):
        return self.data[self.SOAP_ENV][self.SOAP_BODY][
            self.NS1_RESPONSE.format(const=self.const)
        ][self.RETURN]

    def _extract_xml_fault_string(self, error):
        try:
            faultstring = self.data[self.SOAP_ENV][self.SOAP_BODY][
                self.SOAP_FAULT][self.FAULT_STRING]
            return faultstring
        except Exception:
            traceback_logger('XML', error)
            raise ProcessingError("Error while XML faultstring parsing.")

    def _extract_data_by_keys(self):
        if self.keys:
            for key in self.keys:
                self.result_data = (
                    [rd[key] for rd in self.result_data[key]]
                    if isinstance(self.result_data[key], tuple)
                    else self.result_data[key]
                )
        return self.result_data


# The `DataNormalizer` class contains methods to normalize and process data by converting boolean
# strings, extracting data via keywords (e.g., 'memberships'), and normalizing values.
class DataNormalizer:
    NULL_VALUE = 'NULL'
    ACTIVE_STATUS = 'active_status'
    ACTIVE = 'Active'
    MEMBERSHIPS = 'memberships'
    VENDOR_ID = 'vendor_id'
    XSI_NIL = 'xsi:nil'
    TRUE = 'true'
    FALSE = 'false'

    def __init__(self, data_holder):
        self.data_holder = data_holder

    @property
    def data(self):
        return self.data_holder.data

    @data.setter
    def data(self, new_data):
        self.data_holder.data = new_data

    def process(self):
        modified_data = Utils.to_list(self.data_holder.data)
        new_modified_data = list()

        self._normalize_data(modified_data, new_modified_data)
        self.data = new_modified_data

    def _normalize_data(self, modified_data, new_modified_data):
        for item in modified_data:
            normalized_item = self._normalize_values(item)
            normalized_item = self._extract_memberships_by_key(normalized_item)
            normalized_item = self._convert_boolean_strings(normalized_item)
            new_modified_data.append(normalized_item)

    def _normalize_values(self, item):
        return {
            k: self.NULL_VALUE if v == self.XSI_NIL
            else True if k == self.ACTIVE_STATUS and v == self.ACTIVE
            else False if k == self.ACTIVE_STATUS
            else v for k, v in item.items()
            if k != self.VENDOR_ID and not isinstance(v, dict)
        }

    def _extract_memberships_by_key(self, item):
        if self.MEMBERSHIPS in item:
            m = item[self.MEMBERSHIPS]
            if isinstance(m, dict) and m != {self.XSI_NIL: self.TRUE}:
                item |= m
        return item

    def _convert_boolean_strings(self, item):
        return {
            k: False if v == self.FALSE
            else True if v == self.TRUE
            else v for k, v in item.items()
        }


# The `DuplicateRemover` class processes data by checking for duplicates in a database table and
# handling new or existing items accordingly.
class DuplicateRemover:
    def __init__(self, data_holder, table, db_key, xd_key, conn):
        self.data_holder = data_holder
        self.table = table
        self.db_key = db_key
        self.xd_key = xd_key
        self.conn = conn
        self.logger = EventLogger()

    def process(self):
        current_data = Utils.to_list(self.data_holder.data)
        insert_data = list()
        update_data = list()
        update_ids = list()

        self._check_dupe(current_data, insert_data, update_data, update_ids)

        self.data_holder.data = insert_data
        self.data_holder.updates = update_data
        self.data_holder.update_ids = update_ids

    def _check_dupe(self, current_data, insert_data, update_data, update_ids):
        for item in current_data[:]:
            df = self._query_database(item)

            if df.rowcount == 0:
                self._handle_new_item(item, insert_data)
            else:
                self._handle_existing_item(item, df, update_data, update_ids)

    def _query_database(self, item):
        """
        The `_query_database` function executes a SQL query to retrieve data from a database table based on
        a specified key.

        :param item: The `item` parameter in the `_query_database` method is a dictionary containing data
        that will be used to query the database.
        :return: The `_query_database` method is returning the result of a SQL query executed using the
        `pymssql_query` function from the `connect_sql` module. The SQL query selects all columns (`*`) from
        a table specified by `self.table` where a specific condition is met. The condition is based on
        comparing a column specified by `self.db_key` with a value extracted from the `item`.
        """
        return connect_sql.pymssql_query(
            self.conn, f"""\
            SELECT * FROM {self.table}
            WHERE {self.db_key}={item[self.xd_key]}"""
        )

    def _handle_new_item(self, item, insert_data):
        """
        The function `_handle_new_item` appends a new item to a list and logs information about the new
        item.

        :param item: The `item` parameter is the data that you want to add to the `insert_data` list.
        :param insert_data: The `insert_data` parameter is a list that stores items to be inserted into a
        database. The `_handle_new_item` method appends a new item to this list for insertion.
        """
        insert_data.append(item)
        self.logger.log_info(f'New {db_key} {item[self.xd_key]}.')

    def _handle_existing_item(self, item, df, update_data, update_ids):
        """
        The function `_handle_existing_item` compares data from a database and XML file, identifies
        differences, and logs messages accordingly.

        :param item: The `item` parameter in the `_handle_existing_item` method is a dictionary representing
        an item that needs to be handled. It contains data that will be compared with data from a database
        query result (`df.fetchone()`). The method compares the data in the `item` dictionary with the data
        from the database query result.
        :param df: The `df` parameter in the `_handle_existing_item` method is likely a database cursor or
        connection object used to fetch data from a database. It is used to retrieve a single row of data
        from the database in the form of a dictionary.
        :param update_data: The `update_data` parameter is a list that stores dictionaries containing the
        data that needs to be updated in the database. Each dictionary in the list represents a set of
        key-value pairs that need to be updated for a specific item.
        :param update_ids: The `update_ids` parameter is a list that is used to store the IDs of items that
        need to be updated in a database table. In the provided code snippet, the `update_ids` list is being
        appended with the ID of the item if it requires updating in the specified table.
        """
        db_dict = Utils.lowercase_keys_and_stringify_values(df.fetchone())
        xml_dict = Utils.lowercase_keys_and_stringify_values(item)
        update_dict = Utils.compare_sets(xml_dict, db_dict)

        if not update_dict:
            self.logger.log_debug(f'{db_key} {item[self.xd_key]} already in '
                                  f'{self.table} with current data.')
        else:
            update_data.append(update_dict)
            update_ids.append(item.get(self.db_key))
            self.logger.log_info(f'{db_key} {item[self.xd_key]} '
                                 f'requires updating in {self.table}')


# The above class `IDataProcessor` is an abstract class with a method `process` that must be
# implemented by its subclasses.
class IDataProcessor(ABC):
    @abstractmethod
    def process(self):
        pass


# The `DataProcessor` class takes in a `DataNormalizer` and a `DuplicateRemover` and processes data by
# normalizing it and removing duplicates.
class DataProcessor(IDataProcessor):
    def __init__(self, data_normalizer: DataNormalizer,
                 duplicate_remover: DuplicateRemover,):
        self.data_normalizer = data_normalizer
        self.duplicate_remover = duplicate_remover

    def process(self):
        self.data_normalizer.process()
        self.duplicate_remover.process()


# The `IDatabaseOperation` class is an abstract base class in Python with an abstract method
# `execute`.
class IDatabaseOperation(ABC):
    @abstractmethod
    def execute(self):
        pass


# The `SqlCreator` class is used to create and execute SQL procedures and updates based on provided
# data and parameters.
class SqlCreator(IDatabaseOperation):
    def __init__(self, proc, table):
        self.proc = proc
        self.table = table

    def execute(self, data):
        return list(map(Utils.create_sql_procedure_str, data, [self.proc]*len(data)))

    def update(self, data, update_ids):
        return list(map(
            Utils.create_sql_update_str, data, [self.table]*len(data),
            [db_key]*len(data), update_ids
        ))


# The `DataBaseHandler` class initializes a `SqlCreator` object and provides methods to create SQL
# queries and updates using the `SqlCreator`.
class DataBaseHandler:
    def __init__(self, table, proc):
        self.sql_creator = SqlCreator(proc, table)

    def create_sql(self, data):
        return self.sql_creator.execute(data)

    def create_sql_update(self, data, update_ids):
        return self.sql_creator.update(data, update_ids)


# The `EventTypeHandler` class handles event data processing and database operations using various
# components.
class EventTypeHandler:
    def __init__(self, conn_holder, data_holder, xml_getter,
                 database_handler, xml_processor, data_processor):
        self.conn_holder = conn_holder
        self.data_holder = data_holder
        self.xml_getter = xml_getter
        self.database_handler = database_handler
        self.xml_processor = xml_processor
        self.data_processor = data_processor

    def handle(self):
        """
        The `handle` function processes XML data, creates SQL statements, and executes them to insert and
        update data in a database.
        """
        self.xml_getter.process()
        self.xml_processor.process()
        self.data_processor.process()

        result_inserts = self.data_holder.data
        result_updates = self.data_holder.updates
        result_update_ids = self.data_holder.update_ids

        sql_inserts = self.database_handler.create_sql(result_inserts)
        sql_updates = self.database_handler.create_sql_update(
            result_updates, result_update_ids)

        for sql in sql_updates:
            connect_sql.execute_commit_sql(sql, self.conn_holder.cnxn_cur)

        for sql in sql_inserts:
            connect_sql.execute_commit_sql(sql, self.conn_holder.cnxn_cur)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    const = AA
    keys = AA_KEY
    table = AA_TABLE
    db_key = 'client_id'
    xd_key = 'client_id'
    proc = AA_PROCEDURE

    try:
        logger = EventLogger()
        data_holder = HolderFactory.create_holder(DataHolder)
        conn_holder = HolderFactory.create_holder(ConnectionHolder)
        xml_getter = XmlGetter(data_holder, const, conn_holder)
        database_handler = DataBaseHandler(table, proc)
        xml_processor = XmlProcessor(data_holder, const, keys)
        data_normalizer = DataNormalizer(data_holder)
        duplicate_remover = DuplicateRemover(data_holder, table, db_key,
                                             xd_key, conn_holder.conn)
        data_processor = DataProcessor(data_normalizer, duplicate_remover)
        handler = EventTypeHandler(conn_holder, data_holder, xml_getter,
                                   database_handler, xml_processor,
                                   data_processor,)
        handler.handle()
    except ProcessingError as pe:
        logger.log_error(f'ProcessingError occured: {pe}')
    except Exception as ex:
        traceback_logger('AA', ex)
    finally:
        logger.log_info('AA up to date.')
