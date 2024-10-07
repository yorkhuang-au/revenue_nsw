""" This module implements the ETL task from Revenue NSW.

It reads data from a file(s) having pipe separated data (data), transform data to data model and
load the data into MongoDB.
"""

import csv
import logging
import datetime as dt
import re
from decimal import Decimal
import pymongo
import os

logger = logging.getLogger(__name__)

# Quote character in the source file
QUOTECHAR = '"'

# Source file delimiter
DELIMITER = '|'

# Source file schema
SCHEMA = [("FirstName", "string"),
          ("LastName", "string"),
          ("Company", "string"),
          ("BirthDate", "number"),
          ("Salary", "number"),
          ("Address", "string"),
          ("Suburb", "string"),
          ("State", "string"),
          ("Post", "number"),
          ("Phone", "number"),
          ("Mobile", "number"),
          ("Email", "string")]

# Reference date used to calculate age
REFERENCE_DATE = dt.datetime.strptime("2024-03-01", "%Y-%m-%d")

MONGO_DATABASE = "revenue_db"
MONGO_COLLECTION = "member_data"


def convert_record_to_dict(record:list, schema:list=SCHEMA)->dict:
    """ Convert a record from a list to a dict.

        If the record list has the same count of items as the schema, a proper dictionary
        is return. All the field values are stripped off the leading and trailing white spaces.
        Otherwise, return None and ignore this record.

        Parameters
        -----------
        record: list
            A list of every fields in the record to be converted.
        schema: list
            A list of (field_name, data_type) of the record fields

        Returns
        -----------
        :dict
            A dictionary with field name as key and field value as value.

    """
    if len(record) != len(schema):
        logger.warning(f"""Ignore record with different number of fields than the schema. Record:[{record}].""")
        return None

    return {fn:v.strip() for ((fn, ft), v) in zip(schema, record)}


def transform_name(raw_name:str)->str:
    """ Transform the raw name .

        Developer Note: Assume a proper name must start with alphabet (A-Z a-z).
        Remove any leading non alpabetic characters and trailing white spaces.
        Capitalize the name. Return None if the name is empty string.

        Parameters
        -----------
        raw_name: str
            A FirstName or LastName string from source file.

        Returns
        -----------
        :str
            A cleansed name if it is valid. None otherwise.
    """
    if not raw_name:
        return None

    name = re.sub(r'(^[^A-Za-z]+)|(\s+$)', '', raw_name).capitalize()

    return name if name else None


def transform_birthdate(raw_birth_date:str)->str:
    """ Transform the raw birth date from dmmyyyy to dd/mm/yyyy format.

        Developer Note: In the requirements, it says the raw birth date format is YYYY-MM-DD which is
        incorrect according to the actual data in the member-data.csv provided.
        In the data file, it is in dmmyyyy format.

        Parameters
        -----------
        raw_birth_date: str
            A birth date string from source file.

        Returns
        -----------
        :str
            A birth date string in dd/mm/yyyy format if the raw_birth_date is valid date. None otherwise.
    """
    if not raw_birth_date:
        return None

    try:
        year = int(raw_birth_date[-4:])
        month = int(raw_birth_date[-6:-4])
        day = int(raw_birth_date[:-6])
        return dt.date(year, month, day).strftime('%d/%m/%Y')
    except:
        logger.warning(f"""Invalid birthdate [{raw_birth_date}] to a date""")
        return None


def transform_salary(raw_salary:str)->str:
    """ Transform the raw salary to $ format with commas.

        Developer Note: I understand I don't need to round to exact dollar value. 4 decimal places are kept. I need to clarify
            with requirements.

        Parameters
        -----------
        raw_salary: str
            A salary string from source file.

        Returns
        -----------
        :str
            A salary string in $ format with commas if the raw_salary is valid number. None otherwise.
    """
    try:
        return '${:,.4f}'.format(Decimal(raw_salary))
    except:
        logger.warning(f"""Cannot convert salary [{raw_salary}] to a number""")
        return None


def transform_fullname(first_name:str, last_name:str)->str:
    """ Create a full name by concatting first_name and last_name.

        The full name is in [firstname lastname] format if both firstname and lastname exist.
        Either firstname or lastname only if the one of them is empty.

        Parameters
        -----------
        first_name: str
            First name.
        last_name: str
            Last name.

        Returns
        -----------
        :str
            A full name if first_name and/or last_name is valid. None otherwise.
    """
    fullname = " ".join([name for name in [first_name, last_name] if name])
    return fullname if fullname else None


def transform_age(birth_date:str, today:dt.datetime=REFERENCE_DATE)->int:
    """ Create an age in year by subtracting today with birth_date.

        Parameters
        -----------
        birth_date: str
            Birth date from the data.
        today: dt.datetime
            A reference date used to calculate the age.

        Returns
        -----------
        :int
            The age in year birth_date is valid. None otherwise.
    """
    if not birth_date:
        return None

    born = dt.datetime.strptime(birth_date, '%d/%m/%Y')
    age = today.year - born.year - ((today.month, today.day) < (born.month, born.day))
    return age if age > 0 else 0


def transform_salary_bucket(formatted_salary:str)->str:
    """ Create an Salary Bucket based on the salary.

        Salary Bucket:
        A for employees earning below 50,000
        B for employees earning between 50,000 and 100,000
        C for employees earning above 100,000

        Developer Note: I believe the bucket limits should be 50,000 and 100,000 instead of 50.000 and 100.00 as
        noted in the requirements.

        Parameters
        -----------
        formatted_salary: str
            Salary from the data.

        Returns
        -----------
        :str
            The salary bucket (A/B/C) if the salary is valid. None otherwise.
    """
    if not formatted_salary:
        return None

    salary = Decimal(re.sub(r'[$,]', '', formatted_salary))
    if salary < 50000:
        bucket = "A"
    elif salary > 100000:
        bucket = "C"
    else:
        bucket = "B"
    return bucket


def transform_address(street:str, suburb:str, state:str, post:str)-> dict:
    """ Create an Address nested field using Address(street), suburb, state and post from the source data.

        The new Address field is as below.
        {"Street": street, "Suburb": suburb, "State": state, "Post": post}
        If a field is empty, it is not included in the new Address dict.

        Parameters
        -----------
        street: str
            The street, which is from the Address field of source data.
        suburb: str
            The suburb
        state: str
            The state
        post: str
            The postcode

        Returns
        -----------
        :dict
            The new address dict if the some address fields are valid. None otherwise.
    """
    address = dict()
    if street:
        address['Street'] = street
    if suburb:
        address['Suburb'] = suburb
    if state:
        address['State'] = state
    if post:
        address['Post'] = post

    if address:
        return address
    else:
        return None


def set_dict_value_not_none(record, key, value):
    """ Set a key's value in the dict. If the value is None, remove the key.

        Parameters
        -----------
        record: dict
            The dict containing all the fields
        key: str
            The dict key which is to set a new value.
        value: object
            The new value

        Returns
        -----------
        :dict
            The dict with new value set or the key deleted
    """
    if value is None:
        record.pop(key, None)
    else:
        record[key] = value


def remove_unused_fileds(record:dict, unused:list=["FirstName", "LastName", "Suburb", "State", "Post"])->dict:
    """ Remove the unused fields from record dict.

        Parameters
        -----------
        record: dict
            The dict containing all the fields
        unused: list
            The list of field names to be removed.

        Returns
        -----------
        :dict
            The dict with unused fields removed.
    """
    for c in unused:
        record.pop(c, None)
    return record


def read_file(filename:str, delimiter:str=DELIMITER, schema:list=SCHEMA, quotechar:str=QUOTECHAR)->list:
    """ Read data from a file and load into memory. Return a dict with data.

        Invalid records are removed from result dict.

        Parameters
        -----------
        filename: str
            The source data filename
        delimiter: str
            The field delimiter
        schema: list
            The list of (field_name, data_type) for every record in data.
        quotechar: str
            The field quote character

        Returns
        -----------
        :list
            A list of dict of data.
    """
    with open(filename, 'r') as csvfile:
        record_reader = csv.reader(csvfile, delimiter=delimiter, quotechar=quotechar)

        # Converted None record is removed from final data
        data = [r for r in [convert_record_to_dict(record, schema) for record in record_reader] if r]

    return data


def transform_data(data:list)->list:
    """ Transform data and return the result.

        The following transformations are done:

        1. Convert the BirthDate from the format YYYY-MM-DD to DD/MM/YYYY, Salary in $ format with commas
        2. Do some cleaning on FirstName and LastName columns when needed and remove any leading/trailing spaces.
        3. Merge the FirstName and LastName columns into a new column named FullName.
        4. Calculate each employees's age from the BirthDate column using as reference Mar 1st, 2024. Add a new column named Age to store the computed age.
        5. Add a new column named SalaryBucket to categorize the employees based on their salary as follows:
            A for employees earning below 50.000
            B for employees earning between 50.000 and 100.000
            C for employees earning above 100.000
        6. Drop columns FirstName, LastName
        7. build nested entity class to have address as nested class.

        Parameters
        -----------
        data: list
            The source data

        Returns
        -----------
        :list
            A list of dict of transformed data.
    """
    for record in data:
        set_dict_value_not_none(record, "BirthDate", transform_birthdate(record.get("BirthDate", None)))
        set_dict_value_not_none(record, "Salary", transform_salary(record.get("Salary", None)))
        set_dict_value_not_none(record, "FirstName", transform_name(record.get("FirstName", None)))
        set_dict_value_not_none(record, "LastName", transform_name(record.get("LastName", None)))
        set_dict_value_not_none(record, "FullName", transform_fullname(record.get("FirstName", None),
                                                                       record.get("LastName", None)))
        set_dict_value_not_none(record, "Age", transform_age(record.get("BirthDate", None)))
        set_dict_value_not_none(record, "SalaryBucket", transform_salary_bucket(record.get("Salary", None)))
        set_dict_value_not_none(record, "Address", transform_address(record.get("Address", None),
                                                                     record.get("Suburb", None),
                                                                     record.get("State", None),
                                                                     record.get("Post", None)))
        remove_unused_fileds(record)

    result = [record for record in data if record]
    return result


def write_mongo(data:list, mongo_db:str, mongo_collection:str)->tuple:
    """ Write data into MongoDB.

        Since there is no unique key defined, records are appended to MongoDB even if they are duplicated.

        Parameters
        -----------
        data: list
            The source data
        mongo_db: str
            Mongo database name
        mongo_collection: str
            Mongo collection name

        Returns
        -----------
        :int
            Record count inserted into mongodb

    """
    mongo_url = os.environ.get("MONGODB_URL", "mongodb://mongo:27017/")
    logger.info(f"mongo_url {mongo_url}")

    inserted_count = 0
    with pymongo.MongoClient(mongo_url) as mongo:
        revenue_db = mongo[mongo_db]
        member_colection = revenue_db[mongo_collection]
        resp = member_colection.insert_many(data)

        inserted_count = len(resp.inserted_ids)
        if inserted_count == len(data):
            logger.info(f"Mongo inserted all {inserted_count} records into database[{mongo_db}], collection[{mongo_collection}].")
        else:
            logger.warning(f"Mongo inserted only {inserted_count} records into database[{mongo_db}], collection[{mongo_collection}], "
                           + f" out of {len(data)} source records."
                           )
    return inserted_count


def run_etl(filename):
    """ Run the etl process for a source filename
    """
    raw_data = read_file(filename)
    data = transform_data(raw_data)

    return write_mongo(data, MONGO_DATABASE, MONGO_COLLECTION)
