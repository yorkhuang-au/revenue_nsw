import pytest
from etl import ingest_member_data as imd

@pytest.fixture
def invalid_record_with_more_fields():
    return ["first_name", "last_name", "company", "birth", "salary", "addr", "suburb",
            "state", "post", "phone", "mobile", "email", "extra field"]


@pytest.fixture
def input_data_list1():
    # The 1st record has more fields. The 2nd record has less fields and the 3rd record has the exact fields.
    return [
        (["first_name", "last_name", "company", "birth", "salary", "addr", "suburb",
            "state", "post", "phone", "mobile", "email", "extra field"],
            None),
        (["first_name", "last_name", "company", "birth", "salary", "addr", "suburb",
            "state", "post", "phone", "mobile",],
            None),
        (["first_name   ", "  last_name", "company", "birth", "salary", "addr", "suburb",
            "state", "post", "phone", "mobile", "  email  "],
        {"FirstName": "first_name", "LastName": "last_name", "Company": "company",
         "BirthDate": "birth", "Salary": "salary", "Address": "addr", "Suburb": "suburb",
        "State": "state", "Post": "post", "Phone": "phone", "Mobile": "mobile", "Email": "email"}),
    ]


def test_convert_record_to_dict(input_data_list1):
    for (source, expected) in input_data_list1:
        assert imd.convert_record_to_dict(source) == expected, \
            "convert_record_to_dict should convert a record from list to a dict if it is valid, None otherwise."


def test_read_file(mocker, input_data_list1):
    mocker.patch("etl.ingest_member_data.open")
    mock_record_reader = mocker.patch("csv.reader")
    mock_record_reader.return_value = iter([source for (source, _) in input_data_list1])
    expected = [expected for (_, expected) in input_data_list1 if expected]
    got = imd.read_file("dummy_filename")

    assert got == expected, "read_file should exclude the invalid records."


@pytest.mark.parametrize(
    "raw_name, expected", [
        (" york ", "York"),
        ("- York-", "York-"),
        ("123York  ", "York"),
        ("YORK", "York"),
        ("   ", None),
        ("1231-^12 3 ", None),
        ("", None),
        (None, None),
    ]
)
def test_transform_name(raw_name, expected):
    assert imd.transform_name(raw_name) == expected, \
        "transform_name should cleanse a name properly."


@pytest.mark.parametrize(
    "raw_birth_date, expected", [
        ("11122021", "11/12/2021"),
        ("1011970", "01/01/1970"),
        ("122190", None),
        ("01311950", None),
        ("31021990", None),
        (None, None),
        ("", None)
    ]
)
def test_transform_brithdate(raw_birth_date, expected):
    assert imd.transform_birthdate(raw_birth_date) == expected, \
        "transform_birthdate should transform a birthdate to dd/mm/yyyy properly."


@pytest.mark.parametrize(
    "raw_salary, expected", [
        ("1112.2021", "$1,112.2021"),
        ("101", "$101.0000"),
        ("5122190.","$5,122,190.0000"),
        ("01311950a", None),
        ("0", "$0.0000"),
        (None, None),
        ("", None)
    ]
)
def test_transform_salary(raw_salary, expected):
    assert imd.transform_salary(raw_salary) == expected, \
        "transform_salary should transform a salary into $format with commas."


@pytest.mark.parametrize(
    "first_name, last_name, expected", [
        ("York", "Huang", "York Huang"),
        ("", "Huang", "Huang"),
        (None, "Huang", "Huang"),
        ("York", "", "York"),
        ("York", None, "York"),
        (None, None, None),
        ("", "", None),
        ("", None, None),
        (None, "", None)
    ]
)
def test_transform_fullname(first_name, last_name, expected):
    assert imd.transform_fullname(first_name, last_name) == expected, \
        "transform_fullname should combine first name and last_name properly."


@pytest.mark.parametrize(
    "birth_date, expected", [
        ("01/12/1970", 53),
        ("02/02/1980", 44),
        ("01/03/2020", 4),
        ("02/03/2024", 0),
        ("01/03/2024", 0),
        (None, None),
        ("", None)
    ]
)
def test_transform_age(birth_date, expected):
    assert imd.transform_age(birth_date) == expected, \
        (f"transform_age should calcluate the age from {imd.REFERENCE_DATE} to "
        + "the birth_date(dd/mm/yyyy) properly.")


@pytest.mark.parametrize(
    "formatted_salary, expected", [
        ("$14,234.5678", "A"),
        ("$49,999.9999", "A"),
        ("$50,000.0000", "B"),
        ("$80,000.3453", "B"),
        ("$100,000.0000", "B"),
        ("$100,000.0001", "C"),
        ("$8,114,234.5678", "C"),
        ("$0.0000", "A"),
        (None, None),
        ("", None),
    ]
)
def test_transform_salary_bucket(formatted_salary, expected):
    assert imd.transform_salary_bucket(formatted_salary) == expected, \
        (f"transform_salary_bucket should calcluate salary from formatted_salary {formatted_salary} properly.")


@pytest.mark.parametrize(
    "street, suburb, state, post, expected", [
        ("1 York street", "Sydney", "NSW", "2000",
            {"Street": "1 York street",
             "Suburb": "Sydney",
             "State": "NSW",
             "Post": "2000"}),
        ("", "", "NSW", "2000",
            {"State": "NSW",
             "Post": "2000"}),
        ("1 York street", None, None, "2000",
            {"Street": "1 York street",
             "Post": "2000"}),
        ("", "", "", "",
            None),
        (None, None, None, None,
            None),
    ]
)
def test_transform_address(street, suburb, state, post, expected):
    assert imd.transform_address(street, suburb, state, post) == expected, \
        ("transform_address should calcluate a nested Address object from (Address, Suburb, State, Post)="
         + f"({street}, {suburb}, {state}, {post}s properly.")


@pytest.mark.parametrize(
    "record, key, value, expected", [
        ({"FirstName": "York", "LastName": "Huang"}, "LastName", "Lin",
            {"FirstName": "York", "LastName": "Lin"}),
        ({"FirstName": "York", "LastName": "Huang"}, "LastName", "",
            {"FirstName": "York", "LastName":""}),
        ({"FirstName": "York", "LastName": "Lin"}, "FirstName", None,
            {"LastName": "Lin"}),
        ({"FirstName": "York", "LastName": "Huang"}, "Post", "2000",
            {"FirstName": "York", "LastName": "Huang", "Post": "2000"}),
        ({"FirstName": "York",}, "FirstName", None,
            {}),
        ({}, "FirstName", None,
            {}),
        ({}, "FirstName", "York",
            {"FirstName": "York"}),
    ]
)
def test_set_dict_value_not_none(record, key, value, expected):
    imd.set_dict_value_not_none(record, key, value)
    assert record == expected, \
        (f"set_dict_value_not_none should set record[key]=value properly for {record} {key} {value}. "
         + "None value will cause the key to be deleted.")


@pytest.mark.parametrize(
    "record, unused, expected", [
        ({"FirstName": "York", "LastName": "Huang"}, ["LastName",],
            {"FirstName": "York"}),
        ({"FirstName": "York", "LastName": "Huang"}, ["LastName", "FirstName"],
            {}),
        ({"FirstName": "York", "LastName": "Lin"}, [],
            {"FirstName": "York", "LastName": "Lin"}),
        ({"FirstName": "York", "LastName": "Huang"}, ["Post",],
            {"FirstName": "York", "LastName": "Huang"}),
        ({}, ["FirstName",],
            {}),
        ({}, [],
            {}),
    ]
)
def test_remove_unused_fileds(record, unused, expected):
    imd.remove_unused_fileds(record, unused)
    assert record == expected, \
        "remove_unused_fileds should remove unused fields properly. "




@pytest.mark.parametrize(
    "data, expected", [
        ([
            {"FirstName": "2york", "LastName": "1huang", "Company": "RevenueNSW",
            "BirthDate": "2111980", "Salary": "89000.56789", "Address": "1 George st", "Suburb": "Sydney",
            "State": "NSW", "Post": "2000", "Phone": "0298765432", "Mobile": "0404123456", "Email": "york.huang@mycom.com"},
            {"FirstName": "George", "LastName": "adam", "Company": "RevenueNSW",
            "BirthDate": "as2111980", "Salary": "89000.56789", "Address": "1 George st", "Suburb": "Sydney",
            "State": "NSW", "Post": "2000", "Phone": "0298765432", "Mobile": "0404123456", "Email": "york.huang@mycom.com"},
            {},
        ],
        [
            {"FullName": "York Huang", "Company": "RevenueNSW",
            "BirthDate": "02/11/1980", "Age": 43, "Salary": "$89,000.5679", "SalaryBucket": "B",
            "Address": {"Street": "1 George st", "Suburb": "Sydney",
                "State": "NSW", "Post": "2000"},
            "Phone": "0298765432", "Mobile": "0404123456", "Email": "york.huang@mycom.com"},
            {"FullName": "George Adam", "Company": "RevenueNSW",
            "Salary": "$89,000.5679", "SalaryBucket": "B",
            "Address": {"Street": "1 George st", "Suburb": "Sydney",
                "State": "NSW", "Post": "2000"},
            "Phone": "0298765432", "Mobile": "0404123456", "Email": "york.huang@mycom.com"},
        ]),
    ]
)
def test_transform_data(data, expected):
    got = imd.transform_data(data)
    assert got == expected, \
        "transform_data should transform data into the final record."


@pytest.mark.parametrize(
    "data, expected", [
        ([
            {"FullName": "York Huang", "Company": "RevenueNSW",
            "BirthDate": "02/11/1980", "Age": 43, "Salary": "$89,000.5679", "SalaryBucket": "B",
            "Address": {"Street": "1 George st", "Suburb": "Sydney",
                "State": "NSW", "Post": "2000"},
            "Phone": "0298765432", "Mobile": "0404123456", "Email": "york.huang@mycom.com"},
            {"FullName": "George Adam", "Company": "RevenueNSW",
            "Salary": "$89,000.5679", "SalaryBucket": "B",
            "Address": {"Street": "1 George st", "Suburb": "Sydney",
                "State": "NSW", "Post": "2000"},
            "Phone": "0298765432", "Mobile": "0404123456", "Email": "york.huang@mycom.com"},
        ],
        [1, 2]),
    ]
)
def test_write_mongo(mocker, data, expected):
    mock_mongo = mocker.patch("pymongo.MongoClient")
    mock_mongo.return_value.__enter__.return_value.__getitem__.return_value.__getitem__ \
        .return_value.insert_many.return_value.inserted_ids = expected

    got = imd.write_mongo(data, "mymongodb", "mycollection")
    mock_insert_many = mock_mongo.return_value.__enter__.return_value.__getitem__ \
        .return_value.__getitem__.return_value.insert_many

    call_args_list = mock_insert_many.call_args_list
    args, kwargs = call_args_list[0]
    assert mock_insert_many.call_count == 1 and args == (data,) and kwargs == {}, \
        "write_mongo should call insert_many once with data parameter"

    assert got == len(expected), \
        f"write_mongo should insert {len(expected)} records into MongoDB."
