import unittest
import util
import pytz
import datetime
import json
import pandas as pd

class TestStringMethods(unittest.TestCase):
    def test_list_to_string(self):
        self.assertEqual(util.convert_list_to_string(["this, is a string", "this is another one"]), "[this, is a string, this is another one]")
        self.assertEqual(util.convert_list_to_string(["this, is a string"]), "[this, is a string]")
        self.assertEqual(util.convert_list_to_string([]), "[]")
        self.assertEqual(util.convert_list_to_string([""]), "[]")
        self.assertEqual(util.convert_list_to_string([{}]), "[{}]")
        self.assertEqual(util.convert_list_to_string([{}, []]), "[{}, []]")
        self.assertEqual(util.convert_list_to_string([{}, [], ["this is a string", "this is another one"], ""]), "[{}, [], [this is a string, this is another one], ]")
        self.assertEqual(util.convert_list_to_string([-5, -4, "", {}, [], ""]), "[-5, -4, , {}, [], ]")

    def test_json_to_string(self):
        dict1 = {
            "key 1": [],
            "key 2": "",
            "key3": {
                "key4": "abcd",
                "key5": ["This is a string", "This, is another one"],
                "key 6": -1,
                "key 7": 225,
                "key8": datetime.datetime.now()
            }
        }
        self.assertEqual(util.convert_json_to_string(dict1), json.dumps(dict1))
        dict2 = {}
        self.assertEqual(util.convert_json_to_string(dict2), json.dumps(dict2))
    
    def test_validate_or_convert(self):
        docu1 = {
            "Key 1": ["This is a list", 123],
            "Key 2": ["This is a list", 123],
            "Key 3": "",
        }
        schema1 = {
            "Key 1": "str",
        }
        check1 = {
            'Key 1': util.convert_list_to_string(docu1['Key 1']),
            'Key 2': util.convert_list_to_string(docu1['Key 2']),
            'Key 3': ''
        }

        docu2 = {
            "Key 1": {
                "Key 2": "abcd",
                "Key 3": ["This is a string", "This, is another one"],
                "Key 4": -1,
                "Key 5": 225,
                "Key 6": datetime.datetime(1602, 2, 28, 5, 30, 24)
            },
        }
        schema2 = {}
        check2 = {
            'Key 1': util.convert_json_to_string(docu2['Key 1'])
        }

        docu3 = {
            "Key 1": datetime.datetime(1602, 2, 28, 5, 30, 24),
            "Key 3": "2021-12-06T10:33:22Z",
        }
        schema3 = {
            "Key 1": "datetime",
            "Key 3": "datetime",
        }
        check3 = {
            'Key 1' : util.utc_to_local(datetime.datetime(1602, 2, 28, 5, 30, 24), pytz.timezone("Asia/Kolkata")),
            "Key 3": util.utc_to_local(datetime.datetime(2021, 12, 6, 10, 33, 22), pytz.timezone("Asia/Kolkata"))
        }

        docu4 = {
            "Key 1": 1.0,
            "Key 2": 1.0,
            "Key 3": 1.0,
            "Key 4": 2,
            "Key 5": 2,
            "Key 6": 2,
            "Key 7": "2 ",
            "Key 8": "2 ",
            "Key 9": "2 abcd",
            "Key 10": "2 abcd",
            "Key 11": "2.012",
            "Key 12": "2.013",
        }
        schema4 = {
            "Key 1": "float",
            "Key 2": "int",
            "Key 4": "float",
            "Key 5": "int",
            "Key 7": "int",
            "Key 8": "float",
            "Key 9": "int",
            "Key 10": "float",
            "Key 11": "int",
            "Key 12": "float"
        }
        check4 = {
            "Key 1": 1.0,
            "Key 2": 1,
            "Key 3": "1.0",
            "Key 4": 2.0,
            "Key 5": 2,
            "Key 6": "2",
            "Key 7": 2,
            "Key 8": 2.0,
            "Key 9": 0,
            "Key 10": None,
            "Key 11": 2,
            "Key 12": 2.013,
        }

        docu5 = {
            "Key 1": False,
            "Key 2": False,
            "Key 3": True,
            "Key 4": True,
            "Key 5": "F",
            "Key 6": "t",
            "Key 7": "y",
            "Key 8": "n",
            "Key 9": "true",
            "Key 10": "T",
            "Key 11": "abcd",
        }
        schema5 = {
            "Key 1": "bool", 
            "Key 3": "bool", 
            "Key 5": "bool", 
            "Key 6": "bool", 
            "Key 7": "bool", 
            "Key 8": "bool", 
            "Key 9": "bool", 
            "Key 10": "bool", 
            "Key 11": "bool", 
        }
        check5 = {
            "Key 1": False,
            "Key 2": "False",
            "Key 3": True,
            "Key 4": "True",
            "Key 5": False,
            "Key 6": True,
            "Key 7": True,
            "Key 8": False,
            "Key 9": True,
            "Key 10": True,
            "Key 11": False,
        }

        docu6 = {}
        schema6 = {
            'Key 1': 'int',
            'Key 2': 'float',
            'Key 3': 'bool',
            'Key 4': 'complex',
            'Key 5': 'datetime',
            'Key 6': 'str',
        }
        check6 = {
            'Key 1': 0,
            'Key 2': None,
            'Key 3': False,
            'Key 4': None,
            'Key 5': pd.Timestamp(None),
            'Key 6': None,
        }
        
        docus = [docu1, docu2, docu3, docu4, docu5, docu6]
        schemas = [schema1, schema2, schema3, schema4, schema5, schema6]
        checks = [check1, check2, check3, check4, check5, check6]

        for i in range(len(docus)):
            self.assertEqual(util.validate_or_convert(docus[i], schemas[i], pytz.timezone("Asia/Kolkata")), checks[i])

if __name__ == '__main__':
    unittest.main()

