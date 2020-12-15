import decimal
import datetime
import inspect
import pyspark.sql.types as types
from dateutil import parser


_pyspark_types = [
    module[1] for module in inspect.getmembers(types, inspect.isclass) 
    if module[1].__module__ == 'pyspark.sql.types'
]


_type_mappings = {
    type(None): types.NullType,
    bool: types.BooleanType,
    int: types.LongType,
    float: types.DoubleType,
    str: types.StringType,
    bytearray: types.BinaryType,
    decimal.Decimal: types.DecimalType,
    datetime.date: types.DateType,
    datetime.datetime: types.TimestampType,
    datetime.time: types.TimestampType,
    bytes: types.BinaryType
}


class TypeInference:
    def _date_time_strategy(candidate: str):
        try:
            date = parser.parse(candidate)
            if date.time() == datetime.time(0):
                return datetime.date
            else:
                return datetime.datetime
        except:
            return None

    def infer(value):
        if TypeInference._date_time_strategy(value):
            return TypeInference._date_time_strategy(value)
        else:
            return type(value)


class StructParser:
    def get_pyspark_type_object(value):
        if inspect.isclass(value):
            return value()
        else:
            return _type_mappings.get(TypeInference.infer(value))()

    def _build_array(value):
        value_type = value[0]
        if isinstance(value_type, dict):
            return types.ArrayType(StructParser.parse(value_type))
        else:
            return types.ArrayType(value_type())

    def parse(data_dict):
        fields = []
        for key, value in data_dict.items():
            if isinstance(value, dict):
                fields.append(types.StructField(key, StructParser.parse(value)))
            elif isinstance(value, list):
                fields.append(types.StructField(key, StructParser._build_array(value)))
            else:
                fields.append(types.StructField(key, StructParser.get_pyspark_type_object(value)))
        
        return types.StructType(fields)