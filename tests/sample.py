import pyspark.sql.types as types

from structparser.parser import StructParser

STRUCT = {
    "kappa": "something",
    "kippo": 123,
    "keppo": types.IntegerType,
    "dict_kappa": {
        "inner_kappa": "2020-10-10",
        "list_kappa": [
            {
                "super_inner_kappa": "hi"
            }
        ]
    }
}


print(StructParser.parse(STRUCT))