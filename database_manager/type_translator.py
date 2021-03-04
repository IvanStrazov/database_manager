# utf-8
# Python 3.7
# 2021-03-01


import numpy as np


str_type = {
    str: "VARCHAR2(1024)",
    object: "VARCHAR2(1024)"
}
int_format = {
    int: "NUMBER(20)",
    np.int8: "NUMBER(3)",
    np.int16: "NUMBER(5)",
    np.int32: "NUMBER(10)",
    np.int64: "NUMBER(20)"
}
float_format = {
    float: "FLOAT",
    np.float16: "FLOAT",
    np.float32: "FLOAT",
    np.float64: "FLOAT"

}
bool_format = {
    bool: "CHAR(1)",
    np.bool_: "CHAR(1)"
}

python2oracle = str_type.copy()
python2oracle.update(int_format)
python2oracle.update(float_format)
python2oracle.update(bool_format)
