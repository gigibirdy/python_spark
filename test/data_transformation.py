# For test purposes only.
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.functions import *


def handle_na(df, types, fillna_columns=None, fillna_val=None, coal_column=None, *args):
    """
    args:
    df -- the dataframe
    types -- data type: list
    choose from:
    dropall
    dropany
    fillna
    coalesce
    fillna_columns -- data type: list
    fillna_val -- fill na with value
    coal_column -- coalesced into column
    *args: If coalesce is selected, a list of values that will be used to fill the NAs, seperated by ','.
    """
    for t in types:
        if t == 'dropall':
            df = df.dropna(how='all')
        elif t == 'dropany':
            df = df.dropna(how='any')
        elif t == 'fillna':
            df = df.fillna(value=fillna_val, subset=fillna_columns)
        elif t == 'coalesce':
            df = df.withColumn(coal_column, coalesce(coal_column, concat(*args)))
    return df


# wrapper
def udf_transform_func(func):
    udf_transform = udf(lambda x: func(x), StringType())
    return udf_transform


# convert legacy to checking
@udf_transform_func
def transform_accounts(x):
    if x == "legacy":
        x = "checking"
    return x


# convert abbreviated state name to full name
states = {'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California', 'CO': 'Colorado',
          'CT': 'Connecticut', 'DE': 'Delaware', 'DC': 'Washington, D.C.', 'FL': 'Florida', 'GA': 'Georgia',
          'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
          'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland', 'MA': 'Massachusetts',
          'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi', 'MO': 'Missouri', 'MT': 'Montana',
          'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico',
          'NY': 'New York', 'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
          'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina', 'SD': 'South Dakota',
          'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington',
          'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming'}


@udf_transform_func
def transform_states(x):
    if x in states:
        x = states[x]
    elif x.title() in states.values():
        x = x.title()
    elif x.title() not in states.values():
        x = 'unknown'
    return x


def transform_func(df, base_col, new_col=None):
    """
    args:
    df -- the dataframe
    base_col -- apply transformation to
    new_col -- create a new column and store the transformed data. Default is None.
    Transformed data will be stored in the base_col
    """
    if base_col == "state":
        df = df.withColumn(base_col, transform_states(col(base_col)))
    elif base_col == "type_of_account":
        df = df.withColumn(base_col, transform_accounts(col(base_col)))
    elif base_col == "created_at":
        df = df.withColumn(new_col, substring(col(base_col).cast(StringType()), 1, 4))
    return df


def concat_columns(df, new_col, opt_sep='', opt_drop=False, *args):
    """
    args:
    df -- the dataframe
    new_col -- new column name
    opt_sep -- delimiter
    opt_drop -- drop the original columns
    *args:  concatenated column names
    """

    if opt_drop:
        df = df.withColumn(new_col, concat_ws(opt_sep, *args)).drop(*args)
    else:
        df = df.withColumn(new_col, concat_ws(opt_sep, *args))
    return df



