import pytest
from functools import reduce
from pyspark.sql.functions import col, isnan, concat
from data_transformation import *


class TestClass:
    def test_handle_na(self, spark_session):
        test_df = spark_session.createDataFrame(
            [
                (1, None, 'Harris', 'male', 'Illinois', None, 'legacy', '2009-07-18T17:59:18.150Z'),
                (2, 'Jerry', None, 'male', 'Louisiana', None, 'legacy', '2004-07-20T13:26:33.822Z'),
                (3, 'Howard', 'Austin', None, 'New Mexico', 'b6c7a9a0-306f-4d70-8a45-2b56bdcbfaf9',
                 'forex', '2004-07-20T13:26:33.822Z'),
                (4, 'Jimmy', 'Reid', 'male', None, None, 'legacy', '2004-07-20T13:26:33.822Z')
            ],
            ['customer_id', 'firstname', 'lastname', 'gender', 'state', 'account_number', 'type_of_account',
             'created_at']
        )
        new_df = handle_na(test_df, ['dropall', 'fillna', 'coalesce'],
                          ["firstname", "lastname", "gender", "state", "type_of_account"],
                         'unknown', "account_number", "customer_id", "type_of_account")
        # there is 0 null values across all columns
        assert reduce(lambda x, y: x + y, [new_df.select(c).where(isnan(c) |
                                                                  col(c).isNull()).count()
                                           for c in new_df.columns]) == 0
        # the 3 rows have legacy account type filled the account_number with value customer_id + type_of_account
        assert new_df.select('account_number').where(col('account_number')
                                                     == concat('customer_id', 'type_of_account')).count() == 3

    def test_transform_func(self, spark_session):
        test_df = spark_session.createDataFrame(
            [
                (1, 'Tracy', 'Harris', 'male', 'NY', '2b96d524-feda-4777-a0aa-483953bec723', 'forex', '2009-07-18T17:59:18.150Z'),
                (2, 'Jerry', 'Jensen', 'male', 'Louisia', '2legacy', 'legacy', '2004-07-20T13:26:33.822Z'),
                (3, 'Howard', 'Austin', 'male', 'new mexico', 'b6c7a9a0-306f-4d70-8a45-2b56bdcbfaf9',
                 'forex', '2004-07-20T13:26:33.822Z'),
                (4, 'Jimmy', 'Reid', 'male', 'TN', '9c123e12-b54c-453f-963d-5fb534ad1a2f', 'saving', '2004-07-20T13:26:33.822Z')
            ],
            ['customer_id', 'firstname', 'lastname', 'gender', 'state', 'account_number', 'type_of_account',
                 'created_at']
        )
        # transform state name
        new_df = transform_func(test_df, 'state')
        assert new_df.filter(col('state') == 'unknown').count() == 1
        assert [row[0] == 'New York' for row in new_df.select('state').where(col('customer_id') == 1).collect()]
        assert new_df.select('state').where(col('customer_id') == 4).collect()[0].asDict()['state'] == 'Tennessee'
        assert [row[0] == 'New Mexico' for row in new_df.select('state').where(col('customer_id') == 3).collect()]
        # transform account type
        new_df = transform_func(test_df, 'type_of_account')
        assert new_df.filter(col('type_of_account') == 'checking').count() == 1
        # create created_at_year column
        new_df = transform_func(test_df, 'created_at', 'created_at_year')
        assert new_df.select('created_at_year').where(col('customer_id') == 2).collect()[0][0] == '2004'

    def test_concat_columns(self, spark_session):
        test_df = spark_session.createDataFrame(
            [
                (1, 'Tracy', 'Harris', 'male', 'NY', '2b96d524-feda-4777-a0aa-483953bec723', 'forex',
                 '2009-07-18T17:59:18.150Z'),
                (2, 'Jerry', 'Jensen', 'male', 'Louisia', '2legacy', 'legacy', '2004-07-20T13:26:33.822Z'),
                (3, 'Howard', 'Austin', 'male', 'new mexico', 'b6c7a9a0-306f-4d70-8a45-2b56bdcbfaf9',
                 'forex', '2004-07-20T13:26:33.822Z'),
                (4, 'Jimmy', 'Reid', 'male', 'TN', '9c123e12-b54c-453f-963d-5fb534ad1a2f', 'saving',
                 '2004-07-20T13:26:33.822Z')
            ],
            ['customer_id', 'firstname', 'lastname', 'gender', 'state', 'account_number', 'type_of_account',
             'created_at']
        )
        new_df = concat_columns(test_df, 'name', ' ', False, 'firstname', 'lastname')
        assert new_df.select('name').filter(col('customer_id') == 1).collect()[0][0] == 'Tracy Harris'



