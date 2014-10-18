#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyleft (C) 2014 - huhamhire <me@huhamhire.com>
# =============================================================================
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
# =============================================================================

__author__ = "huhamhire <me@huhamhire.com>"

import re
import unittest

from data.dml import (
    Column, JoinConditions, Where, Having, GroupBy, OrderBy,
    UnsupportedJoinTypeError)
from data.dialect import Dialect
from data.constants import (
    ValueTypes, CompareTypes, RelationTypes, AggregateFunctions)


# ========================================
# Unit tests for auxiliary DML components:
#   1. JoinConditionsTestCase
# ========================================
class JoinConditionsTestCase(unittest.TestCase):
    """
    Unittest for generating table join conditions.
    """
    def setUp(self):
        self.condition = JoinConditions()
        self.dialect = Dialect()

    def tearDown(self):
        self.condition.clear()

    def test_simple_two_column(self):
        column_1 = Column("alpha")
        column_1.table = "foo"
        column_2 = Column("beta")
        column_2.table = "bar"
        self.condition.add_condition(column_1, column_2)
        expected = " ON foo.alpha=bar.beta"
        result = self.condition.to_sql(self.dialect)
        self.assertEqual(result, expected)

    def test_simple_one_column(self):
        column = Column("alpha")
        column.table = "foo"
        column.value = 10
        column.compare = CompareTypes.LESS_THAN
        self.condition.add_condition(column)
        expected = " ON foo.alpha<10"
        result = self.condition.to_sql(self.dialect)
        self.assertEqual(result, expected)

    def test_two_conditions(self):
        column_1 = Column("alpha")
        column_1.table = "foo"
        column_2 = Column("beta")
        column_2.table = "bar"
        column_3 = Column("alpha")
        column_3.table = "foo"
        column_3.value = 10
        column_3.compare = CompareTypes.LESS_THAN
        self.condition.add_condition(column_1, column_2)
        self.condition.add_condition(column_3)
        expected = " ON foo.alpha=bar.beta AND foo.alpha<10"
        result = self.condition.to_sql(self.dialect)
        self.assertEqual(result, expected)

    def test_one_column_error(self):
        flag = 0
        column = Column("alpha")
        column.table = "foo"
        try:
            self.condition.add_condition(column)
            self.condition.to_sql(self.dialect)
        except UnsupportedJoinTypeError, e:
            expected = "UnsupportedJoinTypeError"
            self.assertRaisesRegexp(e, re.compile(expected))
            flag += 1
        self.assertEqual(flag, 1)


# ===================================
# Unit tests for generic SQL clauses:
#   1. WhereTestCase
#   2. HavingTestCase
#   3. GroupByTestCase
#   4. OrderByTestCase
# ===================================
class WhereTestCase(unittest.TestCase):
    """
    Unittest for generating SQL `WHERE` clause.
    """
    class TestColumns(object):
        basic_int = {
            "column_name": "alpha",
            "column_value": 1,
            "column_type": ValueTypes.INTEGER
        }
        basic_int_less_equal = {
            "column_name": "alpha",
            "column_value": 1,
            "column_type": ValueTypes.INTEGER,
            "compare_type": CompareTypes.LESS_THAN_OR_EQUAL
        }
        basic_int_with_table = {
            "column_name": "alpha",
            "column_value": 1,
            "table_name": "foo",
            "column_type": ValueTypes.INTEGER
        }
        basic_str = {
            "column_name": "foo",
            "column_value": "bar",
            "column_type": ValueTypes.STRING
        }
        basic_str_relation_or = {
            "column_name": "foo",
            "column_value": "bar",
            "column_type": ValueTypes.STRING,
            "relation_type": RelationTypes.OR
        }

    def setUp(self):
        self.where = Where()
        self.dialect = Dialect()

    def tearDown(self):
        self.where.clear()

    def test_one_column_int(self):
        self.where.add_column(**self.TestColumns.basic_int)
        expected = " WHERE alpha=1"
        result = self.where.to_sql(self.dialect)
        self.assertEqual(result, expected)

    def test_one_column_int_compare(self):
        self.where.add_column(**self.TestColumns.basic_int_less_equal)
        expected = " WHERE alpha<=1"
        result = self.where.to_sql(self.dialect)
        self.assertEqual(result, expected)

    def test_one_column_int_with_table(self):
        self.where.add_column(**self.TestColumns.basic_int_with_table)
        expected = " WHERE foo.alpha=1"
        result = self.where.to_sql(self.dialect)
        self.assertEqual(result, expected)

    def test_one_column_str(self):
        self.where.add_column(**self.TestColumns.basic_str)
        expected = " WHERE foo='bar'"
        result = self.where.to_sql(self.dialect)
        self.assertEqual(result, expected)

    def test_two_columns_mixed(self):
        self.where.add_column(**self.TestColumns.basic_int_less_equal)
        self.where.add_column(**self.TestColumns.basic_str)
        expected = " WHERE alpha<=1 AND foo='bar'"
        result = self.where.to_sql(self.dialect)
        self.assertEqual(result, expected)

    def test_two_columns_relation(self):
        self.where.add_column(**self.TestColumns.basic_int)
        self.where.add_column(**self.TestColumns.basic_str_relation_or)
        expected = " WHERE alpha=1 OR foo='bar'"
        result = self.where.to_sql(self.dialect)
        self.assertEqual(result, expected)


class HavingTestCase(unittest.TestCase):
    """
    Unittest for generating SQL `HAVING` clause.
    """
    class TestColumns(object):
        basic_int_avg = {
            "column_name": "alpha",
            "aggr_func": AggregateFunctions.AVG,
            "column_value": 1,
            "column_type": ValueTypes.INTEGER
        }
        basic_int_less_equal = {
            "column_name": "beta",
            "aggr_func": AggregateFunctions.SUM,
            "column_value": 100,
            "column_type": ValueTypes.INTEGER,
            "compare_type": CompareTypes.LESS_THAN_OR_EQUAL,
            "relation_type": RelationTypes.OR
        }
        basic_int_no_func = {
            "column_name": "alpha",
            "column_value": 1,
            "column_type": ValueTypes.INTEGER
        }

    def setUp(self):
        self.having = Having()
        self.dialect = Dialect()

    def tearDown(self):
        self.having.clear()

    def test_one_column_int_avg(self):
        self.having.add_column(**self.TestColumns.basic_int_avg)
        expected = " HAVING AVG(alpha)=1"
        result = self.having.to_sql(self.dialect)
        self.assertEqual(result, expected)

    def test_two_column_int_sum_or(self):
        self.having.add_column(**self.TestColumns.basic_int_avg)
        self.having.add_column(**self.TestColumns.basic_int_less_equal)
        expected = " HAVING AVG(alpha)=1 OR SUM(beta)<=100"
        result = self.having.to_sql(self.dialect)
        self.assertEqual(result, expected)

    def test_one_column_no_func(self):
        flag = 0
        try:
            self.having.add_column(**self.TestColumns.basic_int_no_func)
        except TypeError, e:
            expected = "TypeError"
            self.assertRaisesRegexp(e, re.compile(expected))
            flag += 1
        self.assertEqual(flag, 1)


class GroupByTestCase(unittest.TestCase):
    """
    Unittest for generating SQL `GROUP BY` clause.
    """
    class TestColumns(object):
        column_alpha = {"column_name": "alpha"}
        column_beta = {"column_name": "beta"}

    def setUp(self):
        self.group_by = GroupBy()
        self.dialect = Dialect()

    def tearDown(self):
        self.group_by.clear()

    def test_one_column(self):
        self.group_by.add_column(**self.TestColumns.column_alpha)
        expected = " GROUP BY alpha"
        result = self.group_by.to_sql(self.dialect)
        self.assertEqual(result, expected)

    def test_two_columns(self):
        self.group_by.add_column(**self.TestColumns.column_alpha)
        self.group_by.add_column(**self.TestColumns.column_beta)
        expected = " GROUP BY alpha, beta"
        result = self.group_by.to_sql(self.dialect)
        self.assertEqual(result, expected)


class OrderByTestCase(unittest.TestCase):
    """
    Unittest for generating SQL `ORDER BY` clause.
    """
    class TestColumns(object):
        column_alpha_asc = {"column_name": "alpha"}
        column_beta_desc = {"column_name": "beta", "asc": False}

    def setUp(self):
        self.order_by = OrderBy()
        self.dialect = Dialect()

    def tearDown(self):
        self.order_by.clear()

    def test_one_column_asc(self):
        self.order_by.add_column(**self.TestColumns.column_alpha_asc)
        expected = " ORDER BY alpha"
        result = self.order_by.to_sql(self.dialect)
        self.assertEqual(result, expected)

    def test_two_columns_mix(self):
        self.order_by.add_column(**self.TestColumns.column_alpha_asc)
        self.order_by.add_column(**self.TestColumns.column_beta_desc)
        expected = " ORDER BY alpha, beta DESC"
        result = self.order_by.to_sql(self.dialect)
        self.assertEqual(result, expected)


def get_dml_test_suite():
    join_condition_test = unittest.makeSuite(JoinConditionsTestCase, "test")
    where_test = unittest.makeSuite(WhereTestCase, "test")
    having_test = unittest.makeSuite(HavingTestCase, "test")
    group_by_test = unittest.makeSuite(GroupByTestCase, "test")
    order_by_test = unittest.makeSuite(OrderByTestCase, "test")
    dml_test = unittest.TestSuite((
        where_test, having_test, group_by_test, order_by_test,
        join_condition_test
    ))
    return dml_test

if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=1).run(get_dml_test_suite())
