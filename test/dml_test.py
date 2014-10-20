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
    Column, JoinedConditions, JoinedTables, Where, Having, GroupBy, OrderBy,
    Select, UnsupportedJoinTypeError)
from data.dialect import Dialect
from data.constants import (
    ValueTypes, CompareTypes, RelationTypes, AggregateFunctions, JoinTypes)


# ========================================
# Unit tests for auxiliary DML components:
#   1. JoinedConditionsTest
#   2. JoinedTableTest
# ========================================
class JoinedConditionsTest(unittest.TestCase):
    """
    Unittest for generating table join conditions.
    """
    def setUp(self):
        self.condition = JoinedConditions()
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


class JoinedTableTest(unittest.TestCase):
    """
    Unittest for generating joined table.
    """
    def setUp(self):
        self.tables = JoinedTables()
        self.dialect = Dialect()

    def tearDown(self):
        self.tables.clear()

    def test_single_table(self):
        self.tables.add_table("foo")
        expected = "foo"
        result = self.tables.to_sql(self.dialect)
        self.assertEqual(result, expected)

    def test_single_table_alias(self):
        self.tables.add_table("foo", "f")
        expected = "foo AS f"
        result = self.tables.to_sql(self.dialect)
        self.assertEqual(result, expected)

    def test_two_tables_join(self):
        self.tables.add_table("foo")
        condition = JoinedConditions()
        column_1 = Column("alpha")
        column_1.table = "foo"
        column_2 = Column("beta")
        column_2.table = "bar"
        condition.add_condition(column_1, column_2)
        self.tables.add_table(
            "bar", join=JoinTypes.LEFT_JOIN, condition=condition)
        expected = "foo LEFT JOIN bar ON foo.alpha=bar.beta"
        result = self.tables.to_sql(self.dialect)
        self.assertEqual(result, expected)

    def test_join_with_select(self):
        # Result table of a select statement
        select = Select()
        select_table = JoinedTables()
        select_table.add_table("foo")
        select.set_tables(select_table)
        select.add_column("alpha")
        select.add_column("beta")
        self.tables.add_table(select=select, alias="f")
        # Join condition
        condition = JoinedConditions()
        column_1 = Column("alpha")
        column_1.table = "f"
        column_2 = Column("alpha")
        column_2.table = "b"
        condition.add_condition(column_1, column_2)
        # Join tables
        self.tables.add_table("bar", "b", condition=condition)
        expected = "(SELECT alpha, beta FROM foo) AS f " \
                   "INNER JOIN bar AS b ON f.alpha=b.alpha"
        result = self.tables.to_sql(self.dialect)
        self.assertEqual(result, expected)


# ===================================
# Unit tests for generic SQL clauses:
#   1. WhereTest
#   2. HavingTest
#   3. GroupByTest
#   4. OrderByTest
# ===================================
class WhereTest(unittest.TestCase):
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


class HavingTest(unittest.TestCase):
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


class GroupByTest(unittest.TestCase):
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


class OrderByTest(unittest.TestCase):
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


# ======================================
# Unit tests for generic DML statements:
#   1. SelectTest
# ======================================
class SelectTest(unittest.TestCase):
    """
    Unittest for generating SQL `SELECT` statement.
    """
    def setUp(self):
        self.select = Select()
        self.dialect = Dialect()

    def tearDown(self):
        self.select.clear()

    def test_simple_select_all(self):
        table = JoinedTables()
        table.add_table("foo")
        self.select.set_tables(table)
        expected = "SELECT * FROM foo"
        result = self.select.to_sql(self.dialect)
        self.assertEqual(result, expected)

    def test_simple_select(self):
        table = JoinedTables()
        table.add_table("foo")
        self.select.set_tables(table)
        self.select.add_column("alpha", "foo")
        self.select.add_column("beta", "foo", AggregateFunctions.SUM, "sum_b")
        expected = "SELECT foo.alpha, SUM(foo.beta) AS sum_b FROM foo"
        result = self.select.to_sql(self.dialect)
        self.assertEqual(result, expected)

    def test_select_with_clauses(self):
        table = JoinedTables()
        table.add_table("foo")
        self.select.set_tables(table)
        self.select.add_column("alpha")
        self.select.add_column("beta")
        where = Where()
        where.add_column("alpha", 1, column_type=ValueTypes.INTEGER,
                         compare_type=CompareTypes.LESS_THAN_OR_EQUAL)
        self.select.set_where(where)
        expected = "SELECT alpha, beta FROM foo WHERE alpha<=1"
        result = self.select.to_sql(self.dialect)
        self.assertEqual(result, expected)
        # Add GroupBy and Having
        self.select.add_column("gamma")
        self.select.add_column("delta")
        group_by = GroupBy()
        group_by.add_column("gamma")
        having = Having()
        having.add_column("delta", AggregateFunctions.AVG, 1,
                          column_type=ValueTypes.INTEGER)
        self.select.set_group_by(group_by)
        self.select.set_having(having)
        expected = "SELECT alpha, beta, gamma, delta FROM foo " \
                   "WHERE alpha<=1 GROUP BY gamma HAVING AVG(delta)=1"
        result = self.select.to_sql(self.dialect)
        self.assertEqual(result, expected)
        # Add OrderBy
        order_by = OrderBy()
        order_by.add_column("gamma", False)
        self.select.set_order_by(order_by)
        expected = "SELECT alpha, beta, gamma, delta FROM foo " \
                   "WHERE alpha<=1 GROUP BY gamma HAVING AVG(delta)=1 " \
                   "ORDER BY gamma DESC"
        result = self.select.to_sql(self.dialect)
        self.assertEqual(result, expected)

    def test_select_joined_tables(self):
        table = JoinedTables()
        table.add_table("foo")
        condition = JoinedConditions()
        column_1 = Column("alpha")
        column_1.table = "foo"
        column_2 = Column("alpha")
        column_2.table = "bar"
        condition.add_condition(column_1, column_2)
        table.add_table("bar", join=JoinTypes.LEFT_JOIN, condition=condition)
        self.select.set_tables(table)
        expected = "SELECT * FROM foo LEFT JOIN bar ON foo.alpha=bar.alpha"
        result = self.select.to_sql(self.dialect)
        self.assertEqual(result, expected)
        # Add columns
        self.select.add_column("alpha", "foo")
        self.select.add_column("beta", "bar")
        expected = "SELECT foo.alpha, bar.beta FROM foo " \
                   "LEFT JOIN bar ON foo.alpha=bar.alpha"
        result = self.select.to_sql(self.dialect)
        self.assertEqual(result, expected)


def dml_test_suite():
    joined_table_test = unittest.makeSuite(JoinedTableTest, "test")
    joined_condition_test = unittest.makeSuite(JoinedConditionsTest, "test")
    where_test = unittest.makeSuite(WhereTest, "test")
    having_test = unittest.makeSuite(HavingTest, "test")
    group_by_test = unittest.makeSuite(GroupByTest, "test")
    order_by_test = unittest.makeSuite(OrderByTest, "test")
    select_test = unittest.makeSuite(SelectTest, "test")
    dml_test = unittest.TestSuite((
        where_test, having_test, group_by_test, order_by_test,
        joined_table_test, joined_condition_test, select_test
    ))
    return dml_test

if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=1).run(dml_test_suite())
