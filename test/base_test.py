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

import unittest

from pydbc import SQLUtils
from pydbc import (
    CompareTypes, RelationTypes, AggregateFunctions, JoinTypes)


class SQLUtilityTest(unittest.TestCase):
    """
    Unittest for basic SQL utilities.
    """
    def setUp(self):
        self.test_value = "test"
        self.column_name = "test"

    def tearDown(self):
        self.test_value = None
        self.column_name = None

    def test_base_operator(self):
        # =
        op, v = SQLUtils.get_operator_with_value(
            CompareTypes.EQUALS, self.test_value)
        self.assertEqual(op, "=")
        # !=
        op, v = SQLUtils.get_operator_with_value(
            CompareTypes.NOT_EQUAL, self.test_value)
        self.assertEqual(op, "!=")
        # >
        op, v = SQLUtils.get_operator_with_value(
            CompareTypes.GREATER_THAN, self.test_value)
        self.assertEqual(op, ">")
        # >=
        op, v = SQLUtils.get_operator_with_value(
            CompareTypes.GREATER_THAN_OR_EQUAL, self.test_value)
        self.assertEqual(op, ">=")
        # <
        op, v = SQLUtils.get_operator_with_value(
            CompareTypes.LESS_THAN, self.test_value)
        self.assertEqual(op, "<")
        # <=
        op, v = SQLUtils.get_operator_with_value(
            CompareTypes.LESS_THAN_OR_EQUAL, self.test_value)
        self.assertEqual(op, "<=")

    def test_like_operator_with_value(self):
        # LIKE
        op, v = SQLUtils.get_operator_with_value(
            CompareTypes.BEGINS_WITH, self.test_value)
        self.assertEqual(op, " LIKE ")
        self.assertEqual(v, self.test_value + "%")
        # NOT LIKE
        op, v = SQLUtils.get_operator_with_value(
            CompareTypes.NOT_BEGIN_WITH, self.test_value)
        self.assertEqual(op, " NOT LIKE ")
        self.assertEqual(v, self.test_value + "%")
        # END WITH
        op, v = SQLUtils.get_operator_with_value(
            CompareTypes.ENDS_WITH, self.test_value)
        self.assertEqual(op, " LIKE ")
        self.assertEqual(v, "%" + self.test_value)
        # NOT END WITH
        op, v = SQLUtils.get_operator_with_value(
            CompareTypes.NOT_END_WITH, self.test_value)
        self.assertEqual(op, " NOT LIKE ")
        self.assertEqual(v, "%" + self.test_value)
        # CONTAINS
        op, v = SQLUtils.get_operator_with_value(
            CompareTypes.CONTAINS, self.test_value)
        self.assertEqual(op, " LIKE ")
        self.assertEqual(v, "%" + self.test_value + "%")
        # NOT CONTAIN
        op, v = SQLUtils.get_operator_with_value(
            CompareTypes.NOT_CONTAIN, self.test_value)
        self.assertEqual(op, " NOT LIKE ")
        self.assertEqual(v, "%" + self.test_value + "%")
        # IN
        op, v = SQLUtils.get_operator_with_value(
            CompareTypes.IN, self.test_value)
        self.assertEqual(op, " IN ")
        # NOT IN
        op, v = SQLUtils.get_operator_with_value(
            CompareTypes.NOT_IN, self.test_value)
        self.assertEqual(op, " NOT IN ")

    def test_null_operator_with_value(self):
        # IS NULL
        op, v = SQLUtils.get_operator_with_value(
            CompareTypes.NULL, self.test_value)
        self.assertEqual(op, " IS NULL")
        self.assertEqual(v, "")
        # IS NOT NULL
        op, v = SQLUtils.get_operator_with_value(
            CompareTypes.NOT_NULL, self.test_value)
        self.assertEqual(op, " IS NOT NULL")
        self.assertEqual(v, "")
        # UNKNOWN
        op, v = SQLUtils.get_operator_with_value(
            -1, self.test_value)
        self.assertEqual(op, "=")

    def test_relation(self):
        # AND
        rel = SQLUtils.get_sql_relation(RelationTypes.AND)
        self.assertEqual(rel, " AND ")
        # OR
        rel = SQLUtils.get_sql_relation(RelationTypes.OR)
        self.assertEqual(rel, " OR ")

    def test_order(self):
        # DESC
        order = SQLUtils.get_sql_order_type(False)
        self.assertEqual(order, " DESC")
        # ASC
        order = SQLUtils.get_sql_order_type(True)
        self.assertEqual(order, " ASC")

    def test_join(self):
        # INNER JOIN
        join = SQLUtils.get_sql_join_operator(JoinTypes.INNER_JOIN)
        self.assertEqual(join, " INNER JOIN ")
        # RIGHT JOIN
        join = SQLUtils.get_sql_join_operator(JoinTypes.RIGHT_JOIN)
        self.assertEqual(join, " RIGHT JOIN ")
        # LEFT JOIN
        join = SQLUtils.get_sql_join_operator(JoinTypes.LEFT_JOIN)
        self.assertEqual(join, " LEFT JOIN ")
        # FULL JOIN
        join = SQLUtils.get_sql_join_operator(JoinTypes.FULL_JOIN)
        self.assertEqual(join, " FULL JOIN ")
        # UNKNOWN
        join = SQLUtils.get_sql_join_operator(-1)
        self.assertEqual(join, None)

    def test_aggr(self):
        # AVG
        aggr = SQLUtils.get_aggr_func_with_column(
            AggregateFunctions.AVG, self.column_name)
        self.assertEqual(aggr, "AVG(" + self.column_name + ")")
        # COUNT
        aggr = SQLUtils.get_aggr_func_with_column(
            AggregateFunctions.COUNT, self.column_name)
        self.assertEqual(aggr, "COUNT(" + self.column_name + ")")
        # MAX
        aggr = SQLUtils.get_aggr_func_with_column(
            AggregateFunctions.MAX, self.column_name)
        self.assertEqual(aggr, "MAX(" + self.column_name + ")")
        # MIN
        aggr = SQLUtils.get_aggr_func_with_column(
            AggregateFunctions.MIN, self.column_name)
        self.assertEqual(aggr, "MIN(" + self.column_name + ")")
        # SUM
        aggr = SQLUtils.get_aggr_func_with_column(
            AggregateFunctions.SUM, self.column_name)
        self.assertEqual(aggr, "SUM(" + self.column_name + ")")
        # UNKNOWN
        aggr = SQLUtils.get_aggr_func_with_column(-1, self.column_name)
        self.assertEqual(aggr, None)

    def test_key_words(self):
        # AS
        self.assertEqual(SQLUtils.get_sql_as_keyword(), " AS ")
        # *
        self.assertEqual(SQLUtils.get_sql_all_columns(), "*")
        # FROM
        self.assertEqual(SQLUtils.get_sql_from_keyword(), " FROM ")


def base_test_suite():
    util_test = unittest.makeSuite(SQLUtilityTest, "test")
    base_test = unittest.TestSuite(util_test)
    return base_test
