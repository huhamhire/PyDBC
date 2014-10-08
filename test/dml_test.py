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

from data.dml import Where, Having
from data.dialect import Dialect
from data.constants import (
    ValueTypes, CompareTypes, RelationTypes, AggregateFunctions)


class WhereTestCase(unittest.TestCase):
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

if __name__ == "__main__":
    where_test_suite = unittest.makeSuite(WhereTestCase, "test")
    having_test_suite = unittest.makeSuite(HavingTestCase, "test")
    dml_test = unittest.TestSuite((where_test_suite, having_test_suite))
    unittest.TextTestRunner(verbosity=1).run(dml_test)
