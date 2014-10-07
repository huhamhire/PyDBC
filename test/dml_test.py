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

from data.dml import Where
from data.dialect import Dialect
from data.constants import ValueTypes, CompareTypes, RelationTypes


class WhereSQLTestCase(unittest.TestCase):
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
        self.columns = self.TestColumns()

    def tearDown(self):
        self.where = None

    def test_one_column_int(self):
        self.where.add_column(**self.columns.basic_int)
        expected = " WHERE alpha=1"
        result = self.where.to_sql(self.dialect)
        self.assertEqual(expected, result)

    def test_one_column_int_compare(self):
        self.where.add_column(**self.columns.basic_int_less_equal)
        expected = " WHERE alpha<=1"
        result = self.where.to_sql(self.dialect)
        self.assertEqual(expected, result)

    def test_one_column_str(self):
        self.where.add_column(**self.columns.basic_str)
        expected = " WHERE foo='bar'"
        result = self.where.to_sql(self.dialect)
        self.assertEqual(expected, result)

    def test_two_columns_mixed(self):
        self.where.add_column(**self.columns.basic_int_less_equal)
        self.where.add_column(**self.columns.basic_str)
        expected = " WHERE alpha<=1 AND foo='bar'"
        result = self.where.to_sql(self.dialect)
        self.assertEqual(expected, result)

    def test_two_columns_relation(self):
        self.where.add_column(**self.columns.basic_int)
        self.where.add_column(**self.columns.basic_str_relation_or)
        expected = " WHERE alpha=1 OR foo='bar'"
        result = self.where.to_sql(self.dialect)
        self.assertEqual(expected, result)

if __name__ == "__main__":
    where_test_suite = unittest.makeSuite(WhereSQLTestCase, "test")
    unittest.TextTestRunner(verbosity=1).run(where_test_suite)
