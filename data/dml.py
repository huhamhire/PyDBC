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

from .constants import CompareTypes, ValueTypes, RelationTypes
from .sqlutils import SQLUtils


class Where(object):
    """
    :ivar list _columns:
    """
    _columns = []

    def add_column(self, column_name, column_value,
                   column_type=ValueTypes.STRING,
                   compare_type=CompareTypes.EQUALS,
                   relation_type=RelationTypes.AND):
        """
        :param column_name:
        :type column_name: str
        :param column_value:
        :type column_value: object
        :param column_type:
        :type column_type: ValueTypes
        :param compare_type:
        :type compare_type: CompareTypes
        :param relation_type:
        :type relation_type: RelationTypes
        """
        if column_name is not None:
            self._columns.append({
                "name": column_name,
                "value": column_value,
                "type": column_type,
                "compare": compare_type,
                "relation": relation_type
            })

    def get_size(self):
        """
        :return:
        :rtype: int
        """
        return len(self._columns)

    def clear(self):
        """
        """
        self._columns = []

    def to_sql(self, dialect):
        """
        :param dialect:
        :type dialect:
        :return:
        :rtype: str
        """
        sql_buffer = []
        need_relation = False
        if self.get_size() > 0:
            sql_buffer.append(" WHERE ")
            for column in self._columns:
                # Ignore column values with " or '
                if "'" in column["value"] or "\"" in column["value"]:
                    continue
                # Add relation key word
                if need_relation:
                    relation = SQLUtils.get_sql_relation(column["relation"])
                    sql_buffer.append(relation)
                else:
                    need_relation = True
                # Add column name
                sql_buffer.append(dialect.column2sql(column["name"]))
                # Add operator & value
                value = column["value"]
                op, val = SQLUtils.get_operator_with_value(
                    column["compare"], value)
                sql_buffer.append(op)
                if column["type"] == ValueTypes.STRING:
                    val = "".join(["'", val, "'"])
                sql_buffer.append(val)
        return "".join(sql_buffer)


class OrderBy(object):
    """
    Create column order method after the SQL `ORDER BY` Keyword.

    :ivar str _column: Column name of the target column to be ordered by.
    :ivar bool _asc: Sort the records in ascending order or in a descending
        order. Default by "True(ascending)".
    """
    _column = None
    _asc = True

    def __init__(self, column_name, asc=True):
        """
        Create the column order method.

        :param column_name: Column name of the target column to be ordered by.
        :type column_name: str
        :param asc: Sort the records in ascending order or in a descending
            order. Default by "True(ascending)".
        :type asc: bool
        """
        self._column = column_name
        self._asc = asc

    def set_column(self, column_name):
        """
        Set the name of current column.

        :param column_name: Column name of the target column to be ordered by.
        :type column_name: str
        """
        self._column = column_name

    def get_column(self):
        """
        Get the name of current column.

        :return: Column name of the target column to be ordered by.
        :rtype: str
        """
        return self._column

    def set_asc(self, asc=True):
        """
        Set sort order of current column.

        :param asc: Sort the records in ascending order or in a descending
            order. Default by "True(ascending)".
        :type asc: bool
        """
        self._asc = asc

    def get_asc(self):
        """
        Get set order of current column.

        :return: A boolean indicating the sort order of the records.
        :rtype: bool
        """
        return self._asc


class Select(object):
    _columns = []
    _orders = []
    _where = None

    def set_where(self, where):
        self._where = where

    def get_where(self):
        return self._where

    def add_column(self, column_name):
        if column_name is not None:
            self._columns.append(column_name)

    def add_order_by(self, column_name, asc=True):
        if column_name is not None:
            self._orders.append(OrderBy(column_name, asc))
