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
from .dialect import Dialect
from .sqlutils import SQLUtils


class Where(object):
    """
     Create SQL `WHERE` clause to filter records.

    :ivar list _columns: Column list to define the criterion.
    """
    _columns = []

    def add_column(self, column_name, column_value,
                   column_type=ValueTypes.STRING,
                   compare_type=CompareTypes.EQUALS,
                   relation_type=RelationTypes.AND):
        """
        Add a column to extract only those records that fulfill a specified
        criterion.

        :param column_name: Column name of target column to filter records.
        :type column_name: str
        :param column_value: Value for target column to filter records.
        :type column_value: object
        :param column_type: Column data type of target column.
        :type column_type: ValueTypes
        :param compare_type: Type of comparison between record value and target
            value.
        :type compare_type: CompareTypes
        :param relation_type: Type of relation between different criterion.
        :type relation_type: RelationTypes

        .. seealso:: :class:`~.constants.ValueTypes`,
            :class:`~.constants.CompareTypes` and
            :class:`~.constants.RelationTypes`.
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
        Get number of the columns for criterion of `WHERE` clause.

        :return: Number of the columns
        :rtype: int
        """
        return len(self._columns)

    def clear(self):
        """
        Reset columns for `WHERE` clause.
        """
        self._columns = []

    def to_sql(self, dialect):
        """
        Convert `Where` object to be component of SQL statement.

        :param dialect: SQL dialect to generate statements to work with
            different databases. This dialect should be an instance of
            :class:`Dialect` class.
        :type dialect: Dialect
        :return: A string of SQL `WHERE` clause.
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
    Create SQL `ORDER BY` clause to sort the result-set by one or more columns.

    :ivar list _columns: Column list in the result-set to be sorted order by.
    """
    _columns = []

    def add_column(self, column_name, asc=True):
        """
        Add a column for sorting order.

        :param column_name: Column name of the target column to be ordered by.
        :type column_name: str
        :param asc: Sort the records in ascending order or in a descending
            order. Default by "True(ascending)".
        :type asc: bool
        """
        if column_name is not None:
            self._columns.append({
                "name": column_name,
                "asc": asc
            })

    def get_size(self):
        """
        Get number of the columns for `ORDER BY` clause.

        :return: Number of the columns.
        :rtype: int
        """
        return len(self._columns)

    def clear(self):
        """
        Reset columns for `ORDER BY` clause.
        """
        self._columns = []

    def to_sql(self, dialect):
        """
        Convert `OrderBy` object to be component of SQL statement.

        :param dialect: SQL dialect to generate statements to work with
            different databases. This dialect should be an instance of
            :class:`Dialect` class.
        :type dialect: Dialect
        :return: A string of SQL `ORDER BY` clause.
        :rtype: str
        """
        sql_buffer = []
        if self.get_size() > 0:
            sql_buffer.append(" ORDER BY ")
            for column in self._columns:
                # Add column name
                sql_buffer.append(dialect.column2sql(column["name"]))
                # Add order type
                sql_buffer.append(SQLUtils.get_sql_order_type(column["asc"]))
                # Add comma
                sql_buffer.append(", ")
            # remove last comma
            sql_buffer.pop()
        return "".join(sql_buffer)


class Select(object):
    _columns = []
    _order = None
    _where = None

    def set_order(self, order):
        self._order = order

    def get_order(self):
        return self._order

    def set_where(self, where):
        self._where = where

    def get_where(self):
        return self._where

    def add_column(self, column_name):
        if column_name is not None:
            self._columns.append(column_name)
