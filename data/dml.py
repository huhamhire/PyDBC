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

from abc import ABCMeta, abstractmethod

from .constants import (
    CompareTypes, ValueTypes, RelationTypes, AggregateFunctions)
from .dialect import Dialect
from .sqlutils import SQLUtils


###################
# Basic DML Classes
###################


class ClauseBase(object):
    """
    Base class of SQL clause classes.

    :ivar str _raw_sql: RAW SQL statement to be used after the clause keyword.
        If this property is set, the RAW statement would be used directly in the
        SQL clause without other details.
    :ivar list _columns: List of column attributes for a SQL statement clause to
        use.
    """
    __metaclass__ = ABCMeta

    _raw_sql = None
    _columns = []

    @abstractmethod
    def add_column(self, *args, **kwargs):
        """
        Add column attributes for a SQL statement clause to use.

        .. note:: This is an abstract method.
        """
        pass

    @abstractmethod
    def to_sql(self, dialect):
        """
        Convert clause object to be component of SQL statement.

        .. note:: This is an abstract method.

        :param dialect: SQL dialect to generate statements to work with
            different databases. This dialect should be an instance of
            :class:`Dialect` class.
        :type dialect: Dialect
        :return: A string of SQL clause.
        :rtype: str
        """
        pass

    @abstractmethod
    def create_keyword(self):
        """
        Create the keyword string of current clause.

        .. note:: This is an abstract method.

        :return: Keyword string of current clause.
        :rtype: str
        """
        pass

    def set_raw_sql(self, sql):
        """
        Set the RAW SQL statement.

        :param sql: RAW SQL statement after the clause keyword.
        :type sql: str
        """
        self._raw_sql = sql

    def get_raw_sql(self):
        """
        Get the RAW SQL statement.

        :return: RAW SQL statement after the clause keyword.
        :rtype: str
        """
        return self._raw_sql

    def get_size(self):
        """
        Get number of the columns in current clause.

        :return: Number of the columns.
        :rtype: int
        """
        return len(self._columns)

    def clear(self):
        """
        Reset current clause.
        """
        self._raw_sql = None
        self._columns = []


#####################
# Generic SQL Clauses
#####################


class Where(ClauseBase):
    """
    Create SQL `WHERE` clause to filter records.

    .. note:: This class is subclass of :class:`ClauseBase`.
    """
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
        if self._raw_sql:
            # Use raw SQL statement if exists
            sql_buffer.append(self.create_keyword())
            sql_buffer.append(self._raw_sql)
        elif self.get_size() > 0:
            sql_buffer.append(self.create_keyword())
            for column in self._columns:
                column_buffer = []
                # Ignore column values with " or '
                if "'" in column["value"] or "\"" in column["value"]:
                    continue
                # Add relation key word
                if need_relation:
                    relation = SQLUtils.get_sql_relation(column["relation"])
                    column_buffer.append(relation)
                else:
                    need_relation = True
                # Add column name
                column_buffer.append(dialect.column2sql(column["name"]))
                # Add operator & value
                value = column["value"]
                op, val = SQLUtils.get_operator_with_value(
                    column["compare"], value)
                column_buffer.append(op)
                if column["type"] == ValueTypes.STRING:
                    val = "".join(["'", val, "'"])
                column_buffer.append(val)
                sql_buffer.append("".join(column_buffer))
        return "".join(sql_buffer)

    def create_keyword(self):
        """
        Create the keyword string of SQL `WHERE` clause.

        :return: Keyword string of SQL `WHERE` clause.
        :rtype: str
        """
        return " WHERE "


class Having(ClauseBase):
    """
    Create SQL `HAVING` clause to filter records with aggregate functions.

    .. note:: This class is subclass of :class:`ClauseBase`.
    """
    def add_column(self, column_name, aggr_func, column_value,
                   column_type=ValueTypes.STRING,
                   compare_type=CompareTypes.EQUALS,
                   relation_type=RelationTypes.AND):
        """
        Add a column to extract only those records that fulfill a specified
        criterion.

        :param column_name: Column name of target column to filter records.
        :type column_name: str
        :param aggr_func: Aggregate function to filter records.
        :type aggr_func: AggregateFunctions
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
                "func": aggr_func,
                "value": column_value,
                "type": column_type,
                "compare": compare_type,
                "relation": relation_type
            })

    def to_sql(self, dialect):
        """
        Convert `Having` object to be component of SQL statement.

        :param dialect: SQL dialect to generate statements to work with
            different databases. This dialect should be an instance of
            :class:`Dialect` class.
        :type dialect: Dialect
        :return: A string of SQL `HAVING` clause.
        :rtype: str
        """
        sql_buffer = []
        need_relation = False
        if self._raw_sql:
            # Use raw SQL statement if exists
            sql_buffer.append(self.create_keyword())
            sql_buffer.append(self._raw_sql)
        elif self.get_size() > 0:
            sql_buffer.append(self.create_keyword())
            for column in self._columns:
                column_buffer = []
                # Ignore column values with " or '
                if "'" in column["value"] or "\"" in column["value"]:
                    continue
                # Add relation key word
                if need_relation:
                    relation = SQLUtils.get_sql_relation(column["relation"])
                    column_buffer.append(relation)
                else:
                    need_relation = True
                # Add aggregate function
                func = SQLUtils.get_aggr_func_with_column(
                    column["func"], dialect.column2sql(column["name"]))
                column_buffer.append(func)
                # Add operator & value
                value = column["value"]
                op, val = SQLUtils.get_operator_with_value(
                    column["compare"], value)
                column_buffer.append(op)
                if column["type"] == ValueTypes.STRING:
                    val = "".join(["'", val, "'"])
                column_buffer.append(val)
                sql_buffer.append("".join(column_buffer))
        return "".join(sql_buffer)

    def create_keyword(self):
        """
        Create the keyword string of SQL `HAVING` clause.

        :return: Keyword string of SQL `HAVING` clause.
        :rtype: str
        """
        return " HAVING "


class OrderBy(ClauseBase):
    """
    Create SQL `ORDER BY` clause to sort the result-set by one or more columns.

    .. note:: This class is subclass of :class:`ClauseBase`.
    """
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
        if self._raw_sql:
            # Use raw SQL statement if exists
            sql_buffer.append(self.create_keyword())
            sql_buffer.append(self._raw_sql)
        elif self.get_size() > 0:
            sql_buffer.append(self.create_keyword())
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

    def create_keyword(self):
        """
        Create the keyword string of SQL `ORDER BY` clause.

        :return: Keyword string of SQL `ORDER BY` clause.
        :rtype: str
        """
        return " ORDER BY "


class GroupBy(ClauseBase):
    """
    Create SQL `GROUP BY` clause to to group the result-set by one or more
    columns.

    .. note:: This class is subclass of :class:`ClauseBase`.
    """
    def add_column(self, column_name):
        """
        Add column to group the result.

        :param column_name: Column name of target column to group.
        :type column_name: str
        """
        self._columns.append(column_name)

    def to_sql(self, dialect):
        """
        Convert `GroupBy` object to be component of SQL statement.

        :param dialect: SQL dialect to generate statements to work with
            different databases. This dialect should be an instance of
            :class:`Dialect` class.
        :type dialect: Dialect
        :return: A string of SQL `GroupBy` clause.
        :rtype: str
        """
        sql_buffer = []
        need_comma = False
        if self._raw_sql:
            # Use raw SQL statement if exists
            sql_buffer.append(self.create_keyword())
            sql_buffer.append(self._raw_sql)
        elif self.get_size() > 0:
            sql_buffer.append(self.create_keyword())
            for column_name in self._columns:
                # Add comma
                if need_comma:
                    sql_buffer.append(", ")
                else:
                    need_comma = True
                # Add column name
                sql_buffer.append(dialect.column2sql(column_name))
        return "".join(sql_buffer)

    def create_keyword(self):
        """
        Create the keyword string of SQL `GROUP BY` clause.

        :return: Keyword string of SQL `GROUP BY` clause.
        :rtype: str
        """
        return " GROUP BY "


########################
# Generic DML statements
########################


class Select(object):
    _columns = []
    _where = None
    _order_by = None

    def set_where(self, where):
        self._where = where

    def get_where(self):
        return self._where

    def set_order_by(self, order_by):
        self._order_by = order_by

    def get_order_by(self):
        return self._order_by

    def add_column(self, column_name):
        if column_name is not None:
            self._columns.append(column_name)
