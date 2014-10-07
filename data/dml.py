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


# =================
# DML base classes:
#   1. DMLBase
#   2. ClauseBase
# =================
class DMLBase(object):
    """
    Base class for creating DML statements.

    :ivar str _raw_sql: RAW SQL statement to be used after the clause keyword.
        If this property is set, the RAW statement would be used directly in the
        SQL clause without other details.
    """
    __metaclass__ = ABCMeta

    _raw_sql = None

    @abstractmethod
    def to_sql(self, dialect):
        """
        Convert statement object to a SQL statement.

        .. note:: This is an abstract method.

        :param dialect: SQL dialect to generate statements to work with
            different databases. This dialect should be an instance of
            :class:`Dialect` class.
        :type dialect: Dialect
        :return: A string of SQL statement.
        :rtype: str
        """
        pass

    def create_keyword(self):
        """
        Create the keyword string of current statement.

        :return: Keyword string of current statement. Default by an empty
            string.
        :rtype: str
        """
        return ""

    def set_raw_sql(self, sql):
        """
        Set the RAW SQL statement.

        :param sql: RAW SQL statement after the statement keyword.
        :type sql: str
        """
        self._raw_sql = sql

    def get_raw_sql(self):
        """
        Get the RAW SQL statement.

        :return: RAW SQL statement after the statement keyword.
        :rtype: str
        """
        return self._raw_sql


class ClauseBase(DMLBase):
    """
    Base class of SQL clause classes.

    :ivar list _columns: List of column attributes for a SQL statement clause to
        use.
    """
    __metaclass__ = ABCMeta

    _columns = []

    def __init__(self):
        super(ClauseBase, self).__init__()
        self._columns = []

    @abstractmethod
    def add_column(self, *args, **kwargs):
        """
        Add column attributes for a SQL statement clause to use.

        .. note:: This is an abstract method.
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

    def to_sql(self, dialect):
        """
        Convert clause object to be component of SQL statement.

        :param dialect: SQL dialect to generate statements to work with
            different databases. This dialect should be an instance of
            :class:`Dialect` class.
        :type dialect: Dialect
        :return: A string of SQL clause.
        :rtype: str
        """
        sql_buffer = []
        if self._raw_sql:
            # Use raw SQL statement if exists
            sql_buffer.append(self.create_keyword())
            sql_buffer.append(self._raw_sql)
        elif self.get_size() > 0:
            sql_buffer.append(self.create_keyword())
            for col in self._columns:
                # Add column SQL
                column = col.to_sql(dialect)
                if column is not None:
                    sql_buffer.append(column)
        return "".join(sql_buffer)

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


# ========================
# DML Exceptions:
#   1. NoneColumnNameError
# ========================
class NoneColumnNameError(ValueError):
    """
    The error raised if the column name is `None`.
    """
    def __init__(self):
        """
        Initialize NoneColumnNameError.
        """
        msg = "Column name must not be 'None'!"
        super(NoneColumnNameError, self).__init__(msg)


# =====================
# Basic SQL Components:
#   1. Table
#   2. Column
# =====================
class Table(DMLBase):
    _tables = []

    def to_sql(self, dialect):
        pass


class Column(DMLBase):
    """
    Create basic SQL component `COLUMN` in a SQL statement.

    .. note:: This class is subclass of :class:`DMLBase`.

    :ivar str name: Column name of target column.
    :ivar AggregateFunctions func: Aggregate function to filter records.
    :ivar object value: Value for target column.
    :ivar ValueTypes type: Column data type of target column.
    :ivar CompareTypes compare: Type of comparison between record value and
        target value.
    :ivar RelationTypes relation: Type of relation between different criterion.
    :ivar str alias: Temporarily SQL alias name for the target column
    :ivar bool asc: Sort the records of target column in ascending order or in a
        descending order.
    :ivar bool is_first: A boolean indicating if current column is the first
        column in a column list.
    """
    name = ""
    func = None
    value = None
    type = None
    compare = None
    relation = None
    alias = None
    asc = True
    is_first = True

    def __init__(self, name, is_first):
        """
        Initialize a `Column` object.

        :param name: Column name of target column.
        :type name: str
        :param is_first: A boolean indicating if current column is the first
            column in a column list.
        :type is_first: bool
        :raises NoneColumnNameError: If column name is `None`.
        """
        if name is not None:
            self.name = name
            self.is_first = is_first
        else:
            raise NoneColumnNameError

    def to_sql(self, dialect):
        """
        Convert column object to be component of SQL statement.

        :param dialect: SQL dialect to generate statements to work with
            different databases. This dialect should be an instance of
            :class:`Dialect` class.
        :type dialect: Dialect
        :return: A string of SQL clause.
        :rtype: str
        """
        col_buffer = []
        # Ignore column values with " or '
        if self.value is not None and self.type == ValueTypes.STRING:
            if "'" in self.value or "\"" in self.value:
                return None
        if not self.is_first:
            if self.relation is not None:
                # Add relation key word
                col_buffer.append(SQLUtils.get_sql_relation(self.relation))
            else:
                # Add comma
                col_buffer.append(", ")
        if self.func is None:
            # Add column name
            col_buffer.append(dialect.column2sql(self.name))
            # Add order type
            if not self.asc:
                col_buffer.append(SQLUtils.get_sql_order_type(self.asc))
        else:
            # Add aggregate function
            func = SQLUtils.get_aggr_func_with_column(
                self.func, dialect.column2sql(self.name))
            col_buffer.append(func)
        # Add operator & value
        if self.compare is not None and self.value is not None:
            op, val = SQLUtils.get_operator_with_value(
                self.compare, self.value)
            col_buffer.append(op)
            if self.type == ValueTypes.STRING:
                val = "".join(["'", val, "'"])
            col_buffer.append(str(val))
        # Add alias
        if self.alias is not None:
            col_buffer.append(SQLUtils.get_sql_as_keyword())
            col_buffer.append(self.alias)
        return "".join(col_buffer)


# ====================
# Generic SQL clauses:
#   1. Where
#   2. Having
#   3. GroupBy
#   4. OrderBy
# ====================
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
        :raises NoneColumnNameError: If column name is `None`.

        .. seealso:: :class:`~.constants.ValueTypes`,
            :class:`~.constants.CompareTypes` and
            :class:`~.constants.RelationTypes`.
        """
        if column_name is not None:
            is_first = self.get_size() == 0
            col = Column(column_name, is_first)
            col.value = column_value
            col.type = column_type
            col.compare = compare_type
            col.relation = relation_type
            self._columns.append(col)
        else:
            raise NoneColumnNameError

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
        :raises NoneColumnNameError: If column name is `None`.

        .. seealso:: :class:`~.constants.ValueTypes`,
            :class:`~.constants.CompareTypes` and
            :class:`~.constants.RelationTypes`.
        """
        if column_name is not None:
            is_first = self.get_size() == 0
            col = Column(column_name, is_first)
            col.func = aggr_func
            col.value = column_value
            col.type = column_type
            col.compare = compare_type
            col.relation = relation_type
            self._columns.append(col)
        else:
            raise NoneColumnNameError

    def create_keyword(self):
        """
        Create the keyword string of SQL `HAVING` clause.

        :return: Keyword string of SQL `HAVING` clause.
        :rtype: str
        """
        return " HAVING "


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
        :raises NoneColumnNameError: If column name is `None`.
        """
        if column_name is not None:
            is_first = self.get_size() == 0
            col = Column(column_name, is_first)
            self._columns.append(col)
        else:
            raise NoneColumnNameError

    def create_keyword(self):
        """
        Create the keyword string of SQL `GROUP BY` clause.

        :return: Keyword string of SQL `GROUP BY` clause.
        :rtype: str
        """
        return " GROUP BY "


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
        :raises NoneColumnNameError: If column name is `None`.
        """
        if column_name is not None:
            is_first = self.get_size() == 0
            col = Column(column_name, is_first)
            col.asc = asc
            self._columns.append(col)
        else:
            raise NoneColumnNameError

    def create_keyword(self):
        """
        Create the keyword string of SQL `ORDER BY` clause.

        :return: Keyword string of SQL `ORDER BY` clause.
        :rtype: str
        """
        return " ORDER BY "


# =======================
# Generic DML statements:
#   1. Select
# =======================
class Select(DMLBase):
    """
    Create SQL `SELECT` statement to select data from a database.

    :cvar bool _distinct: A boolean indicating whether to use `DISTINCT` keyword
        in the SQL `SELECT` statement or not.
    :cvar list _columns:

    :cvar Where _where: Object to create SQL `WHERE` clause to filter records.
    :cvar Having _having: Object to create SQL `HAVING` clause to filter records
        with aggregate functions.
    :cvar GroupBy _group_by: Object to create SQL `GROUP BY` clause to to group
        the result-set by one or more columns.
    :cvar OrderBy _order_by: Object to create SQL `ORDER BY` clause to sort the
        result-set by one or more columns.
    """
    _distinct = False
    _columns = []
    _table = None
    _where = None
    _having = None
    _group_by = None
    _order_by = None

    def add_column(self, column_name, aggr_func=None, alias=None):
        """
        Add column for selecting data from.

        :param column_name: Column name of target column to select.
        :type column_name: str
        :param aggr_func: Aggregate function for calculating records. Default by
            `None`.
        :type aggr_func: AggregateFunctions
        :param alias: Temporarily SQL alias name for the target column. Default
            by `None`.
        :type alias: str
        :raises NoneColumnNameError: If column name is `None`.
        """
        if column_name is not None:
            is_first = len(self._columns) == 0
            column = Column(column_name, is_first)
            column.func = aggr_func
            column.alias = alias
            self._columns.append(column)
        else:
            raise NoneColumnNameError

    def set_distinct(self, distinct):
        """
        Set whether to use `DISTINCT` keyword in the SQL `SELECT` statement or
        not.

        :param distinct: A boolean indicating whether to return only distinct
            (different) values or not.
        :type distinct: bool
        """
        self._distinct = distinct

    def set_where(self, where):
        """
        Set DML `Where` object for creating SQL `SELECT` statement.

        :param where: Object to create SQL `WHERE` clause to filter records.
        :type where: Where
        """
        self._where = where

    def get_where(self):
        """
        Get DML `Where` object of SQL `SELECT` statement.

        :return: Object to create SQL `WHERE` clause to filter records.
        :rtype: Where
        """
        return self._where

    def set_having(self, having):
        """
        Set DML `Having` object for creating SQL `SELECT` statement.

        :param having: Object to create SQL `ORDER BY` clause to sort the
            result-set by one or more columns.
        :type having: Having
        """
        self._having = having

    def get_having(self):
        """
        Get DML `Having` object of SQL `SELECT` statement.

        :return: Object to create SQL `ORDER BY` clause to sort the result-set
            by one or more columns.
        :rtype: Having
        """
        return self._having

    def set_group_by(self, group_by):
        """
        Set DML `GroupBy` object for creating SQL `SELECT` statement.

        :param group_by: Object to create SQL `GROUP BY` clause to to group the
            result-set by one or more columns.
        :type group_by: GroupBy
        """
        self._group_by = group_by

    def get_group_by(self):
        """
        Get DML `GroupBy` object of SQL `SELECT` statement.

        :return: Object to create SQL `GROUP BY` clause to to group the
            result-set by one or more columns.
        :rtype: GroupBy
        """
        return self._group_by

    def set_order_by(self, order_by):
        """
        Set DML `OrderBy` object for creating SQL `SELECT` statement.

        :param order_by: Object to create SQL `ORDER BY` clause to sort the
            result-set by one or more columns.
        :type order_by: OrderBy
        """
        self._order_by = order_by

    def get_order_by(self):
        """
        Get DML `OrderBy` object of SQL `SELECT` statement.

        :return: Object to create SQL `ORDER BY` clause to sort the  result-set
            by one or more columns.
        :rtype: OrderBy
        """
        return self._order_by

    def create_keyword(self):
        """
        Create the keyword string of SQL `SELECT` statement.

        :return: Keyword string of SQL `SELECT` statement.
        :rtype: str
        """
        if self._distinct:
            return "SELECT DISTINCT "
        else:
            return "SELECT "

    def to_sql(self, dialect):
        """
        Convert `Select` object to be a SQL `SELECT` statement.

        :param dialect: SQL dialect to generate statements to work with
            different databases. This dialect should be an instance of
            :class:`Dialect` class.
        :type dialect: Dialect
        :return: A string of SQL `SELECT` statement.
        :rtype: str
        """
        sql_buffer = []
        if self._raw_sql:
            # Use raw SQL statement if exists
            sql_buffer.append(self.create_keyword())
            sql_buffer.append(self._raw_sql)
        elif len(self._columns) > 0:
            sql_buffer.append(self.create_keyword())
            for col in self._columns:
                # Add column SQL
                sql_buffer.append(col.to_sql(dialect))
            if self._where:
                sql_buffer.append(self._where.to_sql(dialect))
            if self._having:
                sql_buffer.append(self._having.to_sql(dialect))
            if self._group_by:
                sql_buffer.append(self._group_by.to_sql(dialect))
            if self._order_by:
                sql_buffer.append(self._order_by.to_sql(dialect))
        return "".join(sql_buffer)
