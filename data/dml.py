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
    CompareTypes, ValueTypes, RelationTypes, AggregateFunctions, JoinTypes)
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
#   2. NoneTableNameError
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


class NoneTableNameError(ValueError):
    """
    The error raised if the table name is `None`.
    """

    def __init__(self):
        """
        Initialize NoneTableNameError.
        """
        msg = "Table name must not be 'None'!"
        super(NoneTableNameError, self).__init__(msg)


class UnsupportedJoinTypeError(ValueError):
    """
    The error raised if the join type between tables is not supported.
    """

    def __init__(self):
        """
        Initialize UnsupportedJoinTypeError.
        """
        msg = "Unsupported table join type!"
        super(UnsupportedJoinTypeError, self).__init__(msg)


# =====================
# Basic SQL Components:
#   1. Table
#   2. Column
#   3. Condition
# =====================
class Table(DMLBase):
    """
    Create basic SQL component `COLUMN` in a SQL statement.

    .. note:: This class is subclass of :class:`DMLBase`.

    :ivar str name: Table name of target table.
    :ivar str alias: Temporarily SQL alias name for the target table or result
        table.
    :ivar Select select: Select object for creating a result table.
    :ivar JoinTypes join: The SQL join type for combining current table with the
        previous table.
    :ivar JoinedConditions condition: The condition for combining current table
        with the previous table.
    :ivar bool is_first: A boolean indicating if current column is the first
        column in a column list.

    .. seealso:: :class:`~.constants.JoinTypes`.
    """
    name = None
    alias = None
    select = None
    join = None
    condition = None
    is_first = True

    def __init__(self, name=None, alias=None, select=None, is_first=True):
        """
        Initialize a `Table` object.

        :param name: Table name of target table.
        :type name: str
        :param alias: Temporarily SQL alias name for the target table or result
            table.
        :type alias: str
        :param select: Select object for creating a result table.
        :type select: Select
        :param is_first: A boolean indicating if current table is the first
            table in a table list.
        :type is_first: bool
        :raises NoneTableNameError: If table name or both of select and alias
            name is `None`.
        """
        if name is not None:
            self.name = name
            if alias is not None:
                self.alias = alias
        elif select is not None and alias is not None:
            self.select = select
            self.alias = alias
        else:
            raise NoneTableNameError
        self.is_first = is_first

    def to_sql(self, dialect):
        """
        Convert table object to be component of SQL statement.

        :param dialect: SQL dialect to generate statements to work with
            different databases. This dialect should be an instance of
            :class:`Dialect` class.
        :type dialect: Dialect
        :return: A string of SQL clause.
        :rtype: str
        :raises UnsupportedJoinTypeError: If join type is not specified between
            different conditions.
        """
        if self._raw_sql:
            # Use raw SQL statement if exists
            return self._raw_sql
        table_buffer = []
        # Add JOIN operator
        if not self.is_first:
            join = SQLUtils.get_sql_join_operator(self.join)
            if join is not None:
                table_buffer.append(join)
            else:
                raise UnsupportedJoinTypeError
        # Add table
        if self.name is not None:
            table_buffer.append(dialect.table2sql(self.name))
            if self.alias is not None:
                table_buffer.append(SQLUtils.get_sql_as_keyword())
                table_buffer.append(dialect.table2sql(self.alias))
        elif self.select is not None and self.alias is not None:
            table_buffer.append("".join([
                "(", self.select.to_sql(dialect), ")",
                SQLUtils.get_sql_as_keyword(), dialect.table2sql(self.alias)
            ]))
        else:
            raise NoneTableNameError
        # Add join condition
        if not self.is_first:
            table_buffer.append(self.condition.to_sql(dialect))
        return "".join(table_buffer)


class Column(DMLBase):
    """
    Create basic SQL component `COLUMN` in a SQL statement.

    .. note:: This class is subclass of :class:`DMLBase`.

    :ivar str name: Column name of target column.
    :ivar str table: Table name or table alias to get column from.
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
    table = None
    func = None
    value = None
    type = None
    compare = None
    relation = None
    alias = None
    asc = True
    is_first = True

    def __init__(self, name, is_first=True):
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
        if self._raw_sql:
            # Use raw SQL statement if exists
            return self._raw_sql
        col_buffer = []
        # Create table name with or without table name
        if self.table is not None:
            col_name = "".join([
                dialect.table2sql(self.table), ".",
                dialect.column2sql(self.name)
            ])
        else:
            col_name = dialect.column2sql(self.name)
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
            col_buffer.append(col_name)
            # Add order type
            if not self.asc:
                col_buffer.append(SQLUtils.get_sql_order_type(self.asc))
        else:
            # Add aggregate function
            func = SQLUtils.get_aggr_func_with_column(self.func, col_name)
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
            col_buffer.append(dialect.column2sql(self.alias))
        return "".join(col_buffer)


class Condition(DMLBase):
    """
    Sub-condition of a set of SQL join condition.

    :ivar Column column_1: The first column in a join condition. This column
        could be transformed into a condition directly if `value` of this column
        object is not `None`.
    :ivar Column column_2: The second column in a join condition. This column
        could be `None` if not required.
    :ivar CompareTypes compare: Type of comparison between record value and
        target value.
    :ivar RelationTypes relation: Type of relation between different criterion.
    :ivar bool is_first: A boolean indicating if current column is the first
        column in a column list.
    """
    column_1 = None
    column_2 = None
    compare = None
    relation = None
    is_first = True

    def __init__(self, column_1, column_2=None,
                 compare_type=CompareTypes.EQUALS,
                 relation_type=RelationTypes.AND, is_first=True):
        """
        Initialize a `Condition` object for making SQL join condition.

        :param column_1: The first column in a join condition. This column could
            be transformed into a condition directly if `value` of this column
            object is not `None`.
        :type column_1: Column
        :param column_2: The second column in a join condition. This column
            could be `None` if not required.
        :type column_2: Column
        :param compare_type: Type of comparison between `column_1` and
            `column_2` value. Default by EQUALS.
        :type compare_type: CompareTypes
        :param relation_type: Type of relation between different criterion.
            Default by AND.
        :type relation_type: RelationTypes
        :raises UnsupportedJoinTypeError: If column definition for setting
            condition is not correct.
        """
        if column_1 is None \
                or (column_1.value is None and column_2 is None):
            raise UnsupportedJoinTypeError
        self.column_1 = column_1
        self.column_2 = column_2
        self.compare = compare_type
        self.relation = relation_type
        self.is_first = is_first

    def to_sql(self, dialect):
        """
        Convert Condition object to be component of SQL statement.

        :param dialect: SQL dialect to generate statements to work with
            different databases. This dialect should be an instance of
            :class:`Dialect` class.
        :type dialect: Dialect
        :return: A string of SQL condition.
        :rtype: str
        """
        condition = []
        # Add relation key word
        if not self.is_first:
            condition.append(SQLUtils.get_sql_relation(self.relation))
        condition.append(self.column_1.to_sql(dialect))
        # Add operator & the second column if needed
        if self.column_1.value is None:
            operator = SQLUtils.get_operator_with_value(self.compare, "")[0]
            condition.append(operator)
            condition.append(self.column_2.to_sql(dialect))
        return "".join(condition)


# =========================
# Auxiliary DML components:
#   1. JoinedConditions
#   2. JoinedTables
# =========================
class JoinedConditions(DMLBase):
    """
    Create condition to join tables.

    :ivar list _conditions: List of sub-conditions used in a SQL join condition.
    """
    _conditions = []

    def __init__(self):
        """
        Initialize a `JoinedConditions` object for making SQL join condition.
        """
        super(JoinedConditions, self).__init__()
        self._conditions = []

    def add_condition(self, column_1, column_2=None,
                      compare_type=CompareTypes.EQUALS,
                      relation_type=RelationTypes.AND):
        """
        Add a sub-condition into a set of SQL join condition.

        :param column_1: The first column in a join condition. This column
            could be transformed into a condition directly if `value` of this
            column object is not `None`.
        :type column_1: Column
        :param column_2: The second column in a join condition. This column
            could be `None` if not required.
        :type column_2: Column
        :param compare_type: Type of comparison between `column_1` and
            `column_2` value. Default by EQUALS.
        :type compare_type: CompareTypes
        :param relation_type: Type of relation between different criterion.
            Default by AND.
        :type relation_type: RelationTypes
        """
        is_first = self.get_size() == 0
        self._conditions.append(Condition(
            column_1, column_2, compare_type, relation_type, is_first)
        )

    def get_size(self):
        """
        Get number of the sub-conditions in current join condition.

        :return: Number of the sub-conditions.
        :rtype: int
        """
        return len(self._conditions)

    def clear(self):
        """
        Reset current condition.
        """
        self._raw_sql = None
        self._conditions = []

    def create_keyword(self):
        """
        Create the keyword string for SQL join condition.

        :return: Keyword string for SQL join condition.
        :rtype: str
        """
        return " ON "

    def to_sql(self, dialect):
        """
        Convert `JoinedConditions` object to be component of SQL statement.

        :param dialect: SQL dialect to generate statements to work with
                different databases. This dialect should be an instance of
                :class:`Dialect` class.
        :type dialect: Dialect
        :return: A string of SQL JOIN condition.
        :rtype: str
        """
        join_buffer = []
        if self.get_size() > 0:
            join_buffer.append(self.create_keyword())
        for condition in self._conditions:
            join_buffer.append(condition.to_sql(dialect))
        return "".join(join_buffer)


class JoinedTables(DMLBase):
    """
    Join tables or result tables into one result table to select data from.

    :ivar list _tables: List of tables or result tables to join into one table.
    """
    _tables = []

    def __init__(self):
        """
        Initialize a `Tables` object for making tables section in a SQL
        statement.
        """
        super(JoinedTables, self).__init__()
        self._tables = []

    def add_table(self, name=None, alias=None, select=None,
                  join=JoinTypes.INNER_JOIN, condition=None):
        """
        A table into the table list.

        :param name: Table name of target table.
        :type name: str
        :param alias: Temporarily SQL alias name for the target table or result
            table.
        :type alias: str
        :param select: Select object for creating a result table.
        :type select: Select
        :param join: The SQL join type for combining current table with the
            previous table.
        :type join: JoinTypes
        :param condition: The condition for combining current table
            with the previous table.
        :type condition: JoinedConditions
        :raises UnsupportedJoinTypeError: If join condition is not provided
            while current table is not the first in the table list.
        """
        is_first = self.get_size() == 0
        table = Table(name, alias, select, is_first)
        if not is_first:
            if condition is None:
                raise UnsupportedJoinTypeError
            table.join = join
            table.condition = condition
        self._tables.append(table)

    def get_size(self):
        """
        Get number of the tables to be used in current SQL.

        :return: Number of the tables.
        :rtype: int
        """
        return len(self._tables)

    def clear(self):
        """
        Reset current tables.
        """
        self._raw_sql = None
        self._tables = []

    def to_sql(self, dialect):
        """
        Convert `Tables` object into part of SQL statement

        :param dialect: SQL dialect to generate statements to work with
            different databases. This dialect should be an instance of
            :class:`Dialect` class.
        :type dialect: Dialect
        :return: A string of SQL clause.
        :rtype: str
        """
        if self._raw_sql:
            # Use raw SQL statement if exists
            return self._raw_sql
        tables_buffer = []
        for table in self._tables:
            tables_buffer.append(table.to_sql(dialect))
        return "".join(tables_buffer)


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

    def add_column(self, column_name, column_value, table_name=None,
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
        :param table_name: Table name or table alias to get column from.
        :type table_name: str
        :param column_type: Column data type of target column.
        :type column_type: ValueTypes
        :param compare_type: Type of comparison between record value and target
            value.
        :type compare_type: CompareTypes
        :param relation_type: Type of relation between different criterion.
        :type relation_type: RelationTypes
        :raises NoneColumnNameError: If column name is `None`.

        .. note:: `column_value` could be a target column name or column alias.

        .. seealso:: :class:`~.constants.ValueTypes`,
            :class:`~.constants.CompareTypes` and
            :class:`~.constants.RelationTypes`.
        """
        if column_name is not None:
            is_first = self.get_size() == 0
            col = Column(column_name, is_first)
            col.value = column_value
            col.table = table_name
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

    def add_column(self, column_name, aggr_func, column_value, table_name=None,
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
        :param table_name: Table name or table alias to get column from.
        :type table_name: str
        :param column_type: Column data type of target column.
        :type column_type: ValueTypes
        :param compare_type: Type of comparison between record value and target
            value.
        :type compare_type: CompareTypes
        :param relation_type: Type of relation between different criterion.
        :type relation_type: RelationTypes
        :raises NoneColumnNameError: If column name is `None`.

        .. note:: `column_value` could be a target column name or column alias.

        .. seealso:: :class:`~.constants.ValueTypes`,
            :class:`~.constants.CompareTypes` and
            :class:`~.constants.RelationTypes`.
        """
        if column_name is not None:
            is_first = self.get_size() == 0
            col = Column(column_name, is_first)
            col.func = aggr_func
            col.value = column_value
            col.table = table_name
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

    def add_column(self, column_name, table_name=None):
        """
        Add column to group the result.

        :param column_name: Column name of target column to group.
        :type column_name: str
        :param table_name: Table name or table alias to get column from.
        :type table_name: str
        :raises NoneColumnNameError: If column name is `None`.
        """
        if column_name is not None:
            is_first = self.get_size() == 0
            col = Column(column_name, is_first)
            col.table = table_name
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

    def add_column(self, column_name, asc=True, table_name=None):
        """
        Add a column for sorting order.

        :param column_name: Column name of the target column to be ordered by.
        :type column_name: str
        :param asc: Sort the records in ascending order or in a descending
            order. Default by "True(ascending)".
        :type asc: bool
        :param table_name: Table name or table alias to get column from.
        :type table_name: str
        :raises NoneColumnNameError: If column name is `None`.
        """
        if column_name is not None:
            is_first = self.get_size() == 0
            col = Column(column_name, is_first)
            col.asc = asc
            col.table = table_name
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
    :cvar list _columns: A list of columns for selecting data.
    :cvar JoinedTables _tables: Target table to select data from. The target
        table could be a single table or a result table joined by multiple
        tables.
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
    _tables = None
    _where = None
    _having = None
    _group_by = None
    _order_by = None

    def add_column(self, column_name, table_name=None, aggr_func=None,
                   alias=None):
        """
        Add column for selecting data from.

        :param column_name: Column name of target column to select.
        :type column_name: str
        :param table_name: Table name or table alias to get column from.
        :type table_name: str
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
            column.table = table_name
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

    def set_tables(self, tables):
        """
        Set the target table to select data from.

        :param tables: Target table to select data from.
        :type tables: JoinedTables
        """
        self._tables = tables

    def get_tables(self):
        """
        Get the target table to select data from.

        :return: Target table to select data from.
        """
        return self._tables

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
