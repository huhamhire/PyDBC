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


class CompareTypes(object):
    EQUALS = 0
    NOT_EQUAL = 1
    GREATER_THAN = 2
    GREATER_THAN_OR_EQUAL = 3
    LESS_THAN = 4
    LESS_THAN_OR_EQUAL = 5
    BEGINS_WITH = 6
    NOT_BEGIN_WITH = 7
    ENDS_WITH = 8
    NOT_END_WITH = 9
    CONTAINS = 10
    NOT_CONTAIN = 11
    IN = 12
    NOT_IN = 13
    NULL = 14
    NOT_NULL = 15


class SQLTypes(object):
    """
    Defines the constants that are used to identify generic SQL types.

    .. note:: This class is never instantiated.

    :cvar int BIT: Type code that identifies the generic SQL type `BIT`.
    :cvar int SMALLINT: Type code that identifies the generic SQL type
        `SMALLINT`.
    :cvar int INTEGER: Type code that identifies the generic SQL type `INTEGER`.
    :cvar int BIGINT: Type code that identifies the generic SQL type `BIGINT`.
    :cvar int REAL: Type code that identifies the generic SQL type `REAL`.
    :cvar int NUMERIC: Type code that identifies the generic SQL type `NUMERIC`.
    :cvar int DECIMAL: Type code that identifies the generic SQL type `DECIMAL`.
    :cvar int CHAR: Type code that identifies the generic SQL type `CHAR`.
    :cvar int VARCHAR: Type code that identifies the generic SQL type `VARCHAR`.
    :cvar int LONG_VARCHAR: Type code that identifies the generic SQL type
        `LONG_VARCHAR`.
    :cvar int BINARY: Type code that identifies the generic SQL type `BINARY`.
    :cvar int VARBINARY: Type code that identifies the generic SQL type
        `VARBINARY`.
    :cvar int LONG_VARBINARY: Type code that identifies the generic SQL type
        `LONG_VARBINARY`.
    :cvar int BLOB: Type code that identifies the generic SQL type `BLOB`.
    :cvar int CLOB: Type code that identifies the generic SQL type `CLOB`.
    """
    BIT = -7
    SMALLINT = 5
    INTEGER = 4
    BIGINT = -5
    REAL = 7
    NUMERIC = 2
    DECIMAL = 3
    CHAR = 2
    VARCHAR = 12
    LONG_VARCHAR = -1
    BINARY = -2
    VARBINARY = -3
    LONG_VARBINARY = -4
    BLOB = 2004
    CLOB = 2005


class ValueTypes(object):
    STRING = 0
    INTEGER = 1
    LONG = 2
    OTHER = -1


class RelationTypes(object):
    AND = 0
    OR = 1


class JoinTypes(object):
    """
    Defines the constants that are used to identify generic SQL join types.

    .. note:: This class is never instantiated.

    :cvar int INNER_JOIN: Type code that identifies the generic SQL join type
        which returns all rows when there is at least one match in BOTH tables.
    :cvar int LEFT_JOIN: Type code that identifies the generic SQL join type
        which returns all rows from the left table, and the matched rows from
        the right table.
    :cvar int RIGHT_JOIN: Type code that identifies the generic SQL join type
        which returns all rows from the right table, and the matched rows from
        the left table.
    :cvar int FULL_JOIN: Type code that identifies the generic SQL join type
        which returns all rows when there is a match in ONE of the tables.
    """
    INNER_JOIN = 0
    RIGHT_JOIN = 1
    LEFT_JOIN = 2
    FULL_JOIN = 3


class AggregateFunctions(object):
    """
    Defines the constants that are used to identify generic SQL aggregate
    functions.

    .. note:: This class is never instantiated.

    :cvar int AVG: Type code that identifies the generic SQL function `AVG`.
    :cvar int COUNT: Type code that identifies the generic SQL function `COUNT`.
    :cvar int SUM: Type code that identifies the generic SQL function `SUM`.
    :cvar int MAX: Type code that identifies the generic SQL function `MAX`.
    :cvar int MIN: Type code that identifies the generic SQL function `MIN`.
    """
    AVG = 0
    COUNT = 1
    SUM = 2
    MAX = 3
    MIN = 4
