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

from .constants import CompareTypes, RelationTypes, AggregateFunctions


class SQLUtils(object):
    @staticmethod
    def get_operator_with_value(compare_type, value):
        if compare_type == CompareTypes.EQUALS:
            operator = "="
        elif compare_type == CompareTypes.NOT_EQUAL:
            operator = "!="
        elif compare_type == CompareTypes.GREATER_THAN:
            operator = ">"
        elif compare_type == CompareTypes.GREATER_THAN_OR_EQUAL:
            operator = ">="
        elif compare_type == CompareTypes.LESS_THAN:
            operator = "<"
        elif compare_type == CompareTypes.LESS_THAN_OR_EQUAL:
            operator = "<="
        elif compare_type == CompareTypes.BEGINS_WITH:
            operator = " LIKE "
            value += "%"
        elif compare_type == CompareTypes.NOT_BEGIN_WITH:
            operator = " NOT LIKE "
            value += "%"
        elif compare_type == CompareTypes.ENDS_WITH:
            operator = " LIKE "
            value = "%" + value
        elif compare_type == CompareTypes.NOT_END_WITH:
            operator = " NOT LIKE "
            value = "%" + value
        elif compare_type == CompareTypes.CONTAINS:
            operator = " LIKE "
            value = "%" + value + "%"
        elif compare_type == CompareTypes.NOT_CONTAIN:
            operator = " NOT LIKE "
            value = "%" + value + "%"
        elif compare_type == CompareTypes.IN:
            operator = " IN "
        elif compare_type == CompareTypes.NOT_IN:
            operator = " NOT IN "
        elif compare_type == CompareTypes.NULL:
            operator = " IS NULL"
            value = ""
        elif compare_type == CompareTypes.NOT_NULL:
            operator = " IS NOT NULL"
            value = ""
        else:
            operator = "="
        return operator, value

    @staticmethod
    def get_sql_relation(relation):
        if relation == RelationTypes.AND:
            return " AND "
        else:
            return " OR "

    @staticmethod
    def get_sql_order_type(asc):
        if not asc:
            return " DESC"
        else:
            return " ASC"

    @staticmethod
    def get_aggr_func_with_column(func_type, column_name):
        if func_type == AggregateFunctions.AVG:
            function = "AVG(" + column_name + ")"
        elif func_type == AggregateFunctions.COUNT:
            function = "COUNT(" + column_name + ")"
        elif func_type == AggregateFunctions.MAX:
            function = "MAX(" + column_name + ")"
        elif func_type == AggregateFunctions.MIN:
            function = "MIN(" + column_name + ")"
        elif func_type == AggregateFunctions.SUM:
            function = "SUM(" + column_name + ")"
        else:
            function = None
        return function