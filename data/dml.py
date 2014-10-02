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


class OrderBy(object):
    """
    Create column order method after the SQL `ORDER BY` Keyword.

    :ivar str column: Column name of the target column to be ordered by.
    :ivar bool asc: Sort the records in ascending order or in a descending
        order. Default by "True(ascending)".
    """
    column = None
    asc = True

    def __init__(self, column_name, asc=True):
        """
        Create the column order method.

        :param column_name: Column name of the target column to be ordered by.
        :type column_name: str
        :param asc: Sort the records in ascending order or in a descending
            order. Default by "True(ascending)".
        :type asc: bool
        """
        self.column = column_name
        self.asc = asc

    def set_column(self, column_name):
        """
        Set the name of current column.

        :param column_name: Column name of the target column to be ordered by.
        :type column_name: str
        """
        self.column = column_name

    def get_column(self):
        """
        Get the name of current column.

        :return: Column name of the target column to be ordered by.
        :rtype: str
        """
        return self.column

    def set_asc(self, asc=True):
        """
        Set sort order of current column.

        :param asc: Sort the records in ascending order or in a descending
            order. Default by "True(ascending)".
        :type asc: bool
        """
        self.asc = asc

    def get_asc(self):
        """
        Get set order of current column.

        :return: A boolean indicating the sort order of the records.
        :rtype: bool
        """
        return self.asc


class Select(object):
    columns = []
    orders = []
    where = None

    def set_where(self, where):
        self.where = where

    def get_where(self):
        return self.where

    def add_column(self, column_name):
        if column_name is not None:
            self.columns.append(column_name)

    def add_order_by(self, column_name, asc=True):
        if column_name is not None:
            self.orders.append(OrderBy(column_name, asc))
