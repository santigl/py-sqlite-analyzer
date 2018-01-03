#!/usr/bin/env python3

# Copyright 2017 Santiago Gil
# (github.com/santigl)
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
# 1. Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright
# notice, this list of conditions and the following disclaimer in the
# documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

import sys

from sqliteanalyzer.sqliteanalyzer import SQLite3Analyzer

# This produces a report with the same format as sqlite_analyzer.
# (https://sqlite.org/sqlanalyze.html)

class SQLite3ClassicReport:
    def __init__(self, db_path):
        self._database_path = db_path
        self._stats = SQLite3Analyzer(db_path)

    def disk_space_report(self):
        print('/** Disk-Space Utilization Report For '
              '{}\n'.format(self._database_path))

        page_count = self._stats.page_count()
        self._stat_line('Page size in bytes', self._stats.page_size())
        self._stat_line('Pages in the whole file (measured)',
                        page_count)
        self._stat_line('Pages in the whole file (calculated)',
                        self._stats.calculated_page_count())

        in_use_pages = self._stats.in_use_pages()
        in_use_percent = self._percentage(in_use_pages, page_count)
        self._stat_line('Pages that store data', in_use_pages,
                        in_use_percent)

        self._stat_line('Pages on the freelist (per header)',
                        self._stats.freelist_count())
        self._stat_line('Pages on the freelist (calculated)',
                        self._stats.calculated_free_pages())

        autovacuum_pages = self._stats.autovacuum_page_count()
        self._stat_line('Pages of auto-vacuum overhead',
                        autovacuum_pages,
                        self._percentage(autovacuum_pages, page_count))

        self._stat_line('Number of tables in the database',
                        self._stats.ntable())
        self._stat_line('Number of indices',
                        self._stats.nindex())
        self._stat_line('Number of defined indices',
                        self._stats.nmanindex())
        self._stat_line('Number of implied indices',
                        self._stats.nautoindex())


        true_file_size = self._stats.file_size()
        if self._stats.is_compressed():
            self._stat_line('Size of uncompressed content in bytes',
                            true_file_size)

            logical_file_size = self._stats.logical_file_size()
            efficiency = self._percentage(true_file_size,
                                          logical_file_size)

            self._stat_line('Size of compressed file on disk',
                            self._stats.file_size(), efficiency)
        else:
            self._stat_line('Size of the file in bytes:',
                            true_file_size)


        payload = self._stats.payload_size()
        payload_percent = self._percentage(payload,
                                           self._stats.file_size())

        self._stat_line('Bytes of user payload stored', payload,
                        payload_percent)
        print()

    def tables_page_count_report(self):
        self._title_line('Page counts for all tables with their '
                         'indices')

        # (We want to display the larger tables first)
        page_usages = sorted(self._stats.table_space_usage().items(),
                             key=lambda k: k[1].size, reverse=True)

        for (table_name, (table_pages, table_size)) in page_usages:
            self._stat_line(table_name.upper(), table_size,
                            self._percentage(table_size,
                                             self._stats.page_count()))
        print()

    def tables_and_indices_page_usage_report(self):
        self._title_line('Page counts for all tables '
                         'and indices separately')

        page_counts = []
        # Getting the page count for tables...
        for table in self._stats.tables():
            page_count = self._stats.table_page_count(table,
                                                      exclude_indices=True)
            page_counts.append((table, page_count))
        # ... and for indices:
        for index in self._stats.indices():
            entry = (index['name'],
                     self._stats.index_page_count(index['name']))
            page_counts.append(entry)

        # (We want to display the larger entries,
        # table or indices, first)
        page_counts.sort(key=lambda k: k[1], reverse=True)

        for (name, pages) in page_counts:
            percentage = self._percentage(pages,
                                          self._stats.page_count())

            self._stat_line(name.upper(), pages, percentage)

        print()

    def table_details(self, table):
        table_name = table.upper()

        if not self._stats.index_list(table):
            self._title_line('Table {}'.format(table_name))
            self._print_stats(self._stats.table_stats(table))
            return

        # Splitting into 3 parts:
        self._title_line('Table {} and all its '
                         'indices'.format(table_name))

        self._print_stats(self._stats.table_stats(table))

        print()

        self._title_line('Table {} w/o any '
                         'indices'.format(table_name))

        self._print_stats(self._stats.table_stats(table,
                                                  exclude_indices=True))

        for index in sorted(self._stats.index_list(table),
                            key=lambda k: k['name']):
            index_name = index['name'].upper()
            title = 'Index {} of table {}'.format(table_name,
                                                  index_name)
            self._title_line(title)

            index_stats = self._stats.index_stats(index['name'])
            self._print_stats(index_stats)
            print()

    def global_usage_report(self):
        self._title_line('All tables and indices')
        global_stats = self._stats.global_stats()
        self._print_stats(global_stats)
        print()

        self._title_line('All tables')
        table_stats = self._stats.global_stats(exclude_indices=True)
        self._print_stats(table_stats)
        print()

    def indices_usage_report(self):
        if self._stats.indices():
            self._title_line('All indices')
            self._print_stats(self._stats.indices_stats())

    def tables_details_report(self):
        # (We display the larger tables first.)
        tables = sorted(self._stats.tables(),
                        key=self._stats.table_page_count,
                        reverse=True)

        for table in tables:
            self.table_details(table)

    def _print_stats(self, table_stats):
        t = table_stats

        self._stat_line('Percentage of total database',
                        '{:g}%'.format(t['total_pages_percent']))
        self._stat_line('Number of entries', t['nentry'])

        self._stat_line('Bytes of storage consumed', t['storage'])

        self._stat_line('Bytes of payload', t['payload'],
                        self._percentage(t['payload'], t['storage']))

        self._stat_line('Bytes of metadata', t['total_metadata'],
                        self._percentage(t['total_metadata'],
                                         t['storage']))

        if t['cnt'] == 1:
            self._stat_line('B-tree depth', t['depth'])

        self._stat_line('Average payload per entry',
                        t['average_payload'])
        self._stat_line('Average unused bytes per entry',
                        t['average_unused'])
        self._stat_line('Average metadata per entry',
                        t['average_metadata'])

        if 'avg_fanout' in t:
            self._stat_line('Average fanout', t['avg_fanout'])

        if t['total_pages'] > 1:
            self._stat_line('Non-sequential pages', t['gap_cnt'],
                            t['fragmentation'])

        self._stat_line('Maximum payload per entry', t['mx_payload'])
        self._stat_line('Entries that use overflow', t['ovfl_cnt'],
                        t['ovfl_percent'])

        if t['int_pages'] > 0:
            self._stat_line('Index pages used', t['int_pages'])

        self._stat_line('Primary pages used', t['leaf_pages'])
        self._stat_line('Overflow pages used', t['ovfl_pages'])
        self._stat_line('Total pages used', t['total_pages'])

        if t['int_unused'] > 0:
            self._stat_line('Unused bytes on index pages',
                            t['int_unused'],
                            t['int_unused_percent'])

        self._stat_line('Unused bytes on primary pages',
                        t['leaf_unused'],
                        t['leaf_unused_percent'])

        self._stat_line('Unused bytes on overflow pages',
                        t['ovfl_unused'],
                        t['ovfl_unused_percent'])

        self._stat_line('Unused bytes on all pages',
                        t['total_unused'], t['total_unused_percent'])

        print()

    def stat_db_dump(self):
        print('The entire text of this report can be sourced into any '
              'SQL database')
        print('engine for further analysis. '
              'All of the text above is an SQL comment.')
        print('The data used to generate this report follows:')
        print('*/')

        for line in self._stats.stat_db_dump():
            print(line)

    @staticmethod
    def _title_line(title):
        print('*** {} {}\n'.format(title, '*' * (79 - len(title) - 5)))

    @classmethod
    def _stat_line(cls, description, value, percentage=None):
        dots = '.' * (50 - len(description))

        label = description + dots

        value = ('{:.2f}' if isinstance(value, float) \
                  else '{}').format(value)

        if percentage is None:
            print('{} {}'.format(label, value))
            return

        sep = ' ' * (10 - len(value))

        p = '{}%'.format(cls._round_percentage(percentage))\
            if not isinstance(percentage, str)\
            else percentage

        print('{} {}{} {:>10}'.format(label, value, sep, p))

    @staticmethod
    def _percentage(value, total):
        if total == 0:
            return 0
        return 100 * value / total

    @staticmethod
    def _round_percentage(percentage):
        if percentage == 100.0 or percentage < 0.001 \
           or(percentage > 1.0 and percentage < 99.0):
            p = '{:5.1f}'

        elif percentage < 0.1 or  percentage > 99.9:
            p = '{:7.3f}'

        else:
            p = '{:6.2f}'

        return p.format(percentage)

    def print_definitions(self):
        d = '''Page size in bytes

    The number of bytes in a single page of the database file.
    Usually 1024.

Number of pages in the whole file

    The number of {}-byte pages that go into forming the complete
    database

Pages that store data

    The number of pages that store data, either as primary B*Tree pages or
    as overflow pages.  The number at the right is the data pages divided by
    the total number of pages in the file.

Pages on the freelist

    The number of pages that are not currently in use but are reserved for
    future use.  The percentage at the right is the number of freelist pages
    divided by the total number of pages in the file.

Pages of auto-vacuum overhead

    The number of pages that store data used by the database to facilitate
    auto-vacuum. This is zero for databases that do not support auto-vacuum.

Number of tables in the database

    The number of tables in the database, including the SQLITE_MASTER table
    used to store schema information.

Number of indices

    The total number of indices in the database.

Number of defined indices

    The number of indices created using an explicit CREATE INDEX statement.

Number of implied indices

    The number of indices used to implement PRIMARY KEY or UNIQUE constraints
    on tables.

Size of the file in bytes

    The total amount of disk space used by the entire database files.

Bytes of user payload stored

    The total number of bytes of user payload stored in the database. The
    schema information in the SQLITE_MASTER table is not counted when
    computing this number.  The percentage at the right shows the payload
    divided by the total file size.

Percentage of total database

    The amount of the complete database file that is devoted to storing
    information described by this category.

Number of entries

    The total number of B-Tree key/value pairs stored under this category.

Bytes of storage consumed

    The total amount of disk space required to store all B-Tree entries
    under this category.  The is the total number of pages used times
    the pages size.

Bytes of payload

    The amount of payload stored under this category.  Payload is the data
    part of table entries and the key part of index entries.  The percentage
    at the right is the bytes of payload divided by the bytes of storage
    consumed.

Bytes of metadata

    The amount of formatting and structural information stored in the
    table or index.  Metadata includes the btree page header, the cell pointer
    array, the size field for each cell, the left child pointer or non-leaf
    cells, the overflow pointers for overflow cells, and the rowid value for
    rowid table cells.  In other words, metadata is everything that is neither
    unused space nor content.  The record header in the payload is counted as
    content, not metadata.

Average payload per entry

    The average amount of payload on each entry.  This is just the bytes of
    payload divided by the number of entries.

Average unused bytes per entry

    The average amount of free space remaining on all pages under this
    category on a per-entry basis.  This is the number of unused bytes on
    all pages divided by the number of entries.

Non-sequential pages

    The number of pages in the table or index that are out of sequence.
    Many filesystems are optimized for sequential file access so a small
    number of non-sequential pages might result in faster queries,
    especially for larger database files that do not fit in the disk cache.
    Note that after running VACUUM, the root page of each table or index is
    at the beginning of the database file and all other pages are in a
    separate part of the database file, resulting in a single non-
    sequential page.

Maximum payload per entry

    The largest payload size of any entry.

Entries that use overflow

    The number of entries that user one or more overflow pages.

Total pages used

    This is the number of pages used to hold all information in the current
    category.  This is the sum of index, primary, and overflow pages.

Index pages used

    This is the number of pages in a table B-tree that hold only key (rowid)
    information and no data.

Primary pages used

    This is the number of B-tree pages that hold both key and data.

Overflow pages used

    The total number of overflow pages used for this category.

Unused bytes on index pages

    The total number of bytes of unused space on all index pages.  The
    percentage at the right is the number of unused bytes divided by the
    total number of bytes on index pages.

Unused bytes on primary pages

    The total number of bytes of unused space on all primary pages.  The
    percentage at the right is the number of unused bytes divided by the
    total number of bytes on primary pages.

Unused bytes on overflow pages

    The total number of bytes of unused space on all overflow pages.  The
    percentage at the right is the number of unused bytes divided by the
    total number of bytes on overflow pages.

Unused bytes on all pages

    The total number of bytes of unused space on all primary and overflow
    pages.  The percentage at the right is the number of unused bytes
    divided by the total number of bytes.
'''.format(self._stats.page_size())
        self._title_line('Definitions')
        print(d)
        print('*' * 79)


def main():
    if len(sys.argv) < 2:
        print('Error: Missing .db file')
        print('Usage:', sys.argv[0], 'file')
        return -1

    db_path = sys.argv[1]
    a = SQLite3ClassicReport(db_path)

    a.disk_space_report()
    a.tables_page_count_report()
    a.tables_and_indices_page_usage_report()
    a.global_usage_report()
    a.indices_usage_report()
    a.tables_details_report()
    a.print_definitions()
    a.stat_db_dump()


if __name__ == '__main__':
    main()
