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

from math import ceil
from os import stat
from include.sqlitemanager import SQLite3Manager

class SQLite3Analyzer:
    def __init__(self, db_path):
        self._db_file = db_path
        self._db = SQLite3Manager(self._db_file)

        self._is_compressed = None

        # Creating temporary DBSTAT table:
        self._create_temp_stat_table()

        # Creating in-memory db to store the stats:
        self._stat_db = SQLite3Manager()
        self._stat_db.execute_query(self._spaceused_table_create_query())

        # Gathering the stats for all tables:
        self._compute_stats()

    def item_count(self):
        return self._db.fetch_single_field('''SELECT COUNT(*)
                                            from SQLITE_MASTER''')

    def file_size(self):
        return stat(self._db_file).st_size

    # file_bytes
    def logical_file_size(self):
        return self.page_count() * self.page_size()

    def page_size(self):
        return self._db.fetch_single_field('PRAGMA page_size')

    # file_pgcnt
    def page_count(self):
        # set file_pgcnt  [db one {PRAGMA page_count}]
        return self._db.fetch_single_field('PRAGMA page_count')

    # free_pgcnt
    # set free_pgcnt    [expr {$file_pgcnt-$inuse_pgcnt-$av_pgcnt}]
    def calculated_free_pages(self):
        return self.page_count()\
               - self.in_use_pages()\
               - self.autovacuum_page_count()

    #file_pgcnt2
    def calculated_page_count(self):
        #$inuse_pgcnt+$free_pgcnt2+$av_pgcnt}
        return self.in_use_pages()\
               + self.freelist_count()\
               + self.autovacuum_page_count()

    #free_pgcnt2
    def freelist_count(self):
        return self._db.fetch_single_field('PRAGMA freelist_count')

    def page_info(self):
        query = '''SELECT pageno, name, path
                   FROM temp.stat
                   ORDER BY pageno'''

        return list(self._db.fetch_all_rows(query))

    # inuse_pgcnt
    # set sql {SELECT sum(leaf_pages+int_pages+ovfl_pages) FROM space_used}
    # set inuse_pgcnt   [expr wide([mem eval $sql])]
    def in_use_pages(self):
        query = '''SELECT sum(leaf_pages+int_pages+ovfl_pages)
                   FROM space_used'''
        return self._stat_db.fetch_single_field(query)


    # inuse_percent
    def in_use_percent(self):
        return self._percentage(self.in_use_pages(), self.page_count())

    def tables(self):
        tables = self._tables()
        return {t['name'] for t in tables if t['name'] == t['tbl_name']}

    def indices(self):
        tables = self._tables()
        return [t for t in tables if t['name'] != t['tbl_name']]

    def index_list(self, table):
        query = 'PRAGMA index_list = "{}"'.format(table)
        return [self._row_to_dict(row)\
              for row in self._stat_db.fetch_all_rows(query)]

    def ntable(self):
        return self._db.fetch_single_field('''SELECT count(*)+1
                                            FROM sqlite_master
                                            WHERE type="table"
                                         ''')

    def nindex(self):
        return self._db.fetch_single_field('''SELECT count(*)
                                            FROM sqlite_master
                                            WHERE type="index"
                                         ''')

    def nautoindex(self):
        return self._db.fetch_single_field('''SELECT count(*)
                                            FROM sqlite_master
                                            WHERE name
                                            LIKE "sqlite_autoindex%"
                                        ''')

    def nmanindex(self):
        return self.nindex() - self.nautoindex()


    def payload_size(self):
        return self._stat_db.fetch_single_field('''SELECT sum(payload)
                                                   FROM space_used
                                                   WHERE NOT is_index
                                                   AND name NOT
                                                   LIKE "sqlite_master";
                                                ''')

    def is_compressed(self):
        if self._is_compressed is None:
            table = self.tables().pop()
            self._iscompresed = self.table_stats(table)['is_compressed']
        return self._iscompresed

    # av_pgcnt
    def autovacuum_page_count(self):
        auto_vacuum = self._db.fetch_single_field('PRAGMA auto_vacuum')
        if auto_vacuum == 0 or self.page_count() == 1:
            return 0

        # The number of entries on each pointer map page.
        #
        # The layout of the database file is one pointer-map
        # page, followed by ptrsPerPage other pages, followed
        # by a pointer-map page, etc.
        #
        # The first pointer-map page is the second page
        # of the file overall.
        page_size = float(self.page_size())
        pointers_per_page = page_size / 5

        # Return the number of pointer map pages
        # in the database.
        return ceil((self.page_count() - 1) / (pointers_per_page + 1))

    def table_space_usage(self, table=None):
        if table is not None:
            return self._table_space_usage(table)

        return self._all_tables_usage()

    def table_page_count(self, name, exclude_indices=False):
        if exclude_indices:
            return self.table_stats(name,
                                    exclude_indices)['total_pages']

        return self._query_page_count(name)

    def index_page_count(self, name):
        return self._query_page_count(name)

    def index_stats(self, name):
        condition = 'name = "{}"'.format(name)
        return self._query_space_used_table(condition)

    def table_stats(self, name, exclude_indices=False):
        if exclude_indices:
            condition = 'name = "{}"'.format(name)
        else:
            condition = 'tblname = "{}"'.format(name)

        return self._query_space_used_table(condition)

    def global_stats(self, exclude_indices=False):
        condition = 'NOT is_index' if exclude_indices else '1'
        return self._query_space_used_table(condition)

    def indices_stats(self):
      #
      # THROW EXCEPTION IF THERE ARE NO INDICES (!)
      #

        return self._query_space_used_table('is_index')

    def is_without_rowid(self, table):
        query = 'PRAGMA index_list = "{}"'.format(table)
        indices = self._db.fetch_all_rows(query)

        for index in indices:
            if index['origin'].upper() == "PK":
                query = '''SELECT count(*)
                           FROM sqlite_master
                           WHERE name="{}"'''.format(table)

                pk_is_table = self._db.fetch_single_field(query)
            if not pk_is_table:
                return True

        return False

    def stat_db_dump(self):
        print('The entire text of this report can be sourced into any '
              'SQL database')
        print('engine for further analysis. '
              'All of the text above is an SQL comment.')
        print('The data used to generate this report follows:')
        print('*/')

        return list(self._stat_db.iterdump())



#### HELPERS ####

    def _query_space_used_table(self, where):
        # total_pages: Database pages consumed.
        # total_pages_percent: Pages consumed as a percentage of the file.
        # storage: Bytes consumed.
        # payload_percent: Payload bytes used as a percentage of $storage.
        # total_unused: Unused bytes on pages.
        # avg_payload: Average payload per btree entry.
        # avg_fanout: Average fanout for internal pages.
        # avg_unused: Average unused bytes per btree entry.
        # avg_meta: Average metadata overhead per entry.
        # ovfl_cnt_percent: Percentage of btree entries that use overflow pages.
        query = '''SELECT
                   sum(
                    CASE WHEN (is_without_rowid OR is_index) THEN nentry
                         ELSE leaf_entries
                    END
                   ) AS nentry,
                   sum(payload) AS payload,
                   sum(ovfl_payload) AS ovfl_payload,
                   max(mx_payload) AS mx_payload,
                   sum(ovfl_cnt) as ovfl_cnt,
                   sum(leaf_pages) AS leaf_pages,
                   sum(int_pages) AS int_pages,
                   sum(ovfl_pages) AS ovfl_pages,
                   sum(leaf_unused) AS leaf_unused,
                   sum(int_unused) AS int_unused,
                   sum(ovfl_unused) AS ovfl_unused,
                   sum(gap_cnt) AS gap_cnt,
                   sum(compressed_size) AS compressed_size,
                   max(depth) AS depth,
                   count(*) AS cnt
                   FROM space_used
                   WHERE {}
                '''.format(where)

        stats = self._stat_db.fetch_one_row(query)
        s = self._row_to_dict(stats)

        # Adding calculated values:
        s['total_pages'] = s['leaf_pages']\
                           + s['int_pages']\
                           + s['ovfl_pages']

        s['total_pages_percent'] = self._percentage(s['total_pages'],
                                                    self.page_count())

        s['storage'] = s['total_pages'] * self.page_size()

        s['is_compressed'] = (s['storage'] > s['compressed_size'])

        s['compressed_overhead'] = 14 if s['is_compressed'] \
                                   else 0

        s['payload_percent'] = self._percentage(s['payload'],
                                                s['storage'])

        s['total_unused'] = s['ovfl_unused']\
                          + s['int_unused'] \
                          + s['leaf_unused']

        s['total_metadata'] = s['storage'] - s['payload']\
                            - s['total_unused']\
                            + 4 * (s['ovfl_pages'] - s['ovfl_cnt'])

        s['metadata_percent'] = self._percentage(s['total_metadata'],
                                                 s['storage'])

        if s['nentry'] == 0:
            s['average_payload'] = 0
            s['average_unused'] = s['average_metadata'] = 0
        else:
            s['average_payload'] = s['payload'] / s['nentry']
            s['average_unused'] = s['total_unused'] / s['nentry']
            s['average_metadata'] = s['total_metadata'] / s['nentry']


        s['ovfl_percent'] = self._percentage(s['ovfl_cnt'], s['nentry'])

        s['fragmentation'] = self._percentage(s['gap_cnt'],
                                              s['total_pages'] - 1)

        s['int_unused_percent'] = self._percentage(s['int_unused'],
                                                   s['int_pages']\
                                                   * self.page_size())

        s['ovfl_unused_percent'] = self._percentage(s['ovfl_unused'],
                                                    s['ovfl_pages']\
                                                    * self.page_size())

        s['leaf_unused_percent'] = self._percentage(s['leaf_unused'],
                                                    s['leaf_pages']\
                                                    * self.page_size())

        s['total_unused_percent'] = self._percentage(s['total_unused'],
                                                     s['storage'])

        return s


    def _query_page_count(self, name):
        query = '''SELECT (int_pages + leaf_pages + ovfl_pages) AS count
                 FROM space_used
                 WHERE name = '{}'
                 '''.format(name)
        return self._stat_db.fetch_single_field(query)

    def _all_tables_usage(self):
        ''' Returns the usage of all tables.
        '''
        query = '''SELECT tblname as name,
                        count(*) AS count,
                        sum(int_pages + leaf_pages + ovfl_pages) AS size
                  FROM space_used
                  GROUP BY tblname
                  ORDER BY size+0 DESC, tblname'''
        return [self._row_to_dict(row)\
             for row in self._stat_db.fetch_all_rows(query)]


    def _table_space_usage(self, table):
        ''' Returns the usage of a table. '''
        query = '''SELECT tblname as name,
                        count(*) AS count,
                        sum(int_pages + leaf_pages + ovfl_pages) AS size
                 FROM space_used
                 WHERE tblname = '{}'
                '''.format(table)

        return self._row_to_dict(self._stat_db.fetch_one_row(query))

    def _compute_stats(self):
        for table in self._tables():
            stats = self._extract_sqlite_stats(table['name'])

            is_index = (table['name'] != table['tbl_name'])

            values = (table['name'],
                      table['tbl_name'],
                      is_index,
                      stats['is_without_rowid'],
                      stats['nentry'],
                      stats['leaf_entries'],
                      stats['depth'],
                      stats['payload'],
                      stats['ovfl_payload'],
                      stats['ovfl_cnt'],
                      stats['mx_payload'],
                      stats['int_pages'],
                      stats['leaf_pages'],
                      stats['ovfl_pages'],
                      stats['int_unused'],
                      stats['leaf_unused'],
                      stats['ovfl_unused'],
                      stats['gap_count'],
                      stats['compressed_size'])

            placeholders = ','.join('?' * len(values))
            insert_query = '''INSERT INTO space_used
                              VALUES ({})'''.format(placeholders)

            self._stat_db.execute_query(insert_query, values)



### HELPERS ###
    def _count_gaps(self, table_name):
# Column 'gap_cnt' is set to the number of non-contiguous entries in the
# list of pages visited if the b-tree structure is traversed in a top-
# down fashion (each node visited before its child-tree is passed). Any
# overflow chains present are traversed from start to finish before any
# child-tree is.
        pages = self._db.fetch_all_rows('''SELECT pageno, pagetype
                                         FROM temp.dbstat
                                         WHERE name="{}"
                                         ORDER BY pageno;
                                      '''.format(table_name))
        gap_count = 0
        previous_page = 0
        for page in pages:
            if previous_page > 0 and (page['pagetype'] == 'leaf') \
               and (page['pageno'] != previous_page+1):
                gap_count += 1

            previous_page = page['pageno']

        return gap_count

    def _tables(self):
        tables = self._db.fetch_all_rows('''SELECT name, tbl_name
                                       FROM sqlite_master
                                       WHERE rootpage>0''')

        tables = [{'name': t['name'],
                   'tbl_name': t['tbl_name']} for t in tables]

        sqlite_master_table = {'name':     'sqlite_master',
                               'tbl_name': 'sqlite_master'}

        return tables + [sqlite_master_table]

    def _extract_sqlite_stats(self, table_name):
        query = '''SELECT
                sum(ncell) AS nentry,
                sum((pagetype == 'leaf') * ncell) AS leaf_entries,
                sum(payload) AS payload,
                sum((pagetype == 'overflow') * payload) AS ovfl_payload,
                sum(path LIKE '%+000000') AS ovfl_cnt,
                max(mx_payload) AS mx_payload,
                sum(pagetype == 'internal') AS int_pages,
                sum(pagetype == 'leaf') AS leaf_pages,
                sum(pagetype == 'overflow') AS ovfl_pages,
                sum((pagetype == 'internal') * unused) AS int_unused,
                sum((pagetype == 'leaf') * unused) AS leaf_unused,
                sum((pagetype == 'overflow') * unused) AS ovfl_unused,
                sum(pgsize) AS compressed_size,
                max((length(CASE WHEN path LIKE '%+%' THEN ''
                                 ELSE path END)+3)/4) AS depth
                FROM temp.dbstat
                WHERE name = '{}';'''.format(table_name)


        stats = self._row_to_dict(self._db.fetch_all_rows(query)[0])
        stats['is_without_rowid'] = self.is_without_rowid(table_name)
        stats['gap_count'] = self._count_gaps(table_name)

        return stats

    @staticmethod
    def _row_to_dict(row):
        res = {}
        for column in row.keys():
            res[column] = row[column]

        return res

    @staticmethod
    def _percentage(value, total):
        if total == 0:
            return 0
        return 100 * value / total

    def _create_stat_virtual_table(self):
        self._db.execute_query('''CREATE VIRTUAL TABLE temp.stat
                                 USING dbstat''')

    def _drop_stat_virtual_table(self):
        self._db.execute_query('DROP TABLE temp.stat')

    def _create_temp_stat_table(self):
        self._create_stat_virtual_table()

        self._db.execute_query('''CREATE TEMP TABLE dbstat
                                 AS SELECT * FROM temp.stat
                                 ORDER BY name, path''')

        self._drop_stat_virtual_table()

    @staticmethod
    def _stat_table_create_query():
        return '''CREATE TABLE stats("
                  name       STRING,  /* Name of table or index */
                  path       INTEGER, /* Path to page from root */
                  pageno     INTEGER, /* Page number */
                  pagetype   STRING,  /* 'internal', 'leaf' or 'overflow' */
                  ncell      INTEGER, /* Cells on page (0 for overflow) */
                  payload    INTEGER, /* Bytes of payload on this page */
                  unused     INTEGER, /* Bytes of unused space on this page */
                  mx_payload INTEGER, /* Largest payload size of all cells */
                  pgoffset   INTEGER, /* Offset of page in file */
                  pgsize     INTEGER  /* Size of the page */
            ");'''

    @staticmethod
    def _spaceused_table_create_query():
        return '''CREATE TABLE space_used(
                  name clob,        -- Name of a table or index in the database file
                  tblname clob,     -- Name of associated table
                  is_index boolean, -- TRUE if it is an index, false for a table
                  is_without_rowid boolean, -- TRUE if WITHOUT ROWID table
                  nentry int,       -- Number of entries in the BTree
                  leaf_entries int, -- Number of leaf entries
                  depth int,        -- Depth of the b-tree
                  payload int,      -- Total amount of data stored in this table or index
                  ovfl_payload int, -- Total amount of data stored on overflow pages
                  ovfl_cnt int,     -- Number of entries that use overflow
                  mx_payload int,   -- Maximum payload size
                  int_pages int,    -- Number of interior pages used
                  leaf_pages int,   -- Number of leaf pages used
                  ovfl_pages int,   -- Number of overflow pages used
                  int_unused int,   -- Number of unused bytes on interior pages
                  leaf_unused int,  -- Number of unused bytes on primary pages
                  ovfl_unused int,  -- Number of unused bytes on overflow pages
                  gap_cnt int,      -- Number of gaps in the page layout
                  compressed_size int  -- Total bytes stored on disk
                )'''
