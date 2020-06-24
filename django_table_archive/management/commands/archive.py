from django.core.management.base import BaseCommand
from django.conf import settings
from django.db import connections
from datetime import datetime
from datetime import timedelta
import traceback
import re


def dictfetchall(cursor):
    """Get rows from sql cursor as dict."""
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]


def chunks(source_list, batch_size):
    """Yield successive n-sized chunks from source_list."""
    for i in range(0, len(source_list), batch_size):
        yield source_list[i:i+batch_size]


def normalize_spaces(text):
    """Used for cleaning continuous white space in strings."""
    return re.sub(r'\s+', ' ', str(text))


class Command(BaseCommand):
    default_db = settings.ARCHIVE_PRIMARY_DB
    archive_db = settings.ARCHIVE_DB_ALIAS
    archive_db_name = settings.DATABASES[settings.ARCHIVE_DB_ALIAS]['NAME']
    batch_size = 10000
    verbosity = 1

    def handle(self, *args, **options):

        self.verbosity = int(options.get('verbosity', 1))

        print('archive has started. verbosity: %s. @ %s!' % (self.verbosity, datetime.utcnow()))

        for archive_dict in settings.ARCHIVE_TABLES:
            db_table = archive_dict.get('table', None)
            delta = archive_dict.get('days_old', None)
            date_field = archive_dict.get('date_field', None)

            if db_table is None or delta is None or date_field is None:
                print('Entry in settings.ARCHIVE_TABLES missing.')
                continue

            tables = []
            try:
                with connections[self.default_db].cursor() as cursor:
                    rows = self.run_sql(cursor, 'SHOW TABLES LIKE "%s%%"' % db_table)
                    if len(rows) > 0:
                        tables = [row[0] for row in rows]
            except:
                print(traceback.format_exc())

            for table in tables:
                self.create_archive_table_if_none_exists(table)
                self.archive_table(table, delta, date_field)

    def archive_table(self, source_table, delta, date_field):
        """
        Archives database items older than X days. Uses batch size when inserting into database.
        """
        now_days = datetime.now() - timedelta(days=int(delta))

        # Get last id that was archived, and use it.
        try:
            with connections[self.archive_db].cursor() as cursor:
                rows = self.run_sql(cursor, 'SELECT id FROM %s ORDER BY id DESC LIMIT 1' % source_table)
                if len(rows) > 0:
                    last_pk = int(rows[0][0])
                else:
                    last_pk = 0
        except:
            last_pk = 0

        print('Getting items to archive...')
        try:
            with connections[self.default_db].cursor() as cursor:
                cursor.execute('SELECT * FROM %s WHERE %s < "%s" AND id > %s' % (
                    source_table, date_field, now_days.strftime('%Y-%m-%d %H:%M:%S'), last_pk))
                items_list = dictfetchall(cursor)
        except:
            print('Error: Could not retrieve items to archive.')
            return

        if len(items_list) > 0:
            with connections[self.archive_db].cursor() as cursor:
                print('Bulk inserting into database %s. table %s...' % (self.archive_db_name, source_table))
                # use the first item to build our placeholders for the query, and column names.
                placeholders = ', '.join(['%s'] * len(items_list[0]))
                columns = ', '.join(items_list[0].keys())
                n_chunks = chunks(items_list, self.batch_size)
                for chunk in n_chunks:
                    tuple_list = [tuple(d.values()) for d in chunk]
                    try:
                        cursor.executemany("INSERT INTO %s ( %s ) VALUES ( %s )" % (
                            source_table, columns, placeholders), tuple_list)
                    except:
                        print(traceback.format_exc())

    def create_archive_table_if_none_exists(self, source_table):
        """
        Check the archive database to see if it contains the destination table. If not, this will
        create the table without foreign keys.
        """
        create_table = False
        with connections[self.archive_db].cursor() as cursor:
            rows = self.run_sql(cursor, '''
                SELECT * FROM information_schema.tables WHERE table_name = '%s' 
                AND table_schema = '%s' LIMIT 1;''' % (source_table, self.archive_db_name))
            if len(rows) == 0:
                # create the table it doesn't exist.
                create_table = True

        if create_table is True:
            print('Archive table not found creating (without constraints)...')
            with connections[self.default_db].cursor() as cursor:
                rows = self.run_sql(cursor, 'SHOW CREATE TABLE %s;' % source_table)
                create_table_sql = rows[0][1]
                create_table_sql = create_table_sql[create_table_sql.index('(') + 1:]
                items = create_table_sql.split(',')
                real_items = [item.strip() for item in items if 'CONSTRAINT' not in item]
                # real_items = []
                # for item in items:
                #     if 'CONSTRAINT' not in item:
                #         real_items.append(item)
                create_table_sql = 'CREATE TABLE %s (%s)' % (source_table, ','.join(real_items))
            with connections[self.archive_db].cursor() as cursor:
                rows = self.run_sql(cursor, create_table_sql)

    def run_sql(self, cursor, sql):
        """
        Execute sql on a given cursor.
        (db does not need to be past, since we are using corresponding cursor.)
        """
        try:
            # For debugging.
            if self.verbosity > 1:
                print('sql> %s' % normalize_spaces(sql.strip()))
            cursor.execute(sql)
            rows = cursor.fetchall()
            return rows
        except:
            print(traceback.format_exc())
        return []

