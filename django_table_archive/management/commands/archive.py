from django.core.management.base import BaseCommand
from django.conf import settings
from django.db import connections
from datetime import datetime
from datetime import timedelta
import traceback
import re
import time


def dictfetchall(cursor):
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]


class Command(BaseCommand):
    archive_db = settings.ARCHIVE_DB_ALIAS
    archive_db_name = settings.DATABASES[settings.ARCHIVE_DB_ALIAS]['NAME']
    def handle(self, *args, **options):

        print('archive has started @ %s!' % datetime.utcnow())

        for archive_dict in settings.ARCHIVE_TABLES:

            db_table = archive_dict['table']
            delta = archive_dict['days_old']
            date_field = archive_dict['date_field']

            tables = []
            try:
                with connections['default'].cursor() as cursor:
                    rows = self.run_sql(cursor, 'SHOW TABLES LIKE "%s%%"' % db_table)
                    if len(rows) > 0:
                        tables = [row[0] for row in rows]
            except:
                print(traceback.format_exc())

            for table in tables:
                self.create_table_if_none_exists(table)
                self.archive_table(table, delta, date_field)

    def archive_table(self, source_table, delta, date_field):
        # now_days = datetime.now() - timedelta(days=int(delta))
        now_days = datetime.now() - timedelta(minutes=5)

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

        with connections['default'].cursor() as cursor:
            cursor.execute('SELECT * FROM %s WHERE %s < "%s" AND id > %s' % (
                source_table, date_field, now_days.strftime('%Y-%m-%d %H:%M:%S'), last_pk))
            items_list = dictfetchall(cursor)

        if len(items_list) > 0:
            with connections[self.archive_db].cursor() as cursor:
                print('bulk inserting into database...')
                tuple_list = []
                for d in items_list:
                    tuple_list.append(tuple(d.values()))
                # use the first item to build our placeholders for the query, and column names.
                placeholders = ', '.join(['%s'] * len(items_list[0]))
                columns = ', '.join(items_list[0].keys())
                try:
                    cursor.executemany("INSERT INTO %s ( %s ) VALUES ( %s )" % (
                        source_table, columns, placeholders), tuple_list)
                except:
                    print(traceback.format_exc())

    def create_table_if_none_exists(self, source_table):
        create_table = False
        with connections[self.archive_db].cursor() as cursor:
            rows = self.run_sql(cursor, '''
                SELECT * FROM information_schema.tables WHERE table_name = '%s' 
                AND table_schema = '%s' LIMIT 1;''' % (source_table, self.archive_db_name))
            if len(rows) == 0:
                # create the table it doesn't exist.
                create_table = True

        if create_table is True:
            print('archive table not found creating (without constraints)...')
            with connections['default'].cursor() as cursor:
                rows = self.run_sql(cursor, 'SHOW CREATE TABLE %s;' % source_table)
                create_table_sql = rows[0][1]
                create_table_sql = create_table_sql[create_table_sql.index('(') + 1:]
                items = create_table_sql.split(',')
                real_items = []
                for item in items:
                    if 'CONSTRAINT' not in item:
                        real_items.append(item)
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
            print('sql> %s' % self.normalize_spaces(sql.strip()))
            cursor.execute(sql)
            rows = cursor.fetchall()
            return rows
        except:
            print(traceback.format_exc())
        return []

    def normalize_spaces(self, text):
        """Used for cleaning continuous white space in strings."""
        return re.sub(r'\s+', ' ', str(text))
