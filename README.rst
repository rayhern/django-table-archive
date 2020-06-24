=====================
django-table-archive
=====================

Django table archive is an app that will allow you to backup your models in MySQL to a different database.

Quick start
-----------

1. ``pip install django-table-archive``

2. Add "django_table_archive" to your INSTALLED_APPS setting like this::

    INSTALLED_APPS = [
        ...
        'django_table_archive',
    ]

3. Add this to your settings.py::

	ARCHIVE_TABLES = [
	    {
	        'table': 'api_person',
	        'days_old': '90',
	        'date_field': 'date_created'
	    }
	]

This archive process will also work with django-table-sharding package. It will look for all tables matching the name
and back them all up.

table = The table to archive.

days_old = Archive all items that are over 90 days old.

date_field = The datetime used to check if it is over 90 days old.