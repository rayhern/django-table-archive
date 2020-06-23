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
