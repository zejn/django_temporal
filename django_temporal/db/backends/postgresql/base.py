#from django.db.backends.postgresql_psycopg2.base import *
from django.db.backends.postgresql_psycopg2.base import DatabaseWrapper as Psycopg2DatabaseWrapper

from django_temporal.db.backends.postgresql.creation import PostgresTemporalCreation
from django_temporal.db.backends.postgresql.operations import PostgresTemporalOperations

class DatabaseWrapper(Psycopg2DatabaseWrapper):
    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)
        self.creation = PostgresTemporalCreation(self)
        self.ops = PostgresTemporalOperations(self)


