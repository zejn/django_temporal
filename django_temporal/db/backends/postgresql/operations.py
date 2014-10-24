
from django.db.backends.postgresql_psycopg2.base import DatabaseOperations

from django_temporal.db.backends.util import TemporalOperation, TemporalFunction, TemporalFunctionTS

class TemporalOperator(TemporalOperation):
    def __init__(self, operator):
        super(TemporalOperator, self).__init__(operator=operator)

class PostgresTemporalOperations(DatabaseOperations):
    
    def __init__(self, connection):
        super(PostgresTemporalOperations, self).__init__(connection)
        
        self.temporal_operators = {
            'exact': TemporalOperator('='),
            'nequals': TemporalOperator('<>'),
            'contains': TemporalOperator('@>'),
            'contained_by': TemporalOperator('<@'),
            #'minus': TemporalOperator('-'),
            #'union': TemporalOperator('+'),
            'overlaps': TemporalOperator('&&'),
            'before': TemporalOperator('<<'),
            'after': TemporalOperator('>>'),
            'overleft': TemporalOperator('&<'),
            'overright': TemporalOperator('&>'),
            'adjacent': TemporalOperator('-|-'),
            'prior': TemporalFunctionTS('prior'),
            'lower': TemporalFunctionTS('lower'),
            'upper': TemporalFunctionTS('upper'),
            'later': TemporalFunctionTS('next'),
            'isempty': TemporalFunctionTS('isempty'),
        }
    
    def temporal_lookup_sql(self, lvalue, lookup_type, value, field, qn):
        alias, col, db_type = lvalue
        temporal_col = '%s.%s' % (qn(alias), qn(col))
        if lookup_type in self.temporal_operators:
            op = self.temporal_operators[lookup_type]
            return op.as_sql(temporal_col, '%s')
        else:
            raise NotImplementedError
