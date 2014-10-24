
from django.db.models.query import sql
from django_temporal.db.models.sql.where import TemporalWhereNode

ALL_TERMS = set([
        'nequals', 'contains', 'contained_by', 'overlaps',
        'before', 'after', 'overleft', 'overright', 'adjacent',
        'prior', 'lower', 'upper', 'later', 'isempty', 'isnull',
        ])

ALL_TERMS.update(sql.constants.QUERY_TERMS)


class TemporalQuery(sql.Query):
    
    query_terms = ALL_TERMS
    
    def __init__(self, model, where=TemporalWhereNode):
        super(TemporalQuery, self).__init__(model, where=where)
    
