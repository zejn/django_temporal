from django.db.models.query import QuerySet, Q, ValuesQuerySet, ValuesListQuerySet
from django_temporal.db.models.sql.query import TemporalQuery

class TemporalQuerySet(QuerySet):
    def __init__(self, model=None, query=None, using=None, **kwargs):
        super(TemporalQuerySet, self).__init__(model=model, query=query, using=using, **kwargs)
        self.query = query or TemporalQuery(self.model)
