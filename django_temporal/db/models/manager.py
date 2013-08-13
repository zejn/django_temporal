from django.db.models.manager import Manager
from django_temporal.db.models.query import TemporalQuerySet

class TemporalManager(Manager):
    use_for_related_fields = True
    
    def get_query_set(self):
        return TemporalQuerySet(self.model)
    
    def at(self, time):
        return self.get_query_set().filter(valid_time__contains=time)

