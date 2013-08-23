
from django.db.models import base

from django.db.models import *

from django_temporal.db.models.manager import TemporalManager
from django_temporal.db.models.fields import \
	TIME_CURRENT, DATE_CURRENT, TIME_RESOLUTION, DATE_RESOLUTION, \
	Period, DateRange, PeriodField, DateRangeField, ValidTime, \
	ForeignKey

"""
def _monkeypatch():
	old_prepare = base.ModelBase._prepare
	def new_prepare(cls):
		print 'Should do stuff here', cls
		for f in cls._meta.fields:
			if isinstance(f, ValidTime):
				print f.name, f
				cls.add_to_class('%s_key' % f.name, IntegerField(db_index=True, null=True, blank=True))
				print [f.name for f in cls._meta.fields]
		old_prepare(cls)
	base.ModelBase._prepare = new_prepare
_monkeypatch()
del _monkeypatch
"""



rules = [
  (
    (PeriodField,),
    [],
    {
        'sequenced_key': ['sequenced_key', {'default': None}],
        'current_unique': ['current_unique', {'default': None}],
        'sequenced_unique': ['sequenced_unique', {'default': None}],
        'nonsequenced_unique': ['nonsequenced_unique', {'default': None}],
        'not_empty': ['not_empty', {'default': True}],
    },
  )
]

try:
    from django.conf import settings
    from south.modelsinspector import add_introspection_rules
    
    add_introspection_rules(rules, ["^django_temporal\.db\.models\.fields\.(Period|Valid|DateRange)Field"])
    print 'South introspection rules included'
except ImportError:
    # no south installed
    pass