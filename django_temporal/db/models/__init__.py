
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


