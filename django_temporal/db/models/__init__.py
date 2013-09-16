
import django.db.models
from django.db.models import base
from django.db.models import *

from django_temporal.db.models.manager import TemporalManager
from django_temporal.db.models.fields import \
	TIME_CURRENT, DATE_CURRENT, TIME_RESOLUTION, DATE_RESOLUTION, \
	Period, DateRange, PeriodField, DateRangeField, ValidTime, \
	TemporalForeignKey, ForeignKey

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





try:
    from django.conf import settings
    from south.modelsinspector import add_introspection_rules
except ImportError:
    pass

else:
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
        ),
        (
            (DateRangeField,),
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

    related_rules = [
        (
            (TemporalForeignKey,),
            [],
            {
                "temporal_current": ["temporal_current", {"default": False}],
                "temporal_sequenced": ["temporal_sequenced", {"default": False}],
                "to": ["rel.to", {}],
                "to_field": ["rel.field_name", {"default_attr": "rel.to._meta.pk.name"}],
                "related_name": ["rel.related_name", {"default": None}],
                "db_index": ["db_index", {"default": True}],
            },
        )
    ]
        
    add_introspection_rules(rules, ["^django_temporal\.db\.models\.fields\.(Period|Valid|DateRange)Field"])
    add_introspection_rules(related_rules, ["^django_temporal\.db\.models\.fields\.TemporalForeignKey"])

    print 'South introspection rules included'
