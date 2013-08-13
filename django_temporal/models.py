
from django_temporal.db.models.fields import PeriodField

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
    
    add_introspection_rules(rules, ["^django_temporal\.db\.models\.fields\.(Period|Valid)Field"])
    print 'South introspection rules included'
except ImportError:
    # no south installed
    pass

