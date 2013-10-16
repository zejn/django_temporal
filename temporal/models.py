
#from django.db import models
from django_temporal.db import models
#from django_temporal.db.models.fields import PeriodField, ValidTime, ForeignKey
#from django_temporal.db.models.manager import TemporalManager

class Category(models.Model):
    cat = models.DecimalField(max_digits=10, decimal_places=0)
    valid_time = models.ValidTime(sequenced_unique=('cat',), current_unique=('cat',))
    
    objects = models.TemporalManager()
    
    def __unicode__(self):
        return u'Category %s = %s' % (self.pk, self.valid_time)

class ReferencedTemporalFK(models.Model):
    name = models.CharField(max_length=64)
    category = models.ForeignKey(Category)

class BothTemporalFK(models.Model):
    name = models.CharField(max_length=20)
    category = models.ForeignKey(Category)
    # intentionally not same name as above
    validity_time = models.ValidTime()

class CategoryToo(models.Model):
    cat = models.DecimalField(max_digits=10, decimal_places=0)
    valid_time = models.PeriodField(nonsequenced_unique=('cat',))

class SequencedFK(models.Model):
    name = models.CharField(max_length=20)
    category = models.ForeignKey(Category)

class DateTestModel(models.Model):
    cat = models.DecimalField(max_digits=10, decimal_places=0)
    date_seen = models.DateRangeField(current_unique=('cat',))

    objects = models.TemporalManager()

class NullEmptyFieldModel(models.Model):
    valid = models.DateRangeField(null=True, empty=True)
    
    objects = models.TemporalManager()