from datetime import datetime, date, tzinfo, timedelta
import pytz
import re

from django.conf import settings
from django.forms import ValidationError
#from django.core.exceptions import ValidationError
from django.db import models
from django.db.models.query_utils import QueryWrapper

try:
    import psycopg2._range
    pgrange = psycopg2._range
except ImportError, e:
    pgrange = None

__all__ = ['TZDatetime', 'TZDateTimeField', 'TIME_CURRENT', 'Period', 'DateRange', 'PeriodField', 'DateRangeField', 'ForeignKey', 'TemporalForeignKey', 'DATE_CURRENT']

class TZDatetime(datetime):
    def aslocaltimezone(self):
        """Returns the datetime in the local time zone."""
        tz = pytz.timezone(settings.TIME_ZONE)
        return self.astimezone(tz)

# 2009-06-04 12:00:00+01:00 or 2009-06-04 12:00:00 +0100
TZ_OFFSET = re.compile(r'^"?(.*?)\s?([-\+])(\d\d):?(\d\d)?"?$')

TIME_CURRENT = datetime(9999, 12, 30, 0, 0, 0, 0)
TIME_RESOLUTION = timedelta(0, 0, 1) # = 1 microsecond
DATE_CURRENT = date(9999, 12, 30)
DATE_RESOLUTION = timedelta(1)
EMPTY = 'empty'


class TZDateTimeField(models.DateTimeField):
    """A DateTimeField that treats naive datetimes as local time zone."""
    __metaclass__ = models.SubfieldBase
    
    def to_python(self, value):
        """Returns a time zone-aware datetime object.
        
        A naive datetime is assigned the time zone from settings.TIME_ZONE.
        This should be the same as the database session time zone.
        A wise datetime is left as-is. A string with a time zone offset is
        assigned to UTC.
        """
        try:
            value = super(TZDateTimeField, self).to_python(value)
        except ValidationError:
            match = TZ_OFFSET.search(value)
            if match:
                value, op, hours, minutes = match.groups()
                minutes = minutes is not None and minutes or '0'
                value = super(TZDateTimeField, self).to_python(value)
                value = value - timedelta(hours=int(op + hours), minutes=int(op + minutes))
                value = value.replace(tzinfo=pytz.utc)
            else:
                raise
        
        if value is None:
            return value
        
        # Only force zone if the datetime has no tzinfo
        #if (value.tzinfo is None) or (value.tzinfo.utcoffset(value) is None):
        #    value = force_tz(value, settings.TIME_ZONE)
        return TZDatetime(value.year, value.month, value.day, value.hour,
            value.minute, value.second, value.microsecond, tzinfo=value.tzinfo)

def force_tz(obj, tz):
    """Converts a datetime to the given timezone.
    
    The tz argument can be an instance of tzinfo or a string such as
    'Europe/London' that will be passed to pytz.timezone. Naive datetimes are
    forced to the timezone. Wise datetimes are converted.
    """
    if not isinstance(tz, tzinfo):
        tz = pytz.timezone(tz)
    
    if (obj.tzinfo is None) or (obj.tzinfo.utcoffset(obj) is None):
        return tz.localize(obj)
    else:
        return obj.astimezone(tz)


class Period(object):
    subvalue_class = TZDateTimeField
    _value_current = TIME_CURRENT
    _value_resolution = TIME_RESOLUTION
    _input_type = datetime
    pg_dbvalue = pgrange != None and pgrange.DateTimeTZRange or None
    
    def __init__(self, period=None, lower=None, upper=None, empty=False):
        self.empty = False
        if empty:
            self.empty = True
            return
        
        if isinstance(period, (self._input_type, date)): # XXX FIXME argument rewriting isn't ok
            lower, upper, period = period, lower, None
        types = [basestring, self.__class__]
        if pgrange is not None:
            types.append(self.pg_dbvalue)
        if not (isinstance(period, tuple(types)) or isinstance(lower, self._input_type)):
            raise TypeError("You must specify either period (string or Period) or lower (TZDatetime or %s), got period=%r lower=%r" % (self._input_type, period.__class__, lower.__class__))
        
        if period is not None:
            if isinstance(period, basestring):
                m = re.match('^([\[\(])([^,]+),([^\]\)]+)([\]\)])$', period.strip())
                if not m:
                    if period.strip() == EMPTY or period.strip() == u'':
                        self.empty = True
                        return
                    raise TypeError("Invalid period string representation: %s" % repr(period))
                start_in, lower, upper, end_in = m.groups()
                
                self.lower = self.subvalue_class().to_python(lower.strip())
                self.upper = self.subvalue_class().to_python(upper.strip())
                if start_in == '[':
                    self.start_included = True
                else:
                    self.start_included = False
                
                if end_in == ']':
                    self.end_included = True
                else:
                    self.end_included = False
            elif pgrange is not None and isinstance(period, self.pg_dbvalue):
                if period.isempty:
                    self.empty = period.isempty
                    return
                self.lower = self.subvalue_class().to_python(period.lower)
                self.start_included = bool(period.lower_inc)
                self.upper = self.subvalue_class().to_python(period.upper)
                self.end_included = bool(period.upper_inc)
            elif isinstance(period, self.__class__):
                if period.empty:
                    self.empty = period.empty
                    return
                self.lower = period.lower
                self.start_included = period.start_included
                self.upper = period.upper
                self.end_included = period.end_included
        else:
            self.lower = lower
            self.start_included = True
            if upper is not None:
                self.upper = upper
                self.end_included = False
            else:
                self.upper = self._value_current
                self.end_included = True
        self.normalize()
    
    def lower():
        def fget(self):
            return self._lower
        def fset(self, value):
            if isinstance(value, TZDatetime):
                self._lower = value.replace(tzinfo=None)
            elif isinstance(value, self._input_type):
                self._lower = self.subvalue_class().to_python(value.strftime(u'%Y-%m-%d %H:%M:%S.%f%z')).replace(tzinfo=None)
            else:
                raise AssertionError("should never happen")
        return (fget, fset, None, "lower limit of period")
    lower = property(*lower())
    
    def start_included():
        def fget(self):
            return self._start_included
        def fset(self, value):
            if not value in (True, False):
                raise ValueError("Must be True or False")
            self._start_included = value
        return (fget, fset, None, "denotes if lower limit timestamp is open or closed")
    start_included = property(*start_included())
    
    def upper():
        def fget(self):
            return self._upper
        def fset(self, value):
            if isinstance(value, TZDatetime):
                self._upper = value.replace(tzinfo=None)
            elif isinstance(value, self._input_type):
                self._upper = self.subvalue_class().to_python(value.strftime(u'%Y-%m-%d %H:%M:%S.%f%z')).replace(tzinfo=None)
            else:
                raise AssertionError("should never happen")
        return (fget, fset, None, "upper limit of period")
    upper = property(*upper())
    
    def end_included():
        def fget(self):
            return self._end_included
        def fset(self, value):
            if not value in (True, False):
                raise ValueError("Must be True or False")
            self._end_included = value
        return (fget, fset, None, "denotes if end timestamp is open or closed")
    end_included = property(*end_included())
    
    def __eq__(self, other):
        if self.empty and other.empty:
            return True
        if (self.empty and not other.empty) or (not self.empty and other.empty):
            return False
        if  self.start_included == other.start_included and \
            self.end_included == other.end_included and \
            self.lower == other.lower and\
            self.upper == other.upper:
            return True
        return False
    
    def normalize(self):
        if not self.start_included:
            if self.lower is not None:
                self.lower += self._value_resolution
            self.start_included = True
        if self.end_included:
            if self.upper is not None:
                self.upper += self._value_resolution
            self.end_included = False
    
    def is_current(self):
        if self.empty:
            return False
        if self.upper == self._value_current and self.end_included == False:
            return True
        return False
    
    def set_current(self):
        self.upper = self._value_current
        self.end_included = False
    
    def prior(self):
        if self.start_included:
            return self.lower - self._value_resolution
        return self.lower
    
    def upper_or_none(self):
        if self.upper.year == 9999: # FIXME
            return None
        return self.upper
    
    def later(self):
        if self.end_included:
            return self.upper + self._value_resolution
        return self.upper
    
    def overlaps(self, other):
        if self.empty or other.empty:
            return False
        return self.upper > other.lower and other.upper > self.lower
    
    def intersection(self, other):
        if self.overlaps(other):
            return self.__class__(lower=max(self.lower, other.lower), upper=min(self.upper, other.upper))
        return self.__class__(empty=True)
    
    def __mul__(self, other):
        return self.intersection(other)
    
    def union(self, other):
        if self.overlaps:
            return self.__class__(lower=min(self.lower, other.lower), upper=max(self.upper, other.upper))
        return [self, other]

    def __add__(self, other):
        return self.union(other)

    def __cmp__(self, other):
        return cmp(self.lower, other.lower) or cmp(self.upper, other.upper)

    def __unicode__(self):
        if self.empty:
            return EMPTY
        return u''.join([
            self.start_included and u'[' or u'(',
            self._value_unicode(self.lower),
            u',',
            self._value_unicode(self.upper),
            self.end_included and ']' or ')',
            ])
    
    def __repr__(self):
        if self.empty:
            return '<%s empty>' % (self.__class__.__name__,)
        return '<%s from %s to %s>' % (self.__class__.__name__, self._value_unicode(self.lower), self._value_unicode(self.upper))
    
    def _value_unicode(self, value):
        return value.replace(tzinfo=pytz.UTC).strftime(u'%Y-%m-%d %H:%M:%S.%f%z')

class DateRange(Period):
    description = "a range of dates"
    subvalue_class = models.DateField
    _value_current = DATE_CURRENT
    _value_resolution = DATE_RESOLUTION
    _input_type = date
    pg_dbvalue = pgrange != None and pgrange.DateRange or None

    def _value_unicode(self, value):
        if value is None:
            return ''
        return value.strftime(u'%Y-%m-%d')

    def lower():
        def fget(self):
            return self.__lower
        def fset(self, value):
            if isinstance(value, date):
                self.__lower = self.subvalue_class().to_python(value.strftime(u'%Y-%m-%d'))
            elif value is None:
                self.__lower = None
            else:
                raise AssertionError("should never happen")
        return (fget, fset, None, "lower limit of date range")
    lower = property(*lower())
    
    def upper():
        def fget(self):
            return self.__upper
        def fset(self, value):
            if isinstance(value, date):
                self.__upper = self.subvalue_class().to_python(value.strftime(u'%Y-%m-%d'))
            elif value is None:
                self.__upper = None
            else:
                raise AssertionError("should never happen")
        return (fget, fset, None, "upper limit of date range")
    upper = property(*upper())


class PeriodField(models.Field):
    description = 'A period of time'
    value_class = Period
    __metaclass__ = models.SubfieldBase
    
    def __init__(self, verbose_name=None, sequenced_key=None, current_unique=None, sequenced_unique=None, nonsequenced_unique=None, empty=False, null=False, **kwargs):
        
        kwargs['verbose_name'] = verbose_name
        
        self.sequenced_key = sequenced_key
        
        self.current_unique = current_unique
        self.sequenced_unique = sequenced_unique
        self.nonsequenced_unique = nonsequenced_unique
        self.not_empty = not bool(empty)
        super(PeriodField, self).__init__(null=null, **kwargs)
    
    def db_type(self, connection):
        return 'tstzrange'
        # ALTER TABLE "temporal_incumbent" ADD EXCLUDE USING gist ("ssn_id" WITH =, "pcn_id" WITH =, "valid_time" WITH &&);
        #if self.sequenced_key:
        #    db_column = db_column + ', EXCLUDE USING gist (%s)' % ', '.join(['%s WITH =' % qn(i) for i in self.sequenced_key] + ['%s WITH &&' % qn(self.name)])
    
    def to_python(self, value):
        if isinstance(value, self.value_class):
            return value
        if value is None:
            return None
        return self.value_class(value)
    
    def get_prep_value(self, value):
        return self.value_class(value)
    
    def get_prep_lookup(self, lookup_type, value):
        if lookup_type == 'contains' and isinstance(value, (date, datetime)):
            return value

        if lookup_type in (
                'exact', 'lt', 'lte', 'gt', 'gte', 'nequals', 'contains', 'contained_by',
                'overlaps', 'before', 'after', 'overleft', 'overright', 'adjacent'):
            if not isinstance(value, self.value_class):
                value = self.value_class(value)
            return unicode(value)
        if lookup_type in ('prior', 'lower', 'upper', 'later'):
            if self.value_class == Period and isinstance(value, datetime):
                return unicode(value)
            elif self.value_class == DateRange and isinstance(value, date):
                return unicode(value)
            else:
                raise ValueError('Got invalid value into lookup? %r' % (value,))
        if lookup_type in ('isempty', 'isnull'):
            return value
        raise TypeError("Field has invalid lookup: %s" % lookup_type)
    
    def get_db_prep_lookup(self, lookup_type, value, connection, prepared=False):
        if lookup_type in ('exact', 'lt', 'lte', 'gt', 'gte'):
            return super(PeriodField, self).get_db_prep_lookup(lookup_type=lookup_type, value=value, connection=connection, prepared=prepared)
        elif lookup_type in ('nequals', 'contains', 'contained_by', 'overlaps', 'before', 'after', 'overleft', 'overright', 'adjacent'):
            return [value]
        elif lookup_type in ('prior', 'lower', 'upper', 'later'):
            return [value]
        elif lookup_type in ('isempty', 'isnull'):
            return [value]
    
    def get_db_prep_value(self, value, connection, prepared=False):
        if isinstance(value, datetime):
            return models.DateTimeField().get_db_prep_value(value, connection, prepared)
        else:
            if self.null and value is None:
                return None
            if pgrange is not None:
                if self.value_class == DateRange:
                    pg_klass = pgrange.DateRange
                elif self.value_class == Period:
                    pg_klass = pgrange.DateTimeTZRange
                else:
                    raise ValueError("Invalid value")
                
                if isinstance(value, basestring):
                    value = self.value_class(value)
                
                if value.empty:
                    val = pg_klass(empty=value.empty)
                else:
                    bounds = (value.start_included and '[' or '(') + (value.end_included and ']' or ')')
                    val = pg_klass(lower=value.lower, upper=value.upper, bounds=bounds)
                return val
            return unicode(self.value_class(value))

class ValidTime(PeriodField):
    "Special class in order to be able to know this represents validity"
    pass

class DateRangeField(PeriodField):
    description = 'a time range between two dates'
    value_class = DateRange

    def db_type(self, connection):
        return 'daterange'

class TemporalForeignKey(models.ForeignKey):
    def __init__(self, *args, **kwargs):
        temp_current = False
        if 'temporal_current' in kwargs:
            temp_current = bool(kwargs.pop('temporal_current'))
        self.temporal_current = temp_current
        temp_sequenced = False
        if 'temporal_sequenced' in kwargs:
            temp_sequenced = bool(kwargs.pop('temporal_sequenced'))
        self.temporal_sequenced = temp_sequenced
        super(TemporalForeignKey, self).__init__(*args, **kwargs)


ForeignKey = TemporalForeignKey

