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

TIME_CURRENT = datetime(9999, 12, 31, 23, 59, 59, 999999)
TIME_RESOLUTION = timedelta(0, 0, 1) # = 1 microsecond
DATE_CURRENT = date(9999, 12, 31)
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
    
    def __init__(self, period=None, start=None, end=None, empty=False):
        self.empty = False
        if empty:
            self.empty = True
            return
        
        if isinstance(period, (self._input_type, date)): # XXX FIXME argument rewriting isn't ok
            start, end, period = period, start, None
        types = [basestring, self.__class__]
        if pgrange is not None:
            types.append(self.pg_dbvalue)
        if not (isinstance(period, tuple(types)) or isinstance(start, self._input_type)):
            raise TypeError("You must specify either period (string or Period) or start (TZDatetime or %s), got period=%r start=%r" % (self._input_type, period.__class__, start.__class__))
        
        if period is not None:
            if isinstance(period, basestring):
                m = re.match('^([\[\(])([^,]+),([^\]\)]+)([\]\)])$', period.strip())
                if not m:
                    if period.strip() == EMPTY or period.strip() == u'':
                        self.empty = True
                        return
                    raise TypeError("Invalid period string representation: %s" % repr(period))
                start_in, start, end, end_in = m.groups()
                
                self.start = self.subvalue_class().to_python(start.strip())
                self.end = self.subvalue_class().to_python(end.strip())
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
                self.start = self.subvalue_class().to_python(period.lower)
                self.start_included = bool(period.lower_inc)
                self.end = self.subvalue_class().to_python(period.upper)
                self.end_included = bool(period.upper_inc)
            elif isinstance(period, self.__class__):
                if period.empty:
                    self.empty = period.empty
                    return
                self.start = period.start
                self.start_included = period.start_included
                self.end = period.end
                self.end_included = period.end_included
        else:
            self.start = start
            self.start_included = True
            if end is not None:
                self.end = end
                self.end_included = False
            else:
                self.end = self._value_current
                self.end_included = True
        self.normalize()
    
    def start():
        def fget(self):
            return self.__start
        def fset(self, value):
            if isinstance(value, TZDatetime):
                self.__start = value.replace(tzinfo=None)
            elif isinstance(value, self._input_type):
                self.__start = self.subvalue_class().to_python(value.strftime(u'%Y-%m-%d %H:%M:%S.%f%z')).replace(tzinfo=None)
            else:
                raise AssertionError("should never happen")
        return (fget, fset, None, "start of period")
    start = property(*start())
    
    def start_included():
        def fget(self):
            return self.__start_included
        def fset(self, value):
            if not value in (True, False):
                raise ValueError("Must be True or False")
            self.__start_included = value
        return (fget, fset, None, "denotes if start timestamp is open or closed")
    start_included = property(*start_included())
    
    def end():
        def fget(self):
            return self.__end
        def fset(self, value):
            if isinstance(value, TZDatetime):
                self.__end = value.replace(tzinfo=None)
            elif isinstance(value, self._input_type):
                self.__end = self.subvalue_class().to_python(value.strftime(u'%Y-%m-%d %H:%M:%S.%f%z')).replace(tzinfo=None)
            else:
                raise AssertionError("should never happen")
        return (fget, fset, None, "end of period")
    end = property(*end())
    
    def end_included():
        def fget(self):
            return self.__end_included
        def fset(self, value):
            if not value in (True, False):
                raise ValueError("Must be True or False")
            self.__end_included = value
        return (fget, fset, None, "denotes if end timestamp is open or closed")
    end_included = property(*end_included())
    
    def __eq__(self, other):
        if self.empty and other.empty:
            return True
        if (self.empty and not other.empty) or (not self.empty and other.empty):
            return False
        if  self.start_included == other.start_included and \
            self.end_included == other.end_included and \
            self.start == other.start and\
            self.end == other.end:
            return True
        return False
    
    def normalize(self):
        if not self.start_included:
            if self.start is not None:
                self.start += self._value_resolution
            self.start_included = True
        if self.end_included:
            if self.end is not None:
                self.end += self._value_resolution
            self.end_included = False
    
    def is_current(self):
        if self.empty:
            return False
        if self.end == self._value_current and self.end_included == False:
            return True
        return False
    
    def set_current(self):
        self.end = self._value_current
        self.end_included = False
    
    def first(self):
        return self.start
    
    def prior(self):
        if self.start_included:
            return self.start - self._value_resolution
        return self.start
    
    def last(self):
        return self.end
    
    def later(self):
        if self.end_included:
            return self.end + self._value_resolution
        return self.end
    
    def overlaps(self, other):
        if self.empty or other.empty:
            return False
        if (self.start <= other.start and self.end > other.start) or \
                (self.start <= other.end and self.end > other.end) or \
                (other.start <= self.start and other.end > self.end):
            return True
        return False
    
    def __unicode__(self):
        if self.empty:
            return EMPTY
        return u''.join([
            self.start_included and u'[' or u'(',
            self._value_unicode(self.start),
            u',',
            self._value_unicode(self.end),
            self.end_included and ']' or ')',
            ])
    
    def __repr__(self):
        if self.empty:
            return '<%s empty>' % (self.__class__.__name__,)
        return '<%s from %s to %s>' % (self.__class__.__name__, self._value_unicode(self.start), self._value_unicode(self.end))
    
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

    def start():
        def fget(self):
            return self.__start
        def fset(self, value):
            if isinstance(value, date):
                self.__start = self.subvalue_class().to_python(value.strftime(u'%Y-%m-%d'))
            elif value is None:
                self.__start = None
            else:
                raise AssertionError("should never happen")
        return (fget, fset, None, "start of date range")
    start = property(*start())
    
    def end():
        def fget(self):
            return self.__end
        def fset(self, value):
            if isinstance(value, date):
                self.__end = self.subvalue_class().to_python(value.strftime(u'%Y-%m-%d'))
            elif value is None:
                self.__end = None
            else:
                raise AssertionError("should never happen")
        return (fget, fset, None, "end of date range")
    end = property(*end())


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
        if lookup_type in (
                'exact', 'lt', 'lte', 'gt', 'gte', 'nequals', 'contains', 'contained_by',
                'overlaps', 'before', 'after', 'overleft', 'overright', 'adjacent'):
            if not isinstance(value, self.value_class):
                value = self.value_class(value)
            return unicode(value)
        if lookup_type in ('prior', 'first', 'last', 'later'):
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
        elif lookup_type in ('prior', 'first', 'last', 'later'):
            return [value]
        elif lookup_type in ('isempty', 'isnull'):
            return [value]
    
    def get_db_prep_value(self, value, connection, prepared=False):
        if isinstance(value, datetime):
            return models.DateTimeField().get_db_prep_value(value, connection, prepared)
        else:
            if self.null and value is None:
                return None
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

