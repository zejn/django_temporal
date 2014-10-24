
import datetime
import unittest2
from django.test import TestCase
from django.db import connection, transaction
from django.db.utils import IntegrityError
from django_temporal.db.models.fields import Period, DateRange, TIME_CURRENT, TZDatetime
from models import Category, CategoryToo, ReferencedTemporalFK, BothTemporalFK, DateTestModel, NullEmptyFieldModel, DateMergeModel, DateMergeModelNull, DateTimeMergeModel

from contextlib import contextmanager

try:
    from django.db.transaction import atomic
    # Django 1.6
    @contextmanager
    def _fail_atomic():
        try:
            with atomic():
                yield
        except IntegrityError:
            pass
        else:
            self.fail('fail')
except ImportError:
    # Django 1.4, 1.5
    @contextmanager
    def _fail_atomic():
        try:
            with transaction.commit_on_success():
                yield
        except IntegrityError:
            connection.connection.rollback()
        else:
            self.fail('fail')



class TestFixtures(TestCase):
    def runTest(self):
        self.assertEqual(5, Category.objects.count())

class TestParseTimestamp(TestCase):
    def runTest(self):
        from django_temporal.db.models.fields import TZ_OFFSET
        
        m = TZ_OFFSET.match('2000-01-01 12:00:00.000000+0000')
        self.assertEqual(m.groups(), ('2000-01-01 12:00:00.000000', '+', '00', '00'))
        
        m = TZ_OFFSET.match('2009-06-04 12:00:00 +0100')
        self.assertEqual(m.groups(), ('2009-06-04 12:00:00', '+', '01', '00'))
        
        m = TZ_OFFSET.match('"2009-06-04 12:00:00 +0100"')
        self.assertEqual(m.groups(), ('2009-06-04 12:00:00', '+', '01', '00'))

class TestOverlaps(TestCase):
    def runTest(self):
        p1 = DateRange('[2010-01-04, 9999-12-30)')
        p2 = DateRange('[2008-05-01, 9999-12-30)')
        p3 = DateRange('[2000-01-01, 2009-01-01)')
        self.assertEqual(p1.overlaps(p2), True)
        self.assertEqual(p2.overlaps(p1), True)
        
        self.assertEqual(p1.overlaps(p3), False)
        self.assertEqual(p3.overlaps(p1), False)
        
        self.assertEqual(p2.overlaps(p3), True)
        self.assertEqual(p3.overlaps(p2), True)
        
        d1 = DateRange('[2011-03-01, 2011-05-01)')
        d2 = DateRange('[2011-05-01, 9999-12-30)')
        
        self.assertEqual(d1.overlaps(d2), False)
        self.assertEqual(d2.overlaps(d1), False)
        

class TestPeriod(TestCase):
    def runTest(self):
        p = Period('[2000-01-01 12:00:00.000000+0000,2000-02-01 12:00:00.000000+0000]')
        
        # periods are always saved and displayed in closed-open notation
        self.assertEqual(p.start_included, True)
        self.assertEqual(p.end_included, False)
        
        self.assertEqual(p.prior(), datetime.datetime(2000, 1, 1, 11, 59, 59, 999999))
        self.assertEqual(p.lower, datetime.datetime(2000, 1, 1, 12, 0, 0, 0))
        self.assertEqual(p.upper, datetime.datetime(2000, 2, 1, 12, 0, 0, 1))
        self.assertEqual(p.later(), datetime.datetime(2000, 2, 1, 12, 0, 0, 1))
        
        
        self.assertEqual(unicode(p), u'[2000-01-01 12:00:00.000000+0000,2000-02-01 12:00:00.000001+0000)')
        p.end_included = True
        self.assertEqual(p.end_included, True)
        self.assertEqual(unicode(p), u'[2000-01-01 12:00:00.000000+0000,2000-02-01 12:00:00.000001+0000]')
        
        p.normalize()
        self.assertEqual(p.end_included, False)
        self.assertEqual(unicode(p), u'[2000-01-01 12:00:00.000000+0000,2000-02-01 12:00:00.000002+0000)')
        
        p.start_included = False
        self.assertEqual(p.start_included, False)
        self.assertEqual(unicode(p), u'(2000-01-01 12:00:00.000000+0000,2000-02-01 12:00:00.000002+0000)')
        p.normalize()
        self.assertEqual(p.start_included, True)
        self.assertEqual(unicode(p), u'[2000-01-01 12:00:00.000001+0000,2000-02-01 12:00:00.000002+0000)')
        
        # test current
        self.assertEqual(p.is_current(), False)
        
        p.set_current()
        self.assertEqual(unicode(p), u'[2000-01-01 12:00:00.000001+0000,9999-12-30 00:00:00.000000+0000)')
        self.assertEqual(p.is_current(), True)
        
        p = Period('(2000-01-01 12:00:00.000000+0000,2000-02-01 12:00:00.000000+0000]')
        self.assertEqual(p.prior(), datetime.datetime(2000, 1, 1, 12, 0))
        self.assertEqual(p.lower, datetime.datetime(2000, 1, 1, 12, 0, 0, 1))
        
        self.assertEqual(p.upper, datetime.datetime(2000, 2, 1, 12, 0, 0, 1))
        self.assertEqual(p.later(), datetime.datetime(2000, 2, 1, 12, 0, 0, 1))
        
        p1 = Period('[2000-01-01 12:00:00.000000+0000,2000-02-01 12:00:00.000000+0000)')
        p2 = Period('[2000-01-14 12:00:00.000000+0000,2000-02-15 12:00:00.000000+0000)')
        
        self.assertEqual(p1.overlaps(p2), True)
        self.assertEqual(p1 < p2, True)

        inter = p1.intersection(p2)
        self.assertEqual(inter, p1 * p2)
        self.assertEqual(inter.lower, datetime.datetime(2000, 1, 14, 12, 0, 0, 0))
        self.assertEqual(inter.upper, datetime.datetime(2000, 2, 1, 12, 0, 0, 0))
        self.assertEqual(unicode(inter), '[2000-01-14 12:00:00.000000+0000,2000-02-01 12:00:00.000000+0000)')
        
        union = p1.union(p2)
        self.assertEqual(union, p1 + p2)
        self.assertEqual(union.lower, datetime.datetime(2000, 1, 1, 12, 0, 0, 0))
        self.assertEqual(union.upper, datetime.datetime(2000, 2, 15, 12, 0, 0, 0))
        
        p4 = Period('[2000-02-01 12:00:00.000000+0000,2000-02-14 12:00:00.000000+0000)')
        p5 = Period('[2000-02-01 12:00:00.000000+0000,2000-02-15 12:00:00.000000+0000)')
        
        self.assertEqual(p4 < p5, True)
        self.assertEqual([p4, p5], sorted([p5, p4]))
        

class TestPostgreSQL(TestCase):
    def runTest(self):
        p = Period('[2000-01-01 12:00:00.000000+0000,2000-02-01 12:00:00.000000+0000]')
        v = Category(id=6, cat='123321', valid_time=p)
        v.save()
        
        self.assertEqual(p.upper, datetime.datetime(2000, 2, 1, 12, 0, 0, 1))
        self.assertEqual(p.later(), datetime.datetime(2000, 2, 1, 12, 0, 0, 1))
        
        # prior does not exist in 9.2
        obj1 = Category.objects.get(valid_time__prior=p.prior())
        self.assertEquals(obj1.pk, v.pk)
        
        obj2 = Category.objects.get(valid_time__lower=p.lower)
        self.assertEquals(obj2.pk, v.pk)
        
        obj3 = Category.objects.get(valid_time__upper=p.upper)
        self.assertEquals(obj3.pk, v.pk, 'foo')
        
        obj4 = Category.objects.get(valid_time__later=p.later())
        self.assertEquals(obj4.pk, v.pk)

class TestProxyObject(TestCase):
    def runTest(self):
        period = Period(lower=datetime.datetime(1996,10,1), upper=datetime.datetime(1997,1,1))
        i = Category(cat=120033, valid_time=period)
        i.save()
        
        for bad in [5, 2.0, 'foo', (10, 20)]:
            try:
                i.valid_time = bad
            except TypeError:
                pass
            else:
                self.fail('Should throw a TypeError')
        
        newstr = '[1996-01-01 00:00:00.000000+0000,1996-06-01 00:00:00.000000+0000)'
        new = Period(newstr)
        
        for good in (new, newstr):
            i.valid_time = good
        
        self.assertEqual(i.valid_time, new)
        i.save()
        self.assertNotEqual(i.pk, None)
        self.assertEqual(new, Category.objects.get(pk=i.pk).valid_time)
        
        i.delete()

class TestFieldOptions(TestCase):
    def runTest(self):
        "Test period field options - unique types (sequenced, current and nonsequenced)"
        period = Period(lower=datetime.datetime(1996,10,1), upper=datetime.datetime(1997,1,1))
        i = Category(cat=120033, valid_time=period)
        
        # Test overlapping periods - sequenced unique.
        i.valid_time = '[1996-05-01 00:00:00.000000+0000,1996-07-01 00:00:00.000000+0000)'
        
        with _fail_atomic():
            i.save()
        
        # Test current unique.
        i.valid_time = '[1996-05-01 00:00:00.000000+0000,9999-12-30 00:00:00.000000+0000)'
        with _fail_atomic():
            i.save()
        
        # Test nonsequenced unique.
        i1 = CategoryToo(cat=120033, valid_time=period)
        i1.save()
        
        i2 = CategoryToo(cat=120033, valid_time=period)
        with _fail_atomic():
            i2.save()
        
        i2.cat = 100100
        # Saves okay.
        i2.save()
        
        i1.delete()
        i2.delete()

class TestOperatorWhereLookups(TestCase):
    def test07_operator_where_lookups(self):
        i = Category.objects.get(pk=1)
        self.assertEqual(i.valid_time, Period(lower=datetime.datetime(1996,1,1), upper=datetime.datetime(1996,6,1)))
        
        equals = Period(lower=datetime.datetime(1996,1,1), upper=datetime.datetime(1996,6,1))
        result = Category.objects.filter(valid_time=equals)
        self.assertEqual(result.count(), 1)
        self.assertEqual(result[0].pk, i.pk)
        
        result = Category.objects.filter(valid_time__nequals=equals)
        self.assertEqual(result.count() > 1, True)
        self.assertEqual(i.pk in [v.pk for v in result], False)
        
        contains = Period(lower=datetime.datetime(1996,3,1), upper=datetime.datetime(1996,3,2))
        result = Category.objects.filter(valid_time__contains=contains)
        self.assertEqual(result.count(), 1)
        self.assertEqual(result[0].pk, i.pk)
        
        contained_by = Period(lower=datetime.datetime(1995,12,1), upper=datetime.datetime(1996,7,1))
        result = Category.objects.filter(valid_time__contained_by=contained_by)
        self.assertEqual(result.count(), 1)
        self.assertEqual(result[0].pk, i.pk)
        
        overlaps = Period(lower=datetime.datetime(1995,12,1), upper=datetime.datetime(1996,3,1))
        result = Category.objects.filter(valid_time__overlaps=overlaps)
        self.assertEqual(result.count(), 1)
        self.assertEqual(result[0].pk, i.pk)
        
        before = Period(lower=datetime.datetime(1996,6,1), upper=datetime.datetime(1996,10,1))
        result = Category.objects.filter(valid_time__before=before)
        self.assertEqual(result.count(), 1)
        self.assertEqual(result[0].pk, i.pk)
        
        after = Period(lower=datetime.datetime(1995,6,1), upper=datetime.datetime(1996,1,1))
        result = Category.objects.filter(valid_time__after=after)
        self.assertEqual(result.count() > 1, True)
        self.assertEqual(i.pk in [v.pk for v in result], True)
        
        # All timestamps in valid_time should be less than or equal to next(overleft)
        overleft = Period(lower=datetime.datetime(1996,6,1), upper=datetime.datetime(1996,7,1))
        result = Category.objects.filter(valid_time__overleft=overleft)
        self.assertEqual(result.count(), 1)
        self.assertEqual(result[0].pk, i.pk)
        
        # All timestamps in valid_time should be greater than or equal to prior(overright)
        overright = Period(lower=datetime.datetime(1996,1,1), upper=datetime.datetime(1996,7,1))
        result = Category.objects.filter(valid_time__overright=overright)
        self.assertEqual(result.count() > 1, True)
        self.assertEqual(i.pk in [v.pk for v in result], True)

class TestFunctionWhereLookups(TestCase):
    def runTest(self):
        
        i = Category.objects.get(pk=1)
        self.assertEqual(i.valid_time, Period(lower=datetime.datetime(1996,1,1), upper=datetime.datetime(1996,6,1)))
        
        p = Period(lower=datetime.datetime(1996,6,1), upper=datetime.datetime(1996,7,1))
        result = Category.objects.filter(valid_time__adjacent=p)
        self.assertEqual(result.count(), 1)
        self.assertEqual(result[0].pk, i.pk)

class TestCurrentForeignKey(TestCase):
    def runTest(self):
        
        # only referenced table is temporal
        v1 = Category.objects.get(pk=1)
        v4 = Category.objects.get(pk=4)
        
        self.assertEqual(v1.valid_time.upper, datetime.datetime(1996, 6, 1))
        self.assertEqual(v4.valid_time.upper, TIME_CURRENT)
        
        tfk1 = ReferencedTemporalFK(name='Will fail', category=v1)
        with _fail_atomic():
            tfk1.save()
        
        tfk2 = ReferencedTemporalFK(name='Shall pass', category=v4)
        tfk2.save()
        
        with _fail_atomic():
            # bth tables are temporal
            p = Period(lower=datetime.datetime(2000, 1, 1, 12, 0), upper=TIME_CURRENT)
            tfk3 = BothTemporalFK(name='Wont do', category=v1, validity_time=p)
            tfk3.save()
        
        tfk4 = BothTemporalFK(name='Will do', category=v4, validity_time=p)
        tfk4.save()

class TestDateRange(TestCase):
    def runTest(self):
        p = DateRange('[2000-01-01, 2000-02-01]')
        
        self.assertEqual(p.prior(), datetime.date(1999, 12, 31))
        self.assertEqual(p.lower, datetime.date(2000, 1, 1))
        self.assertEqual(p.upper, datetime.date(2000, 2, 2))
        self.assertEqual(p.later(), datetime.date(2000, 2, 2))
        
        # periods are always saved and displayed in closed-open notation
        self.assertEqual(p.start_included, True)
        self.assertEqual(p.end_included, False)
        
        self.assertEqual(unicode(p), u'[2000-01-01,2000-02-02)')
        p.end_included = True
        self.assertEqual(p.end_included, True)
        p.normalize()
        self.assertEqual(p.end_included, False)
        self.assertEqual(unicode(p), u'[2000-01-01,2000-02-03)')
        
        p.start_included = False
        self.assertEqual(p.start_included, False)
        p.normalize()
        self.assertEqual(p.start_included, True)
        self.assertEqual(unicode(p), u'[2000-01-02,2000-02-03)')
        
        # test current
        self.assertEqual(p.is_current(), False)
        
        p.set_current()
        self.assertEqual(unicode(p), u'[2000-01-02,9999-12-30)')
        self.assertEqual(p.is_current(), True)
        

class TestDateRangeQueries(TestCase):
    def runTest(self):
        p1 = DateRange(u'[2000-01-01,9999-12-30)')
        d1 = DateTestModel(cat=1, date_seen=p1)
        d1.save()
        
        try:
            a = DateRange(1, 2)
        except TypeError, e:
            pass
        else:
            self.fail("Should raise TypeError")
        
        self.assertEqual(p1, DateRange(datetime.date(2000, 1, 1), datetime.date(9999, 12, 30)))

        p2 = DateRange(u'[2010-10-10,9999-12-30)')
        d2 = DateTestModel(cat=2, date_seen=p2)
        d2.save()
        
        self.assertEqual(DateTestModel.objects.all().count(), 2)
        qs = DateTestModel.objects.filter(date_seen__lower=datetime.date(2010, 10, 10))
        self.assertEqual(qs[0].pk, d2.pk)
        
        qs = DateTestModel.objects.filter(date_seen__prior=datetime.date(1999, 12, 31))
        self.assertEqual(qs[0].pk, d1.pk)
    

class TestNullEmptyField(TestCase):
    def runTest(self):
        m1 = NullEmptyFieldModel()
        m1.valid = DateRange(empty=True)
        m1.save()
        
        m2 = NullEmptyFieldModel()
        m2.valid = DateRange(u'[2010-10-10,9999-12-30)')
        m2.save()
        
        m3 = NullEmptyFieldModel()
        m3.valid = None
        m3.save()
        
        self.assertEqual(unicode(m1.valid), 'empty')
        self.assertEqual(NullEmptyFieldModel.objects.filter(valid__isempty=True).count(), 1)
        self.assertEqual(NullEmptyFieldModel.objects.filter(valid__isempty=False).count(), 1)
        self.assertEqual(NullEmptyFieldModel.objects.filter(valid__isnull=True).count(), 1)
        self.assertEqual(NullEmptyFieldModel.objects.filter(valid__isnull=False).count(), 2)
        

class TestDateMerge(TestCase):
    def runTest(self):
        
        from django_temporal.utils import merge
        import os
        datafile = lambda x: os.path.join(os.path.abspath(os.path.dirname(__file__)), 'data', x)
        
        datum1 = datetime.date.today() - datetime.timedelta(4)
        datum2 = datetime.date.today() - datetime.timedelta(2)
        datum3 = datetime.date.today()
        
        merge(datafile('daterange_1.csv'),
            DateMergeModel,
            datum1,
            keys=['a'],
            snapshot='full'
            )

        m1 = DateMergeModel.objects.filter(a=3).order_by('-valid')[0]
        self.assertEqual(m1.valid.upper.year, 9999)
        
        merge(datafile('daterange_2.csv'),
            DateMergeModel,
            datum2,
            keys=['a'],
            snapshot='full'
            )
        
        m2 = DateMergeModel.objects.filter(a=3).order_by('-valid')[0]
        self.assertEqual(m2.valid.upper.year, 9999)

        merge(datafile('daterange_3.csv'),
            DateMergeModel,
            datum3,
            keys=['a'],
            snapshot='full'
            )
        
        m3 = DateMergeModel.objects.filter(a=3).order_by('-valid')[0]
        self.assertEqual(m3.valid.upper.year, 9999)
        
        m1 = DateMergeModel.objects.get(pk=m1.pk)
        m2 = DateMergeModel.objects.get(pk=m2.pk)
        m3 = DateMergeModel.objects.get(pk=m3.pk)

        self.assertEqual(m1.valid.upper, datum2)
        self.assertEqual(m2.valid.upper, datum3)
        self.assertEqual(m3.valid.upper.year, 9999)

class TestDateMergeWithNullKey(TestCase):
    def runTest(self):
        from django_temporal.utils import merge
        
        import os
        datafile = lambda x: os.path.join(os.path.abspath(os.path.dirname(__file__)), 'data', x)
        
        datum1 = datetime.date.today() - datetime.timedelta(4)
        datum2 = datetime.date.today() - datetime.timedelta(2)
        datum3 = datetime.date.today()
        datum4 = datetime.date.today() + datetime.timedelta(1)

        merge(datafile('daterangenull_1.csv'),
            DateMergeModelNull,
            datum1,
            keys=['k1', 'k2'],
            snapshot='full'
            )

        m1 = DateMergeModelNull.objects.filter(k1='c', k2='test').order_by('-valid')[0]
        self.assertEqual(m1.valid.upper.year, 9999)
        n1 = DateMergeModelNull.objects.filter(k1='x', k2__isnull=True).order_by('-valid')[0]
        self.assertEqual(n1.valid.upper.year, 9999)
        
        merge(datafile('daterangenull_2.csv'),
            DateMergeModelNull,
            datum2,
            keys=['k1', 'k2'],
            snapshot='full'
            )
        
        m2 = DateMergeModelNull.objects.filter(k1='c', k2='test').order_by('-valid')[0]
        self.assertEqual(m2.valid.upper.year, 9999)
        n2 = DateMergeModelNull.objects.filter(k1='x', k2__isnull=True).order_by('-valid')[0]
        self.assertEqual(n2.valid.upper.year, 9999)

        merge(datafile('daterangenull_3.csv'),
            DateMergeModelNull,
            datum3,
            keys=['k1', 'k2'],
            snapshot='full'
            )
        
        m3 = DateMergeModelNull.objects.filter(k1='c', k2='test').order_by('-valid')[0]
        self.assertEqual(m3.valid.upper.year, 9999)
        n3 = DateMergeModelNull.objects.filter(k1='x', k2__isnull=True).order_by('-valid')[0]
        self.assertEqual(n3.valid.upper.year, 9999)
        
        merge(datafile('daterangenull_4.csv'),
            DateMergeModelNull,
            datum4,
            keys=['k1', 'k2'],
            snapshot='full'
            )
        
        self.assertEqual(DateMergeModelNull.objects.filter(k1='x', k2__isnull=True).count(), 2)
        
        m4 = DateMergeModelNull.objects.filter(k1='c', k2='test').order_by('-valid')[0]
        self.assertEqual(m4.valid.upper.year, 9999)
        n4 = DateMergeModelNull.objects.filter(k1='x', k2__isnull=True).order_by('-valid')[0]
        self.assertEqual(n4.valid.upper.year, datum1.year)
        
        

        m1 = DateMergeModelNull.objects.get(pk=m1.pk)
        m2 = DateMergeModelNull.objects.get(pk=m2.pk)
        m3 = DateMergeModelNull.objects.get(pk=m3.pk)
        m4 = DateMergeModelNull.objects.get(pk=m4.pk)

        # check end keys match
        end_keys = [
            ('c', 'test'),
            ('d', 'echo'),
            ('e', 'gold'),
        ]
        end_keys.sort()
        
        found_end_keys = [(i.k1, i.k2) for i in DateMergeModelNull.objects.filter(valid__overlaps=DateRange(datum4, datum4+datetime.timedelta(1)))]
        found_end_keys.sort()
        self.assertEqual(end_keys, found_end_keys)
        

        self.assertEqual(m1.valid.upper, datum2)
        self.assertEqual(m2.valid.upper, datum3)
        self.assertEqual(m3.valid.upper, datum4)
        self.assertEqual(m4.valid.upper.year, 9999)

class TestDateTimeMerge(TestCase):
    def runTest(self):
        
        from django_temporal.utils import merge
        import os
        datafile = lambda x: os.path.join(os.path.abspath(os.path.dirname(__file__)), 'data', x)
        
        datum1 = datetime.datetime.now() - datetime.timedelta(0, 3600*10)
        datum2 = datetime.datetime.now() - datetime.timedelta(0, 3600*2)
        datum3 = datetime.datetime.now()
        
        merge(datafile('daterange_1.csv'),
            DateTimeMergeModel,
            datum1,
            keys=['a'],
            snapshot='full'
            )

        m1 = DateTimeMergeModel.objects.filter(a=3).order_by('-valid')[0]
        self.assertEqual(m1.valid.upper.year, 9999)
        
        merge(datafile('daterange_2.csv'),
            DateTimeMergeModel,
            datum2,
            keys=['a'],
            snapshot='full'
            )
        
        m2 = DateTimeMergeModel.objects.filter(a=3).order_by('-valid')[0]
        self.assertEqual(m2.valid.upper.year, 9999)

        merge(datafile('daterange_3.csv'),
            DateTimeMergeModel,
            datum3,
            keys=['a'],
            snapshot='full'
            )
        
        m3 = DateTimeMergeModel.objects.filter(a=3).order_by('-valid')[0]
        self.assertEqual(m3.valid.upper.year, 9999)
        
        m1 = DateTimeMergeModel.objects.get(pk=m1.pk)
        m2 = DateTimeMergeModel.objects.get(pk=m2.pk)
        m3 = DateTimeMergeModel.objects.get(pk=m3.pk)

        self.assertEqual(m1.valid.upper, datum2)
        self.assertEqual(m2.valid.upper, datum3)
        self.assertEqual(m3.valid.upper.year, 9999)
        
class TestCopyField(TestCase):
    def runTest(self):
        
        from django_temporal.utils import merge
        from django.db import connection
        from temporal.models import CopyFieldModel
        import os
        datafile = lambda x: os.path.join(os.path.abspath(os.path.dirname(__file__)), 'data', x)
        
        datum1 = datetime.date.today() - datetime.timedelta(4)
        datum2 = datetime.date.today() - datetime.timedelta(2)
        datum3 = datetime.date.today()
        
        cur = connection.connection.cursor()
        
        initial = open(datafile('copyfield_0.csv'))
        cur.copy_expert('COPY %s FROM stdin WITH CSV HEADER' % CopyFieldModel._meta.db_table, initial)
        
        cur.execute('SELECT max(id) FROM %s' % CopyFieldModel._meta.db_table)
        maxid = cur.fetchall()[0][0]
        cur.execute(''' SELECT SETVAL('%s_id_seq', %s) FROM %s;''' % (
            CopyFieldModel._meta.db_table, maxid, CopyFieldModel._meta.db_table))
        
        
        #merge(datafile('copyfield_0.csv'),
            #DateMergeModel,
            ##datum1,
            #keys=['a'],
            #snapshot='full'
            #)
        #return
    
        m1 = CopyFieldModel.objects.filter(a=3).order_by('-valid')[0]
        self.assertEqual(m1.valid.upper.year, 9999)
        
        merge(datafile('copyfield_1.csv'),
            CopyFieldModel,
            datum2,
            keys=['a'],
            snapshot='full',
            copy_fields=['c'],
            debug=True
            )
        
        m2 = CopyFieldModel.objects.filter(a=3).order_by('-valid')[0]
        self.assertEqual(m2.valid.upper.year, 9999)

        merge(datafile('copyfield_2.csv'),
            CopyFieldModel,
            datum3,
            keys=['a'],
            snapshot='full',
            copy_fields=['c'],
            debug=True
            )
        
        m3 = CopyFieldModel.objects.filter(a=3).order_by('-valid')[0]
        self.assertEqual(m3.valid.upper.year, 9999)
        
        m1 = CopyFieldModel.objects.get(pk=m1.pk)
        m2 = CopyFieldModel.objects.get(pk=m2.pk)
        m3 = CopyFieldModel.objects.get(pk=m3.pk)

        self.assertEqual(m1.valid.upper, datum3)
        self.assertEqual(m2.valid.upper, datum3)
        self.assertEqual(m3.valid.upper.year, 9999)
        
        import sys
        cur.copy_expert('COPY (SELECT * FROM %s ORDER BY a) TO stdout' % CopyFieldModel._meta.db_table, sys.stdout);




