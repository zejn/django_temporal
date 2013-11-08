
import datetime
import unittest2
from django.test import TestCase
from django.db import connection
from django.db.utils import IntegrityError
from django_temporal.db.models.fields import Period, DateRange, TIME_CURRENT, TZDatetime
from models import Category, CategoryToo, ReferencedTemporalFK, BothTemporalFK, DateTestModel, NullEmptyFieldModel


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
        p1 = DateRange('[2010-01-04, 9999-12-31)')
        p2 = DateRange('[2008-05-01, 9999-12-31)')
        p3 = DateRange('[2000-01-01, 2009-01-01)')
        self.assertEqual(p1.overlaps(p2), True)
        self.assertEqual(p2.overlaps(p1), True)
        
        self.assertEqual(p1.overlaps(p3), False)
        self.assertEqual(p3.overlaps(p1), False)
        
        self.assertEqual(p2.overlaps(p3), True)
        self.assertEqual(p3.overlaps(p2), True)
        
        

class TestPeriod(TestCase):
    def runTest(self):
        p = Period('[2000-01-01 12:00:00.000000+0000,2000-02-01 12:00:00.000000+0000]')
        
        # periods are always saved and displayed in closed-open notation
        self.assertEqual(p.start_included, True)
        self.assertEqual(p.end_included, False)
        
        self.assertEqual(p.prior(), datetime.datetime(2000, 1, 1, 11, 59, 59, 999999))
        self.assertEqual(p.first(), datetime.datetime(2000, 1, 1, 12, 0, 0, 0))
        self.assertEqual(p.last(), datetime.datetime(2000, 2, 1, 12, 0, 0, 1))
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
        self.assertEqual(unicode(p), u'[2000-01-01 12:00:00.000001+0000,9999-12-31 23:59:59.999999+0000)')
        self.assertEqual(p.is_current(), True)
        
        try:
            p.end_included = True
            p.normalize()
        except OverflowError, e:
            pass
        else:
            self.fail("Should raise OverflowError on datetime")
        
        p = Period('(2000-01-01 12:00:00.000000+0000,2000-02-01 12:00:00.000000+0000]')
        self.assertEqual(p.prior(), datetime.datetime(2000, 1, 1, 12, 0))
        self.assertEqual(p.first(), datetime.datetime(2000, 1, 1, 12, 0, 0, 1))
        
        self.assertEqual(p.last(), datetime.datetime(2000, 2, 1, 12, 0, 0, 1))
        self.assertEqual(p.later(), datetime.datetime(2000, 2, 1, 12, 0, 0, 1))
        
        p1 = Period('[2000-01-01 12:00:00.000000+0000,2000-02-01 12:00:00.000000+0000)')
        p2 = Period('[2000-01-14 12:00:00.000000+0000,2000-02-15 12:00:00.000000+0000)')
        
        self.assertEqual(p1.overlaps(p2), True)
        self.assertEqual(p1 < p2, True)

        inter = p1.intersection(p2)
        self.assertEqual(inter, p1 * p2)
        self.assertEqual(inter.start, datetime.datetime(2000, 1, 14, 12, 0, 0, 0))
        self.assertEqual(inter.end, datetime.datetime(2000, 2, 1, 12, 0, 0, 0))
        self.assertEqual(unicode(inter), '[2000-01-14 12:00:00.000000+0000,2000-02-01 12:00:00.000000+0000)')
        
        union = p1.union(p2)
        self.assertEqual(union, p1 + p2)
        self.assertEqual(union.start, datetime.datetime(2000, 1, 1, 12, 0, 0, 0))
        self.assertEqual(union.end, datetime.datetime(2000, 2, 15, 12, 0, 0, 0))
        
        p4 = Period('[2000-02-01 12:00:00.000000+0000,2000-02-14 12:00:00.000000+0000)')
        p5 = Period('[2000-02-01 12:00:00.000000+0000,2000-02-15 12:00:00.000000+0000)')
        
        self.assertEqual(p4 < p5, True)
        self.assertEqual([p4, p5], sorted([p5, p4]))
        

class TestPostgreSQL(TestCase):
    def runTest(self):
        p = Period('[2000-01-01 12:00:00.000000+0000,2000-02-01 12:00:00.000000+0000]')
        v = Category(id=6, cat='123321', valid_time=p)
        v.save()
        
        self.assertEqual(p.last(), datetime.datetime(2000, 2, 1, 12, 0, 0, 1))
        self.assertEqual(p.later(), datetime.datetime(2000, 2, 1, 12, 0, 0, 1))
        
        # prior does not exist in 9.2
        obj1 = Category.objects.get(valid_time__prior=p.prior())
        self.assertEquals(obj1.pk, v.pk)
        
        obj2 = Category.objects.get(valid_time__first=p.first())
        self.assertEquals(obj2.pk, v.pk)
        
        obj3 = Category.objects.get(valid_time__last=p.last())
        self.assertEquals(obj3.pk, v.pk, 'foo')
        
        obj4 = Category.objects.get(valid_time__later=p.later())
        self.assertEquals(obj4.pk, v.pk)

class TestProxyObject(TestCase):
    def runTest(self):
        period = Period(start=datetime.datetime(1996,10,1), end=datetime.datetime(1997,1,1))
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
        period = Period(start=datetime.datetime(1996,10,1), end=datetime.datetime(1997,1,1))
        i = Category(cat=120033, valid_time=period)
        
        # Test overlapping periods - sequenced unique.
        i.valid_time = '[1996-05-01 00:00:00.000000+0000,1996-07-01 00:00:00.000000+0000)'
        try:
            i.save()
        except IntegrityError:
            connection.connection.rollback()
        else:
            self.fail('Should throw an IntegrityError')
        
        # Test current unique.
        i.valid_time = '[1996-05-01 00:00:00.000000+0000,9999-12-31 23:59:59.999999+0000)'
        try:
            i.save()
        except IntegrityError:
            connection.connection.rollback()
        else:
            self.fail('Should throw an IntegrityError')
        
        # Test nonsequenced unique.
        i1 = CategoryToo(cat=120033, valid_time=period)
        i1.save()
        
        i2 = CategoryToo(cat=120033, valid_time=period)
        try:
            i2.save()
        except IntegrityError:
            connection.connection.rollback()
        else:
            self.fail('Should throw and IntegrityError')
        
        i2.cat = 100100
        # Saves okay.
        i2.save()
        
        i1.delete()
        i2.delete()

class TestOperatorWhereLookups(TestCase):
    def test07_operator_where_lookups(self):
        i = Category.objects.get(pk=1)
        self.assertEqual(i.valid_time, Period(start=datetime.datetime(1996,1,1), end=datetime.datetime(1996,6,1)))
        
        equals = Period(start=datetime.datetime(1996,1,1), end=datetime.datetime(1996,6,1))
        result = Category.objects.filter(valid_time=equals)
        self.assertEqual(result.count(), 1)
        self.assertEqual(result[0].pk, i.pk)
        
        result = Category.objects.filter(valid_time__nequals=equals)
        self.assertEqual(result.count() > 1, True)
        self.assertEqual(i.pk in [v.pk for v in result], False)
        
        contains = Period(start=datetime.datetime(1996,3,1), end=datetime.datetime(1996,3,2))
        result = Category.objects.filter(valid_time__contains=contains)
        self.assertEqual(result.count(), 1)
        self.assertEqual(result[0].pk, i.pk)
        
        contained_by = Period(start=datetime.datetime(1995,12,1), end=datetime.datetime(1996,7,1))
        result = Category.objects.filter(valid_time__contained_by=contained_by)
        self.assertEqual(result.count(), 1)
        self.assertEqual(result[0].pk, i.pk)
        
        overlaps = Period(start=datetime.datetime(1995,12,1), end=datetime.datetime(1996,3,1))
        result = Category.objects.filter(valid_time__overlaps=overlaps)
        self.assertEqual(result.count(), 1)
        self.assertEqual(result[0].pk, i.pk)
        
        before = Period(start=datetime.datetime(1996,6,1), end=datetime.datetime(1996,10,1))
        result = Category.objects.filter(valid_time__before=before)
        self.assertEqual(result.count(), 1)
        self.assertEqual(result[0].pk, i.pk)
        
        after = Period(start=datetime.datetime(1995,6,1), end=datetime.datetime(1996,1,1))
        result = Category.objects.filter(valid_time__after=after)
        self.assertEqual(result.count() > 1, True)
        self.assertEqual(i.pk in [v.pk for v in result], True)
        
        # All timestamps in valid_time should be less than or equal to next(overleft)
        overleft = Period(start=datetime.datetime(1996,6,1), end=datetime.datetime(1996,7,1))
        result = Category.objects.filter(valid_time__overleft=overleft)
        self.assertEqual(result.count(), 1)
        self.assertEqual(result[0].pk, i.pk)
        
        # All timestamps in valid_time should be greater than or equal to prior(overright)
        overright = Period(start=datetime.datetime(1996,1,1), end=datetime.datetime(1996,7,1))
        result = Category.objects.filter(valid_time__overright=overright)
        self.assertEqual(result.count() > 1, True)
        self.assertEqual(i.pk in [v.pk for v in result], True)

class TestFunctionWhereLookups(TestCase):
    def runTest(self):
        
        i = Category.objects.get(pk=1)
        self.assertEqual(i.valid_time, Period(start=datetime.datetime(1996,1,1), end=datetime.datetime(1996,6,1)))
        
        p = Period(start=datetime.datetime(1996,6,1), end=datetime.datetime(1996,7,1))
        result = Category.objects.filter(valid_time__adjacent=p)
        self.assertEqual(result.count(), 1)
        self.assertEqual(result[0].pk, i.pk)

class TestCurrentForeignKey(TestCase):
    def runTest(self):
        
        # only referenced table is temporal
        v1 = Category.objects.get(pk=1)
        v4 = Category.objects.get(pk=4)
        
        self.assertEqual(v1.valid_time.end, datetime.datetime(1996, 6, 1))
        self.assertEqual(v4.valid_time.end, TIME_CURRENT)
        
        tfk1 = ReferencedTemporalFK(name='Will fail', category=v1)
        try:
            tfk1.save()
        except IntegrityError:
            connection.connection.rollback()
        else:
            tfk1
            self.fail('Should raise an IntegrityError')
        
        tfk2 = ReferencedTemporalFK(name='Shall pass', category=v4)
        tfk2.save()
        
        # both tables are temporal
        p = Period(start=datetime.datetime(2000, 1, 1, 12, 0), end=TIME_CURRENT)
        tfk3 = BothTemporalFK(name='Wont do', category=v1, validity_time=p)
        try:
            tfk3.save()
        except IntegrityError:
            connection.connection.rollback()
        else:
            self.fail('Should raise an IntegrityError')
        
        tfk4 = BothTemporalFK(name='Will do', category=v4, validity_time=p)
        tfk4.save()

class TestDateRange(TestCase):
    def runTest(self):
        p = DateRange('[2000-01-01, 2000-02-01]')
        
        self.assertEqual(p.prior(), datetime.date(1999, 12, 31))
        self.assertEqual(p.first(), datetime.date(2000, 1, 1))
        self.assertEqual(p.last(), datetime.date(2000, 2, 2))
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
        self.assertEqual(unicode(p), u'[2000-01-02,9999-12-31)')
        self.assertEqual(p.is_current(), True)
        
        try:
            p.end_included = True
            p.normalize()
        except OverflowError, e:
            pass
        else:
            self.fail("Should raise OverflowError on datetime")

class TestDateRangeQueries(TestCase):
    def runTest(self):
        p1 = DateRange(u'[2000-01-01,9999-12-31)')
        d1 = DateTestModel(cat=1, date_seen=p1)
        d1.save()
        
        try:
            a = DateRange(1, 2)
        except TypeError, e:
            pass
        else:
            self.fail("Should raise TypeError")
        
        self.assertEqual(p1, DateRange(datetime.date(2000, 1, 1), datetime.date(9999, 12, 31)))

        p2 = DateRange(u'[2010-10-10,9999-12-31)')
        d2 = DateTestModel(cat=2, date_seen=p2)
        d2.save()
        
        self.assertEqual(DateTestModel.objects.all().count(), 2)
        qs = DateTestModel.objects.filter(date_seen__first=datetime.date(2010, 10, 10))
        self.assertEqual(qs[0].pk, d2.pk)
        
        qs = DateTestModel.objects.filter(date_seen__prior=datetime.date(1999, 12, 31))
        self.assertEqual(qs[0].pk, d1.pk)
    

class TestNullEmptyField(TestCase):
    def runTest(self):
        m1 = NullEmptyFieldModel()
        m1.valid = DateRange(empty=True)
        m1.save()
        
        m2 = NullEmptyFieldModel()
        m2.valid = DateRange(u'[2010-10-10,9999-12-31)')
        m2.save()
        
        m3 = NullEmptyFieldModel()
        m3.valid = None
        m3.save()
        
        self.assertEqual(unicode(m1.valid), 'empty')
        self.assertEqual(NullEmptyFieldModel.objects.filter(valid__isempty=True).count(), 1)
        self.assertEqual(NullEmptyFieldModel.objects.filter(valid__isempty=False).count(), 1)
        self.assertEqual(NullEmptyFieldModel.objects.filter(valid__isnull=True).count(), 1)
        self.assertEqual(NullEmptyFieldModel.objects.filter(valid__isnull=False).count(), 2)
        
        
    
