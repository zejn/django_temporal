
import datetime
import unittest
from django.test import TestCase
from django.db import connection
from django.db.utils import IntegrityError
from django_temporal.db.models.fields import Period, TIME_CURRENT
from models import Category, CategoryToo, ReferencedTemporalFK, BothTemporalFK


class TemporalModelTest(TestCase):
	def test01_fixtures(self):
		self.assertEqual(5, Category.objects.count())
	
	def test02_parse_timestamp(self):
		from django_temporal.db.models.fields import TZ_OFFSET
		
		m = TZ_OFFSET.match('2000-01-01 12:00:00.000000+0000')
		self.assertEqual(m.groups(), ('2000-01-01 12:00:00.000000', '+', '00', '00'))
		
		m = TZ_OFFSET.match('2009-06-04 12:00:00 +0100')
		self.assertEqual(m.groups(), ('2009-06-04 12:00:00', '+', '01', '00'))
		
		m = TZ_OFFSET.match('"2009-06-04 12:00:00 +0100"')
		self.assertEqual(m.groups(), ('2009-06-04 12:00:00', '+', '01', '00'))
	
	def test03_period(self):
		p = Period('[2000-01-01 12:00:00.000000+0000,2000-02-01 12:00:00.000000+0000]')
		
		self.assertEqual(p.prior(), datetime.datetime(2000, 1, 1, 11, 59, 59, 999999))
		self.assertEqual(p.first(), datetime.datetime(2000, 1, 1, 12, 0, 0, 0))
		self.assertEqual(p.last(), datetime.datetime(2000, 2, 1, 12, 0, 0, 0))
		self.assertEqual(p.next(), datetime.datetime(2000, 2, 1, 12, 0, 0, 1))
		
		# periods are always saved and displayed in closed-open notation
		self.assertEqual(p.start_included, True)
		self.assertEqual(p.end_included, False)
		
		self.assertEqual(unicode(p), u'[2000-01-01 12:00:00.000000+0000,2000-02-01 12:00:00.000001+0000)')
		p.end_included = True
		self.assertEqual(p.end_included, False)
		self.assertEqual(unicode(p), u'[2000-01-01 12:00:00.000000+0000,2000-02-01 12:00:00.000002+0000)')
		
		p.start_included = False
		self.assertEqual(p.start_included, True)
		self.assertEqual(unicode(p), u'[2000-01-01 12:00:00.000001+0000,2000-02-01 12:00:00.000002+0000)')
		
		# test current
		self.assertEqual(p.is_current(), False)
		
		p.set_current()
		self.assertEqual(unicode(p), u'[2000-01-01 12:00:00.000001+0000,9999-12-31 23:59:59.999999+0000)')
		self.assertEqual(p.is_current(), True)
		
		try:
			p.end_included = True
		except OverflowError, e:
			pass
		else:
			self.fail("Should raise OverflowError on datetime")
	
	def test04_test_postgresql(self):
		p = Period('[2000-01-01 12:00:00.000000+0000,2000-02-01 12:00:00.000000+0000]')
		v = Category(id=6, cat='123321', valid_time=p)
		v.save()
		
		# prior does not exist in 9.2
		obj1 = Category.objects.get(valid_time__prior=p.prior())
		self.assertEquals(obj1.pk, v.pk)
		
		obj2 = Category.objects.get(valid_time__first=p.first())
		self.assertEquals(obj2.pk, v.pk)
		
		obj3 = Category.objects.get(valid_time__last=p.last())
		self.assertEquals(obj3.pk, v.pk)
		
		# next does not exist in 9.2
		obj4 = Category.objects.get(valid_time__next=p.next())
		self.assertEquals(obj4.pk, v.pk)
	
	def test05_proxy(self):
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
	
	def test06_field_options(self):
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
	
	def test08_function_where_lookups(self):
		
		i = Category.objects.get(pk=1)
		self.assertEqual(i.valid_time, Period(start=datetime.datetime(1996,1,1), end=datetime.datetime(1996,6,1)))
		
		p = Period(start=datetime.datetime(1996,6,1), end=datetime.datetime(1996,7,1))
		result = Category.objects.filter(valid_time__adjacent=p)
		self.assertEqual(result.count(), 1)
		self.assertEqual(result[0].pk, i.pk)
	
	def test09_current_foreign_key(self):
		
		# only referenced table is temporal
		v1 = Category.objects.get(pk=1)
		v4 = Category.objects.get(pk=4)
		
		self.assertEqual(v1.valid_time.end, datetime.datetime(1996, 6, 1))
		self.assertEqual(v4.valid_time.end, datetime.datetime(9999, 12, 31, 23, 59, 59, 999999))
		
		tfk1 = ReferencedTemporalFK(name='Will fail', category=v1)
		try:
			tfk1.save()
		except IntegrityError:
			connection.connection.rollback()
		else:
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
	
	def test10_sequenced_foreign_key(self):
		pass
		self.fail('Not written yet')


