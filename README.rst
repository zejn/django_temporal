
Django Temporal
===============

Introduction
------------

This is a Django module that tries to make temporal databases somewhat easier
in Django. It is in early stages of development and still needs a lot of work.

Requirements
------------

Required software::

- PostgreSQL 9.2
- Django 1.5


Features
--------

Django Temporal is an easy to use implementation of temporal database semantics,
integrated into Django ORM.

All implementations use closed-open representation of period::

- Sequenced primary key
- Current uniqueness (no two records valid at current time)
- Sequenced uniqueness (no two records overlapping in time)
- Nonsequenced uniqueness (no two records in table are the same)


Referential integrity (when both tables are temporal)::

- Current foreign key
- Sequenced foreign key
- Contigious foreign key


Referential integrity (referenced table is temporal)::

- Current foreign key





