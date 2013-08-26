
Django Temporal
===============

Also known as "beat the time frenzy".

Introduction
------------

This is a Django module that tries to make temporal databases somewhat easier
in [Django](https://www.djangoproject.com/). It is in early stages of development and still needs a lot of work.

Tutorial
--------

Django Temporal is an easy to use implementation of temporal database semantics,
integrated into Django ORM. The module is a database backend similar to GeoDjango
and provides a temporal extension of Django's backend. Due to built in range
types, currently only PostgreSQL is supported, but that may change.

### Representation of current time

There's a specific problem when handling temporal data: how should the currently
valid record's time be represented? As Richard Snodgrass has devised, using a 
far distant time is by far the most elegant solution. This module uses a notion 
of *current time*, meaning that if the interval end time equals a precise time
stamp in the future, then it represents a currently active record.

### Model definition

Let's see how it works by example. Imagine there's a model of categories you 
want to have temporal. Here's how you might define it:

    from django_temporal.db import models

    class Category(models.Model):
        cat = models.DecimalField(max_digits=10, decimal_places=0)
        valid_time = models.ValidTime(sequenced_unique=('cat',), current_unique=('cat',))

1. Note that we imported module models from `django_temporal.db` instead of
   `django.db`.
1. See the new ValidTime field. This is an  subclass of
   [PeriodField](#PeriodField), which represents a time range between two 
   points in time.
1. Note the *sequenced_unique* and *current_unique* keyword arguments to
   ValidTime field which define the temporal constraints.


### PeriodField

PeriodField is a type of Django field. It is a general representation of the 
range database field in Django and takes care of the conversion between Python 
and underlying database.

The PostgreSQL backend uses [`tstzrange`](http://www.postgresql.org/docs/9.2/static/rangetypes.html).


### ValidField

A ValidField is a subclass of PeriodField. It's use makes a model temporal and
automatically enables temporal features on the model such as temporal queries.

### Period

A time range in Python is represented with custom Period class and is thus the
value a ValidField returns and can be set to. It has several useful attributes:

|Attribute|Function|
|---|---|
|start|Represents the start time of the period. Start time is generally included in the period.|
|start_included|Returns True if the start time is included in period.|
|end|Represents the end time of the period. End time is usually excluded from the period.|
|end_included|Returns True if end time is included in period.|
|is_current|Returns True if this period represents current time.|
|set_current|Sets end time to current time.|
|first|Same as start.|
|prior|Returns the time immediately before start.|
|last|Same as end.|
|later|Returns the time immediately after end.|

### Temporal constraints

Temporal constraints are a very useful feature. This module features a number of
temporal constraints, which can be set on a ValidField.

|Uniqueness|Keyword|Description|
|---|---|---|
|Value unique|-|This is the normally used unique, where there must be no equivalent set of values.|
|Nonsequenced unique|nonsequenced_unique|This is normally used unique, but includes the valid range. It only requires that there be no record with same fields and same valid range. There can be a record with same fields but different time range, which can even overlap.|
|Current unique|current_unique|This constraint requires there be only one record with the same fields and current time. This type of uniqueness is most useful for representing currently active records.|
|Sequenced unique|sequenced_unique|This constraint requires there be at most one record with the same fields at every point of time. This is very useful for sheduling resources.|

### Sequenced primary key

A table with a sequenced primary key is a table with multi column primary key, consisting of a serial and a temporal range. This is very useful for keeping a
history of an object whose id keeps the same over time, but other attributes
change.

> Multi column primary key isn't supported in Django as of version 1.5.
> There are branches, which would make this possible, but I haven't inspected
> them yet.

### Temporal foreign keys

Having two tables, that can be temporal, there are four cases. The case where
temporal foreign keys play role are when referenced tables are temporal. To 
solve this, Django_temporal provides model field named TemporalForeignKey.

There are two useful foreign keys, current and sequenced. Current foreign key
relation is enabled by writing

    class ThisModel(models.Model)
        temporal_fk = models.TemporalForeignKey(OtherModel, temporal_current=True)

and similarly, sequenced foreign key is enabled by stating
`temporal_sequenced=True`.






[1] Developing Time-Oriented Database Applications in SQL, Richard T. Snodgrass, Morgan Kaufmann Publishers, Inc., San Francisco, July, 1999, 504+xxiii pages, ISBN 1-55860-436-7.
