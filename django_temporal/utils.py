import csv
import datetime
import logging
import os
import time

from django.db import connection, transaction
from psycopg2.extensions import adapt
from django_temporal.db.models.fields import DATE_CURRENT, TIME_CURRENT
# TODO
# merge in between two times
# documentation

def merge(new_csv, model, timestamp, keys, snapshot='full', copy_fields=None, callback=None, conn=None, valid_field='valid', debug=False):
    """
    `new_csv` is a path to a CSV file, containing the records for the model.
    
    `timestamp is the date when the given dataset was valid
    
    `keys` is a list of model fields, which together with valid_field form 
    a sequenced unique temporal index.
    
    `snapshot` can be either "full" or "delta". If snapshot is "full", it is
    assumed the missing records are no longer valid. If snapshot is "delta",
    then the records given are updated.
    
    `copy_fields` is a list of fields to be copied from existing records in
    the table when updating the records.
    
    `callback` is a function to be called before the end of transaction 
    as callback(model, timestamp, keys, snapshot, conn)
    """
    
    import time
    rdr = csv.reader(open(new_csv))
    fields = rdr.next()
    assert snapshot in ('full', 'delta')
    
    new_csv = os.path.abspath(new_csv)
    
    if conn is None:
        # FIXME
        conn = connection
        
    fieldtypes = dict([(f.attname, f.db_type(conn)) for f in model._meta.fields])
    valid_field_type = fieldtypes[valid_field]
    if valid_field_type == 'daterange':
        CURRENT_VALUE = DATE_CURRENT
    elif valid_field_type == 'tstzrange':
        CURRENT_VALUE = TIME_CURRENT
    else:
        raise ValueError("Unknown type of valid field")
    
    with transaction.commit_on_success():
        cur = conn.cursor()
        orig_table = model._meta.db_table
        tmptable = orig_table + '_temp'
        tmptable_term = orig_table + '_term_temp'
        qn = conn.ops.quote_name
        #fielddef = ', '.join(['%s varchar(500)' % qn(i) for i in fields])
        fielddef = ', '.join(['%s %s NULL' % (qn(i), fieldtypes[i]) for i in fields])
        
        total_t1 = time.time()
        
        if debug:
            print 'STARTING STATE'
            print '~'*80
            import sys
            sql = '''COPY ''' + qn(orig_table) + ''' TO stdout WITH CSV HEADER NULL '';'''
            cur.copy_expert(sql, sys.stdout)
            print '~'*80
            
        sql = 'DROP TABLE IF EXISTS ' + qn(tmptable) + ';'
        if debug:
            print sql
        cur.execute(sql)
        
        sql = 'DROP TABLE IF EXISTS ' + qn(tmptable_term) + ';'
        if debug:
            print sql
        cur.execute(sql)
        
        # First we load the new dump into db as a table
        # This table is `tmptable`
        logging.debug('Creating table ' + tmptable)
        sql = 'CREATE TABLE ' + qn(tmptable) + '(' + fielddef + ');'
        if debug:
            print sql
        cur.execute(sql)
        t1 = time.time()
        logging.debug('Copying from ' + new_csv)
        
        sql = '''COPY ''' + qn(tmptable) + ''' FROM %s WITH CSV HEADER NULL '';'''
        if debug:
            print sql % adapt(new_csv).getquoted()
        sql = sql % 'stdin'
        cur.copy_expert(sql, open(new_csv))
        t2 = time.time()
        logging.debug('COPY took %.2f seconds' % (t2-t1,))
        sql = 'SELECT COUNT(*) FROM %s' % qn(tmptable) + ';'
        if debug:
            print sql
        cur.execute(sql)
        count = cur.fetchall()[0][0]
        
        logging.debug('Number of records in input CSV: %d' % count)
        
        logging.debug('Locking table ' + orig_table)
        sql = 'LOCK TABLE ' + qn(orig_table) + ' IN ROW EXCLUSIVE MODE;'
        if debug:
            print sql
        cur.execute(sql)
        
        logging.debug('Deleting unchanged records...')
        t1 = time.time()
        
        if snapshot == 'full':
            logging.debug('Creating index on temporary table')
            sql = 'CREATE INDEX ' + qn(tmptable + '_keys_idx') + ' ON ' \
                + qn(tmptable) + '(' + ', '.join([qn(i) for i in keys]) + ');'
            if debug:
                print sql
            cur.execute(sql)
            
            logging.debug('Terminating validity for newly missing records')
            # To find out which records have no counterpart in existing table,
            # we first make a table containing (temporal) keys from both tables
            # side by side.
            sql = 'SELECT DISTINCT ' \
                + ', '.join(['%s.%s AS %s' % (qn(orig_table), qn(i), qn("orig_" + i)) for i in keys]) \
                + ', ' \
                + ', '.join(['%s.%s' % (qn(tmptable), qn(i)) for i in keys]) \
                + ' INTO ' + qn(tmptable_term) \
                + ' FROM ' + qn(orig_table) + ' LEFT OUTER JOIN ' + qn(tmptable) + ' ON ' \
                + ' AND '.join(['(%s.%s=%s.%s::%s OR (%s.%s IS NULL AND %s.%s IS NULL))' % (qn(orig_table), qn(i), qn(tmptable), qn(i), fieldtypes[i], qn(orig_table), qn(i), qn(tmptable), qn(i)) for i in keys]) \
                + ' AND (' + ' OR '.join(['%s.%s IS NOT NULL' % (qn(orig_table), qn(i)) for i in keys]) + ')' \
                + ' WHERE upper(' + qn(orig_table) + "." + qn(valid_field) + ") = %s ;"
            params = [CURRENT_VALUE]
            if debug:
                print sql % tuple([adapt(i).getquoted() for i in params])
            cur.execute(sql, params)
            
            logging.debug('Creating index.')
            sql = 'CREATE INDEX ' + qn(tmptable_term + '_idx') \
                + ' ON ' + qn(tmptable_term) \
                + '(' + ', '.join([qn("orig_" + i) for i in keys]) + ');'
            if debug:
                print sql
            cur.execute(sql)
            
            sql = 'ANALYZE ' + qn(tmptable_term) + ';'
            if debug:
                print sql
            cur.execute(sql)
            
            # Delete records which counterpart in new dump, to get those, which
            # have gone missing and are to have their validity terminated.
            sql = 'DELETE FROM ' + qn(tmptable_term) + " WHERE " \
                + '\n OR '.join(['%s.%s IS NOT NULL' % (qn(tmptable_term), qn(i)) for i in keys]) \
                + ';'
            if debug:
                print sql
            cur.execute(sql)
            
            sql = 'SELECT COUNT(*) FROM ' + qn(tmptable_term) + ';'
            if debug:
                print sql
            cur.execute(sql)
            data = cur.fetchall()
            logging.debug('Deleted entries count: %d' % data[0][0])
            
            logging.debug('Updating.')
            # Terminate validity to records, which have gone missing.
            sql = 'UPDATE ' + qn(orig_table) + ' SET ' \
                + qn(valid_field) + " = ('[' || lower(" + qn(valid_field) + ") || ',' || %s || ')')::" + fieldtypes[valid_field] \
                + ' FROM ' + qn(tmptable_term) \
                + ' WHERE upper(' + qn(valid_field) + ') = %s AND ' \
                + '\n AND '.join(['(%s.%s=%s.%s::%s OR (%s.%s IS NULL AND %s.%s IS NULL))' % (qn(orig_table), qn(i), qn(tmptable_term), qn('orig_' + i), fieldtypes[i], qn(orig_table), qn(i), qn(tmptable_term), qn('orig_' + i)) for i in keys]) \
                + ';'
            
            params = [timestamp, CURRENT_VALUE]
            if debug:
                print sql % tuple([adapt(i).getquoted() for i in params])
            cur.execute(sql, params)
            t2 = time.time()
            logging.debug('Terminating validity took %.2f seconds' % (t2-t1))
            
            sql = 'DROP TABLE ' + qn(tmptable_term) + ';'
            if debug:
                print sql
            cur.execute(sql)
        
        t1 = time.time()
        
        # Select keys from current temporal table, that have exact counterparts
        # (including non-key fields) in new dump. We use this to see which
        # records have not changed.
        sql = 'SELECT ' \
            + ', '.join(['%s.%s' % (qn(orig_table), qn(i)) for i in keys]) \
            + ' INTO ' + qn(tmptable_term) \
            + ' FROM ' + qn(orig_table) \
            + ' JOIN ' + qn(tmptable) + ' ON ' \
            + ' AND '.join(['(%s.%s=%s.%s::%s OR (%s.%s IS NULL AND %s.%s IS NULL))' % (qn(orig_table), qn(i), qn(tmptable), qn(i), fieldtypes[i], qn(orig_table), qn(i), qn(tmptable), qn(i)) for i in fields]) \
            + ' ' \
            + ' WHERE upper(' + qn(valid_field) + ') = %s AND ' \
            + '\n AND '.join(['(%s.%s=%s.%s::%s OR (%s.%s IS NULL AND %s.%s IS NULL))' % (qn(orig_table), qn(i), qn(tmptable), qn(i), fieldtypes[i], qn(orig_table), qn(i), qn(tmptable), qn(i)) for i in fields]) \
            + ';'
        params = [CURRENT_VALUE]
        if debug:
            print sql % tuple([adapt(i).getquoted() for i in params])
        cur.execute(sql, params)
        
        # Delete rows from new dump, which have not changed compared to temporal
        # table.
        sql = 'DELETE FROM ' + qn(tmptable) \
            + ' USING ' + qn(tmptable_term) \
            + ' WHERE ' \
            + '\n AND '.join(
                ['(%s.%s::%s=%s.%s::%s OR (%s.%s IS NULL AND %s.%s IS NULL))' % (
                    qn(tmptable_term), qn(i), fieldtypes[i], qn(tmptable), qn(i), fieldtypes[i], qn(tmptable_term), qn(i), qn(tmptable), qn(i)) for i in keys]
                ) \
            + ';'
        if debug:
            print sql
        cur.execute(sql)
        
        t2 = time.time()
        logging.debug('Deleting took %.2f' % (t2-t1,))
        
        sql = 'SELECT COUNT(*) FROM %s' % qn(tmptable) + ';'
        if debug:
            print sql
        cur.execute(sql)
        count = cur.fetchall()[0][0]
        logging.debug('Number of changed or new records in temp table: %d' % count)
        
        logging.debug('Adding changed items')
        # First terminate validity to records in temporal table. New records
        # will have same key, starting with current time.
        sql = 'UPDATE ' + qn(orig_table) + " SET " + qn(valid_field) + " = ('[' || lower(" + qn(valid_field) + ") || ',' || %s || ')'):: " + fieldtypes[valid_field] \
            + ' FROM ' + qn(tmptable) \
            + " WHERE upper(" + qn(valid_field) + ") = %s AND " \
            + ' AND '.join(
                ['(%s.%s::%s=%s.%s::%s OR (%s.%s IS NULL AND %s.%s IS NULL))' % (
                    qn(orig_table), qn(i), fieldtypes[i], qn(tmptable), qn(i), fieldtypes[i], qn(orig_table), qn(i), qn(tmptable), qn(i)) for i in keys]
                ) \
            + ';'
        params = [timestamp, CURRENT_VALUE]
        if debug:
            print sql % tuple([adapt(i).getquoted() for i in params])
        cur.execute(sql, params)
        
        print '~'*30
        # Insert new records into temporal table, with current time as start of
        # validity. This covers both updated and new records.
        if copy_fields is None:
            copy_fields = []
            copy_field_spec = []
            copy_fields_from = ''
        else:
            copy_field_spec = ['%s.%s::%s' % (qn(orig_table), qn(i), fieldtypes[i]) for i in copy_fields]
            copy_fields_from = ' LEFT OUTER JOIN ' + qn(orig_table) + ' ON ' \
                + ' AND '.join(
                ['(%s.%s::%s=%s.%s::%s OR (%s.%s IS NULL AND %s.%s IS NULL))' % (
                    qn(orig_table), qn(i), fieldtypes[i], qn(tmptable), qn(i), fieldtypes[i], qn(orig_table), qn(i), qn(tmptable), qn(i)) for i in keys]
                ) \
                + ' AND upper(' + qn(orig_table) + '.' + qn(valid_field) + ') = %s'
            
        sql = 'INSERT INTO ' + qn(orig_table) + '(' + ','.join([qn(i) for i in fields + copy_fields + [valid_field]]) + ') ' \
            + ' SELECT DISTINCT ' + \
                ', '.join(['%s.%s::%s' % (qn(tmptable), qn(i), fieldtypes[i]) for i in fields] + \
                copy_field_spec + \
            ["('[' || %s || ',' || %s || ')')::" + fieldtypes[valid_field]]) \
                + ' FROM ' + qn(tmptable) + copy_fields_from + ';'
        if copy_fields:
            params = [timestamp, CURRENT_VALUE, timestamp]
        else:
            params = [timestamp, CURRENT_VALUE]
        if debug:
            print sql % tuple([adapt(i).getquoted() for i in params])
        cur.execute(sql, params)
        
        logging.debug('Dropping temporary table ' + tmptable)
        cur.execute('DROP TABLE ' + qn(tmptable) + ';')
        
        sql = 'DROP TABLE ' + qn(tmptable_term) + ';'
        if debug:
            print sql
        cur.execute(sql)

        sql = 'SAVEPOINT merge_complete;'
        if debug:
            print sql
        cur.execute(sql)

        if callback is not None and callable(callback):
            logging.info('Calling callback.')
            callback(model=model, timestamp=timestamp, keys=keys, snapshot=snapshot, conn=conn)

        total_t2 = time.time()
        logging.info('Total time: %.2f seconds.' % (total_t2-total_t1))
