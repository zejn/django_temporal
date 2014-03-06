import csv
import datetime
import logging
import os
import time

from django.db import connection, transaction


# TODO
# merge in between two times
# documentation
# remove "keys=['enota']"
# add support for "copy fields", which inherit from extra fields from existing table?

def merge(new_csv, model, datum, keys=['enota'], snapshot='full', callback=None, conn=None, valid_field='valid'):
    """
    `new_csv` is a path to a CSV file, containing the records for the model.
    
    `datum is the date when the given dataset was valid
    
    `keys` is a list of model fields, which together with valid_field form 
    a sequenced unique temporal index.
    
    `snapshot` can be either "full" or "delta". If snapshot is "full", it is
    assumed the missing records are no longer valid. If snapshot is "delta",
    then the records given are updated.
    
    `callback` is a function to be called before the end of transaction 
    as callback(model, datum, keys, snapshot, conn)
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
        DATE_CURRENT = datetime.date(9999, 12, 31)
    elif valid_field_type == 'tstzrange':
        DATE_CURRENT = datetime.datetime(9999, 12, 31, 12, 00)
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
        
        sql = 'DROP TABLE IF EXISTS ' + qn(tmptable)
        cur.execute(sql)
        
        sql = 'DROP TABLE IF EXISTS ' + qn(tmptable_term)
        cur.execute(sql)
        
        logging.debug('Creating table ' + tmptable)
        sql = 'CREATE TABLE ' + qn(tmptable) + '(' + fielddef + ');'
        cur.execute(sql)
        t1 = time.time()
        logging.debug('Copying from ' + new_csv)
        #sql = '''COPY ''' + qn(tmptable) + ''' FROM %s WITH CSV HEADER NULL '';'''
        #cur.execute(sql, [new_csv])
        sql = '''COPY ''' + qn(tmptable) + ''' FROM stdin WITH CSV HEADER NULL '';'''
        cur.copy_expert(sql, open(new_csv))
        t2 = time.time()
        logging.debug('COPY took %.2f seconds' % (t2-t1,))
        sql = 'SELECT COUNT(*) FROM %s' % qn(tmptable)
        cur.execute(sql)
        count = cur.fetchall()[0][0]
        
        logging.debug('Number of records in input CSV: %d' % count)
        
        logging.debug('Locking table ' + orig_table)
        sql = 'LOCK TABLE ' + qn(orig_table) + ' IN ROW EXCLUSIVE MODE'
        cur.execute(sql)
        
        logging.debug('Deleting unchanged records...')
        t1 = time.time()
        
        
        if snapshot == 'full':
            logging.debug('Creating index on temporary table')
            sql = 'CREATE INDEX ' + qn(tmptable + '_keys_idx') + ' ON ' \
                + qn(tmptable) + '(' + ', '.join([qn(i) for i in keys]) + ');'
            cur.execute(sql)
            
            logging.debug('Terminating validity for newly missing records')
            #+ ' WHERE upper(' + qn(valid_field) + ') = %s AND ' \
            
            sql = 'SELECT DISTINCT ' \
                + ', '.join(['%s.%s AS %s' % (qn(orig_table), qn(i), qn("orig_" + i)) for i in keys]) \
                + ', ' \
                + ', '.join(['%s.%s' % (qn(tmptable), qn(i)) for i in keys]) \
                + ' INTO ' + qn(tmptable_term) \
                + ' FROM ' + qn(orig_table) + ' LEFT OUTER JOIN ' + qn(tmptable) + ' ON ' \
                + ' AND '.join(['%s.%s=%s.%s::%s' % (qn(orig_table), qn(i), qn(tmptable), qn(i), fieldtypes[i]) for i in keys]) \
                + ' WHERE upper(' + qn(orig_table) + "." + qn(valid_field) + ") = %s"
            cur.execute(sql, [DATE_CURRENT])
            
            logging.debug('Creating index.')
            sql = 'CREATE INDEX ' + qn(tmptable_term + '_idx') \
                + ' ON ' + qn(tmptable_term) \
                + '(' + ', '.join([qn("orig_" + i) for i in keys]) + ');'
            cur.execute(sql)
            
            sql = 'ANALYZE ' + qn(tmptable_term) + ';'
            cur.execute(sql)
            
            sql = 'DELETE FROM ' + qn(tmptable_term) + " WHERE " \
                + '\n AND '.join(['%s.%s IS NOT NULL' % (qn(tmptable_term), qn(i)) for i in keys])
            cur.execute(sql)
            
            sql = 'SELECT COUNT(*) FROM ' + qn(tmptable_term)
            cur.execute(sql)
            data = cur.fetchall()
            logging.debug('Deleted entries count: %d' % data[0][0])
            
            logging.debug('Updating.')
            sql = 'UPDATE ' + qn(orig_table) + ' SET ' \
                + qn(valid_field) + " = ('[' || lower(" + qn(valid_field) + ") || ',' || %s || ')')::" + fieldtypes[valid_field] \
                + ' FROM ' + qn(tmptable_term) \
                + ' WHERE upper(' + qn(valid_field) + ') = %s AND ' \
                + '\n AND '.join(['%s.%s=%s.%s::%s' % (qn(orig_table), qn(i), qn(tmptable_term), qn('orig_' + i), fieldtypes[i]) for i in keys])
            
            params = [datum, DATE_CURRENT]
            cur.execute(sql, params)
            t2 = time.time()
            logging.debug('Terminating validity took %.2f seconds' % (t2-t1))
            
            
            sql = 'DROP TABLE ' + qn(tmptable_term)
            cur.execute(sql)
        
        t1 = time.time()
        
        sql = 'SELECT ' \
            + ', '.join(['%s.%s' % (qn(orig_table), qn(i)) for i in keys]) \
            + ' INTO ' + qn(tmptable_term) \
            + ' FROM ' + qn(orig_table) \
            + ' JOIN ' + qn(tmptable) + ' ON ' \
            + ' AND '.join(['%s.%s=%s.%s::%s' % (qn(orig_table), qn(i), qn(tmptable), qn(i), fieldtypes[i]) for i in keys]) \
            + ' ' \
            + ' WHERE upper(' + qn(valid_field) + ') = %s AND ' \
            + '\n AND '.join(['(%s.%s=%s.%s::%s OR (%s.%s IS NULL AND %s.%s IS NULL))' % (qn(orig_table), qn(i), qn(tmptable), qn(i), fieldtypes[i], qn(orig_table), qn(i), qn(tmptable), qn(i)) for i in fields])
        cur.execute(sql, [DATE_CURRENT])
        
        sql = 'DELETE FROM ' + qn(tmptable) \
            + ' USING ' + qn(tmptable_term) \
            + ' WHERE ' \
            + '\n AND '.join(['%s.%s::%s=%s.%s::%s' % (qn(tmptable_term), qn(i), fieldtypes[i], qn(tmptable), qn(i), fieldtypes[i]) for i in keys])
        cur.execute(sql)
        
        t2 = time.time()
        logging.debug('Deleting took %.2f' % (t2-t1,))
        
        sql = 'SELECT COUNT(*) FROM %s' % qn(tmptable)
        cur.execute(sql)
        count = cur.fetchall()[0][0]
        logging.debug('Number of changed or new records in temp table: %d' % count)
        
        logging.debug('Adding changed items')
        sql = 'UPDATE ' + qn(orig_table) + " SET " + qn(valid_field) + " = ('[' || lower(" + qn(valid_field) + ") || ',' || %s || ')'):: " + fieldtypes[valid_field] \
            + ' FROM ' + qn(tmptable) \
            + " WHERE upper(" + qn(valid_field) + ") = %s AND " \
            + ' AND '.join(['%s.%s=%s.%s::%s' % (qn(orig_table), qn(i), qn(tmptable), qn(i), fieldtypes[i]) for i in keys])
    
        cur.execute(sql, [datum, DATE_CURRENT])
        
        sql = 'INSERT INTO ' + qn(orig_table) + '(' + ','.join([qn(i) for i in fields + [valid_field]]) + ') ' \
            + ' SELECT DISTINCT ' + ', '.join(['%s.%s::%s' % (qn(tmptable), qn(i), fieldtypes[i]) for i in fields] + \
            ["('[' || %s || ',' || %s || ')')::" + fieldtypes[valid_field]]) \
            + ' FROM ' + qn(tmptable) + ';'
        cur.execute(sql, [datum, DATE_CURRENT])
        
        logging.debug('Dropping temporary table ' + tmptable)
        cur.execute('DROP TABLE ' + qn(tmptable) + ';')
        
        sql = 'DROP TABLE ' + qn(tmptable_term) + ';'
        cur.execute(sql)

        sql = 'SAVEPOINT merge_complete;'
        cur.execute(sql)

        if callback is not None and callable(callback):
            logging.info('Calling callback.')
            callback(model=model, datum=datum, keys=keys, snapshot=snapshot, conn=conn)

        total_t2 = time.time()
        logging.info('Total time: %.2f seconds.' % (total_t2-total_t1))
