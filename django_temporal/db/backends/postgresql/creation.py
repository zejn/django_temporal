# vim: set ts=4

from django.db.backends.postgresql_psycopg2.creation import DatabaseCreation

class PostgresTemporalCreation(DatabaseCreation):
    def sql_indexes_for_field(self, model, f, style):
        from django.db.backends.util import truncate_name
        from django_temporal.db.models.fields import PeriodField, ValidTime, ForeignKey
        output = super(PostgresTemporalCreation, self).sql_indexes_for_field(model, f, style)
        qn = self.connection.ops.quote_name
        
        if isinstance(f, PeriodField):
            db_table = model._meta.db_table
            if f.not_empty:
                i_name = '%s_%s' % (f.name, 'nonempty')
                output.append(style.SQL_KEYWORD('ALTER TABLE') + ' ' +
                    style.SQL_TABLE(qn(db_table)) + ' ' +
                    style.SQL_KEYWORD('ADD CONSTRAINT') + ' ' +
                    style.SQL_TABLE(qn(i_name)) + ' ' +
                    style.SQL_KEYWORD('CHECK') + ' (' +
                    style.SQL_KEYWORD('isempty') + '(' +
                    style.SQL_FIELD('%s' % qn(f.name)) + ') = ' +
                    style.SQL_KEYWORD('false') + ');'
                )
                
            if f.current_unique is not None:
                columns = []
                for curuniq in f.current_unique:
                    for fld in model._meta.fields:
                        if fld.name == curuniq:
                            columns.append(style.SQL_FIELD(qn(fld.column)))
                
                i_name = '%s_%s' % (db_table, 'curuniq')
                output.append(style.SQL_KEYWORD('CREATE UNIQUE INDEX') + ' ' +
                    style.SQL_TABLE(qn(truncate_name(i_name, self.connection.ops.max_name_length()))) + ' ' +
                    style.SQL_KEYWORD('ON') + ' ' +
                    style.SQL_TABLE(qn(db_table)) + ' ' +
                    '(' + ', '.join(columns) + ', ' +
                    style.SQL_FIELD('upper(%s)' % qn(f.name)) +
                    ');'
                )
            if f.sequenced_unique is not None:
                sequenced_unique_parts = []
                i_name = '%s_%s' % (db_table, 'sequniq')
                sequenced_unique_parts.append(style.SQL_KEYWORD('ALTER TABLE') + ' ' +
                    style.SQL_TABLE(qn(db_table)) + ' ' +
                    style.SQL_KEYWORD('ADD CONSTRAINT') + ' ' +
                    style.SQL_TABLE(qn(i_name)) + ' ' +
                    style.SQL_KEYWORD('EXCLUDE USING GIST') + ' ('
                    )
                
                for sqk in f.sequenced_unique:
                    # need to find column
                    if hasattr(model._meta, 'fields'):
                        for fld in model._meta.fields:
                            if fld.name == sqk:
                                column = fld.column
                    else:
                        # XXX FIXME: south support lacks
                        # so if db_column differs from field_name (eg. foreign key)
                        # when doing a add_column, this will break.
                        column = sqk
                    sequenced_unique_parts.append(
                        style.SQL_FIELD(qn(column)) + ' ' +
                        style.SQL_KEYWORD('WITH') + ' ' +
                        style.SQL_COLTYPE('=') + ','
                        )
                
                sequenced_unique_parts.append(
                    style.SQL_FIELD(qn(f.name)) + ' ' +
                    style.SQL_KEYWORD('WITH') + ' ' +
                    style.SQL_COLTYPE('&&') +
                    ');'
                    )
                output.append(''.join(sequenced_unique_parts))
            if f.nonsequenced_unique is not None:
                columns = []
                for curuniq in f.nonsequenced_unique:
                    for fld in model._meta.fields:
                        if fld.name == curuniq:
                            columns.append(style.SQL_FIELD(qn(fld.column)))
                
                i_name = '%s_%s' % (db_table, 'nsequniq')
                output.append(style.SQL_KEYWORD('CREATE UNIQUE INDEX') + ' ' +
                    style.SQL_TABLE(qn(truncate_name(i_name, self.connection.ops.max_name_length()))) + ' ' +
                    style.SQL_KEYWORD('ON') + ' ' +
                    style.SQL_TABLE(qn(db_table)) + ' ' +
                    '(' + ', '.join(columns) +
                    ', ' + style.SQL_FIELD('lower(%s)' % qn(f.name)) +
                    ', ' + style.SQL_FIELD('upper(%s)' % qn(f.name)) +
                    ');'
                )
        
        
        if isinstance(f, ForeignKey):
            this_temporal = related_temporal = False
            this_validity_field = None
            for tmf in f.model._meta.fields:
                if isinstance(tmf, ValidTime):
                    this_temporal = True
                    this_validity_field = tmf
            
            related_field = f.rel.get_related_field()
            related_model = related_field.model
            related_validity_field = None
            for rmf in related_model._meta.fields:
                if isinstance(rmf, ValidTime):
                    related_temporal = True
                    related_validity_field = rmf
            
            
            if this_temporal:
                if related_temporal:
                    func_template = '''\
CREATE FUNCTION %(trigger_name)s() RETURNS TRIGGER AS '
BEGIN
IF EXISTS (
    SELECT * FROM %(referencing_table)s AS A
        WHERE upper(A.%(referencing_validity_field)s) = TIMESTAMP WITH TIME ZONE ''9999-12-30 00:00:00.000000+0000''
        AND NOT EXISTS (
            SELECT * FROM %(referenced_table)s AS B
            WHERE A.%(referencing_field)s = B.%(referenced_field)s
            AND upper(B.%(referenced_validity_field)s) = TIMESTAMP WITH TIME ZONE ''9999-12-30 00:00:00.000000+0000''
    )
) THEN
    RAISE ''Temporal current foreign key constraint violation on %(referencing_table)s.%(referencing_field)s'' USING ERRCODE = ''23503'';
END IF;
RETURN NULL;
END
'
LANGUAGE 'plpgsql';'''
                    trigger_template = '''\
CREATE TRIGGER %(name)s
AFTER INSERT OR UPDATE OR DELETE ON %(referencing_table)s
FOR EACH ROW
EXECUTE PROCEDURE %(trigger_name)s();'''
                    
                    name = '%s_%s_%s' % (f.model._meta.db_table, f.name, 'cur_tfk')
                    trigger_name = '%s_%s' % (name, 'tr')
                    info = {
                        'name': qn(name),
                        'trigger_name': qn(trigger_name),
                        'referencing_table': qn(f.model._meta.db_table),
                        'referencing_field': qn(f.column),
                        'referencing_validity_field': qn(this_validity_field.column),
                        'referenced_table': qn(related_model._meta.db_table),
                        'referenced_field': qn(related_field.column),
                        'referenced_validity_field': qn(related_validity_field.column),
                        }
                    output.append(func_template % info)
                    output.append(trigger_template % info)
                else:
                    # no change needed
                    pass
            else:
                if related_temporal:
                    func_template = '''\
CREATE FUNCTION %(trigger_name)s() RETURNS TRIGGER AS '
BEGIN
IF EXISTS (
    SELECT * FROM %(referencing_table)s AS A
        WHERE NOT EXISTS (
            SELECT * FROM %(referenced_table)s AS B
            WHERE A.%(referencing_field)s = B.%(referenced_field)s
            AND upper(B.%(referenced_validity_field)s) = TIMESTAMP WITH TIME ZONE ''9999-12-30 00:00:00.000000+0000''
    )
) THEN
    RAISE ''Temporal current foreign key constraint violation on %(referencing_table)s.%(referencing_field)s'' USING ERRCODE = ''23503'';
END IF;
RETURN NULL;
END
'
LANGUAGE 'plpgsql';'''
                    trigger_template = '''\
CREATE TRIGGER %(name)s
AFTER INSERT OR UPDATE OR DELETE ON %(referencing_table)s
FOR EACH ROW
EXECUTE PROCEDURE %(trigger_name)s();'''
                    name = '%s_%s_%s' % (f.model._meta.db_table, f.name, 'cur_tfk')
                    trigger_name = '%s_%s' % (name, 'tr')
                    info = {
                        'name': qn(name),
                        'trigger_name': qn(trigger_name),
                        'referencing_table': qn(f.model._meta.db_table),
                        'referencing_field': qn(f.column),
                        'referenced_table': qn(related_model._meta.db_table),
                        'referenced_field': qn(related_field.column),
                        'referenced_validity_field': qn(related_validity_field.column),
                        }
                    output.append(func_template % info)
                    output.append(trigger_template % info)
                else:
                    # no changes required
                    pass
            
        return output
