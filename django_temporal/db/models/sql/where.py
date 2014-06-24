
from django.db.models.sql.where import Constraint, WhereNode
from django_temporal.db.models.fields import PeriodField, DateRangeField

class TemporalConstraint(Constraint):
    """
    This subclass overrides `process` to better handle geographic SQL
    construction.
    """
    def __init__(self, init_constraint):
        self.alias = init_constraint.alias
        self.col = init_constraint.col
        self.field = init_constraint.field
    
    """
    def process(self, lookup_type, value, connection):
        if isinstance(value, SQLEvaluator):
            # Make sure the F Expression destination field exists, and
            # set an `srid` attribute with the same as that of the
            # destination.
            geo_fld = GeoWhereNode._check_geo_field(value.opts, value.expression.name)
            if not geo_fld:
                raise ValueError('No geographic field found in expression.')
            value.srid = geo_fld.srid
        db_type = self.field.db_type(connection=connection)
        params = self.field.get_db_prep_lookup(lookup_type, value, connection=connection)
        return (self.alias, self.col, db_type), params
    """

class TemporalWhereNode(WhereNode):
    def add(self, data, connector):
        if isinstance(data, (list, tuple)):
            obj, lookup_type, value = data
            if (isinstance(obj, Constraint) and
                isinstance(obj.field, (PeriodField, DateRangeField)) and
                lookup_type not in ('isnull',)):
                data = (TemporalConstraint(obj), lookup_type, value)
        super(TemporalWhereNode, self).add(data, connector)
    
    def make_atom(self, child, qn, connection):
        lvalue, lookup_type, value_annot, params_or_value = child
        if isinstance(lvalue, TemporalConstraint):
            data, params = lvalue.process(lookup_type, params_or_value, connection)
            temporal_sql = connection.ops.temporal_lookup_sql(data, lookup_type, params_or_value, lvalue.field, qn)
            return temporal_sql, params
        else:
            return super(TemporalWhereNode, self).make_atom(child, qn, connection)
