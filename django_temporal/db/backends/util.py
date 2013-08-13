

class TemporalOperation(object):
    
    sql_template = '%(temporal_col)s %(operator)s %(period)s'
    
    def __init__(self, function='', operator='', result='', **kwargs):
        self.function = function
        self.operator = operator
        self.result = result
        self.extra = kwargs
    
    def as_sql(self, temporal_col, period='%s'):
        return self.sql_template % self.params(temporal_col, period)
    
    def params(self, temporal_col, period='%s'):
        params = {
            'function': self.function,
            'temporal_col': temporal_col,
            'period': period,
            'operator': self.operator,
            'result': self.result,
            }
        params.update(self.extra)
        return params

class TemporalFunction(TemporalOperation):
    sql_template = '%(function)s(%(temporal_col)s, %(period)s)'
    
    def __init__(self, func, result='', operator='', **kwargs):
        # Getting the function prefix.
        default = {'function' : func,
                    'operator' : operator,
                    'result' : result
                    }
        kwargs.update(default)
        super(TemporalFunction, self).__init__(**kwargs)

# rework as temporal attribute?
class TemporalFunctionTS(TemporalFunction):
    sql_template = '%(function)s(%(temporal_col)s) = %(period)s'
