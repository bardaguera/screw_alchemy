#!/usr/bin/env python
# coding: utf-8

##
## Version 3 -- 2020-01-20
##

import json
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData, Table
from sqlalchemy.exc import SQLAlchemyError
import datetime
import decimal

import types
        

def alchemyencoder(obj):
    if isinstance(obj, datetime.date):
        return obj.isoformat()
    elif isinstance(obj, decimal.Decimal):
        return float(obj)
    
def jsonify_query_result(conn, query):
    res = query.all()
    #res = conn.execute(query)
    #return [dict(r) for r in res]
    return [r._asdict() for r in res]

def transfer_columns(srs_meta, dest_table_obj):
    for column in srs_meta.columns:
        if column.autoincrement:
            column.autoincrement = False
            print(column.server_default)
            column.server_default = None
        dest_table_obj.append_column(column.copy())
    return dest_table_obj


def get_columns(self):
    return self.__table__


class BaseInstance(object):
    def __init__(self, instance = None, js = None):
        self.instance = str(instance)
        self._js_ = json.loads(json.dumps(js))
        self.engine = None
        self.session = None
        self._cur_schema_ = None #alias
        #self._cur_table_ = None #alias
        self.status = {}
    
    def _gen_engine_(self, conn_str, debug=False):
        self.engine = create_engine(conn_str,
                                    echo='debug' if debug else False) #engines[instance]["debug"]
        Session = sessionmaker(bind=self.engine, autocommit=True)
        self.session = Session()
        
    
    def _gen_schema_(self, schema_name, schema_objects):
        setattr(self,
                schema_name + '_meta',
                MetaData(bind=self.engine, schema=schema_name))
        self._cur_schema_ = eval('self.'+schema_name + '_meta')
        try:
            if type(schema_objects) != bool:
                self._cur_schema_.reflect(only=schema_objects)
            else:
                self._cur_schema_.reflect()
        except SQLAlchemyError as e:
            self.status[schema_name] = e._code_str
    
    def _gen_table_(self, table, m_args = None):
        Base = declarative_base()
        #Base.b_method = types.MethodType(standalone_function, Base, AClass)
        __table__ = Table(table, self._cur_schema_, autoload=True, autoload_with=self.engine)
        table_dict = {'__tablename__':table, '__table__': __table__}
        if m_args:
            table_dict['__mapper_args__'] = eval(m_args)
            
        #TODO: обернуть в try-except
        setattr(self, table, type(str(table),
                                  (Base,),
                                  table_dict
                                 ))
        
                
        setattr(eval('self.'+table), 'get_columns', get_columns(self))
        #gettting columns
        #cur_table_name = self._cur_schema_.schema+'.'+table
        #col_list = list(col for col in self._cur_schema_.tables[cur_table_name].columns)
        
        
    def _check_connection_(self):
        try:
            self.engine.connect()
        except:
            self.status['{}_engine'.format(self.instance)] = "No connection"
    
    def gen_from_js(self):
        for schema_name in self._js_['tables'].keys():
            schema_objects = self._js_['tables'][schema_name]
            
            self._gen_engine_(self._js_['conn_string'], self._js_['debug'])
            self._check_connection_()
            self._gen_schema_(schema_name, schema_objects)
            
            if type(schema_objects) != bool:
                for table, m_args in schema_objects.items():
                    self._gen_table_(table, m_args)
                    
    def log(self, log_path, json_log):
        with open(log_path, 'a') as f:
            json.dump(json_log, f)
    
    def dispose(self):
        self.engine.dispose()