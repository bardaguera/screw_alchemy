#!/usr/bin/env python
# coding: utf-8

##
## Version 6 -- 2020-02-07
#    Added method add_column to base class and to table attr
## Version 5 -- 2020-02-01
#    Added _UpdInstanceAddColumn method
## Version 4 -- 2020-01-27
#    Added mapper args parser
## Version 3 -- 2020-01-20
##

import json
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData, Table, Column
from sqlalchemy.exc import SQLAlchemyError, ArgumentError
import datetime
import decimal

from types import MethodType
       

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
    col_list = []
    for col in self.__table__.columns:
        col_list.append({'name':col.name, 'type':col.type, 'nullable': col.nullable})
    return col_list

def _MArgsSerialize(m_args):
    if '__table__.c' in m_args:
        return m_args
    else:
        key_columns = str(list(map(lambda x:'__table__.c.'+x, m_args['primary_key']))).replace("'",'')
        return "{\'primary_key\': "+ key_columns+"}"
    
def _UpdInstanceTypeMapping(engine, s):
    #Dict values could be: 
    ## SQLAlchemy types
    ##  dicts with 'engine_name' and ! 'default' -- if engine hasn't found
    mapping_dict = {'bool': sqlalchemy.types.Boolean,
                    'yes_or_no': sqlalchemy.types.Boolean,
                    'bigint': sqlalchemy.types.BigInteger,
                    'int8': sqlalchemy.sql.sqltypes.INTEGER,
                    'int4': sqlalchemy.sql.sqltypes.INTEGER,
                    'int2': sqlalchemy.sql.sqltypes.INTEGER,
                    'numeric': sqlalchemy.types.Numeric,
                    'bit': sqlalchemy.types.CHAR, #---?
                    'uuid': {'postgresql':sqlalchemy.dialects.postgresql.UUID,
                            'default':sqlalchemy.types.TEXT},
                    'date': sqlalchemy.types.Date,
                    'time': sqlalchemy.types.Time,
                    'interval': sqlalchemy.types.Interval,
                    'timestamp': sqlalchemy.types.TIMESTAMP,
                    'timestamptz': sqlalchemy.types.TIMESTAMP(timezone=True),
                    'abstime': {'default':sqlalchemy.types.TIMESTAMP},
                    'varchar': sqlalchemy.types.TEXT,
                    'char': sqlalchemy.types.TEXT,
                    'text': sqlalchemy.types.TEXT,
                    'inet':  sqlalchemy.types.TEXT,
                    'float4': sqlalchemy.types.Float(precision=4),
                    'float8': sqlalchemy.types.Float(precision=8),
                    'int2vector': sqlalchemy.types.ARRAY(sqlalchemy.sql.sqltypes.INTEGER),
                    'bytea': sqlalchemy.types.LargeBinary}

    if type(mapping_dict.get(s, False)) == dict:
        engine_specific_type = mapping_dict[s].get(engine.name, False)
        if not engine_specific_type:
            return mapping_dict[s]['default']
        else:
            return engine_specific_type
    else:
        return mapping_dict.get(s, False)
    
def _UpdInstanceAddColumn(self, col_dict, table_name = None):
    #Two use-cases: self is base_object or self is table_obj
    if 'BaseInstance' in str(self.__class__):
        loc_engine_obj = self.engine
        if not table_name:
            loc_table_name = self._cur_table_.__tablename__
        else:
            loc_table_name = table_name
    else:
        loc_table_name = self.__tablename__
        loc_engine_obj = self._engine_link_

    if not _UpdInstanceTypeMapping(loc_engine_obj, col_dict['col_type']):
        print('TYPE DOES NOT FOUND %s' % col_dict['col_type'])
        return None
    column_obj = Column(col_dict['col_name'],
                        _UpdInstanceTypeMapping(loc_engine_obj, col_dict['col_type']),
                        nullable=True)
    loc_engine_obj.execute('ALTER TABLE {} ADD COLUMN {} {};'.format(loc_table_name,
                                                                     column_obj.compile(dialect=loc_engine_obj.dialect),
                                                                     column_obj.type.compile(loc_engine_obj.dialect)))

class BaseInstance(object):
    def __init__(self, instance = None, js = None):
        self.instance = str(instance)
        self._js_ = json.loads(json.dumps(js))
        self.engine = None
        self.session = None
        self._cur_schema_ = None #alias
        self._cur_table_ = None #alias
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
    
    def _gen_table_(self, table_name, schema_obj = None, m_args = None):
        if not schema_obj:
            schema_obj = self._cur_schema_
        Base = declarative_base()
        __table__ = Table(table_name, schema_obj, autoload=True, autoload_with=self.engine)
        table_dict = {'__tablename__':table_name, '__table__': __table__}
        if m_args:
            table_dict['__mapper_args__'] = eval(_MArgsSerialize(m_args))
        try:
            setattr(self, table_name, type(str(table_name),
                                      (Base,),
                                      table_dict
                                      ))
        except ArgumentError as e:
            table_data = schema_obj.tables[schema_obj.schema+'.'+table_name]
            all_cols = list(c.name for c in t.columns)
            self._gen_table_(table_name = table_name, schema_obj =schema_obj, m_args = {'primary_key':all_cols})
            
        else:
            self._cur_table_ = eval('self.'+table_name)
            self._cur_table_._engine_link_ = self.engine
            self._cur_table_.get_columns = MethodType(get_columns, self._cur_table_)
            self._cur_table_.add_column = MethodType(_UpdInstanceAddColumn, self._cur_table_)
        
        #setattr(eval('self.'+table), 'get_columns', get_columns)
        #gettting columns
        #cur_table_name = self._cur_schema_.schema+'.'+table
        #col_list = list(col for col in self._cur_schema_.tables[cur_table_name].columns)
        
        
    def _check_connection_(self):
        try:
            self.engine.connect()
        except:
            self.status['{}_engine'.format(self.instance)] = "No connection"
    
    def gen_instance(self):
        for schema_name in self._js_['tables'].keys():
            schema_objects = self._js_['tables'][schema_name]
            
            self._gen_engine_(self._js_['conn_string'], self._js_['debug'])
            self._check_connection_()
            self._gen_schema_(schema_name, schema_objects)
            
            if type(schema_objects) != bool:
                for table, m_args in schema_objects.items():
                    self._gen_table_(table_name=table, m_args=m_args)
                    
    def log(self, log_path, json_log):
        with open(log_path, 'a') as f:
            json.dump(json_log, f)
    
    def dispose(self):
        self.engine.dispose()
        
    def add_column(self, col_dict, table_name):
        _UpdInstanceAddColumn(self, col_dict, table_name = table_name)


    def _UpdInstanceAddTable(self, table_name, columns_list, schema_obj = None):
        if not schema_obj:
            schema_obj = self._cur_schema_
        self.engine.execute('CREATE TABLE {}.{} ()'.format(schema_obj.schema, table_name))
        for col_dict in columns_list:
            self.add_column(col_dict={'col_name': col_dict['col_name'], 'col_type': col_dict['col_type']},
                            table_name=table_name)
        schema_obj.reflect()
        key_names = list(d['col_name'] for d in filter(lambda d: d['is_primary'] == True, columns_list))
        self._gen_table_(table_name, schema_obj, {'primary_key': key_names})
