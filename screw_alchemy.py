#!/usr/bin/env python
# coding: utf-8

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
    """deprecated"""
    res = query.all()
    #res = conn.execute(query)
    #return [dict(r) for r in res]
    return [r._asdict() for r in res]


def transfer_columns(srs_meta, dest_table_obj):
    for column in srs_meta.columns:
        if column.autoincrement:
            column.autoincrement = False
            #print(column.server_default)
            column.server_default = None
        dest_table_obj.append_column(column.copy())
    return dest_table_obj

def get_columns(self, mode='general'):
    '''
        'general'
            Desc: wrapper for .__table__.columns sqlalchemy method
            Out: [{'name': 'id', 'type': UUID(), 'nullable': False} ...]
        'full'
            Desc: format suitable for add_table, add_column methods
            Out: [{'col_name': 'id', 'col_type': UUID(), 'nullable': True,
                   'is_primary': True}, ...]
        'names-only'
            Desc: list of column names
            Out: ['id', ...]
        'columns'
            Desc: list of columns as objects
            Out: [Column('id', INTEGER(), table=<some_table>), ...]
    '''
    col_list = []
    if mode == 'columns':
         col_list = self.__table__.columns
    else:
        for col in self.__table__.columns:
            col_list.append({'name':col.name,
                             'type':col.type,
                             'nullable': col.nullable})

    if mode == 'names-only':
        col_list = [c['name'] for c in col_list]
    elif mode == 'full':
        keys = self.get_keys_columns()
        for d in col_list:
            d['col_name'] = d['name']
            d['col_type'] = d['type']
            if d['name'] in keys:
                d['is_primary'] = True
            else:
                d['is_primary'] = False

            del d['name']
            del d['type']

    return col_list

def get_keys_columns(self):
    return self._mapped_keys_

def get_schema(self):
    '''table function'''
    return {'schema_name': self.__dict__['__table__'].schema,
            'schema_obj': self.__dict__['__table__'].metadata}

def generate_on_clause(base, ltable_name, rtable_name, keys):
    '''
    :base name of the BaseInstance object; for example bpm
    :ltable_name {base}.{table_name}; e.g. 'opportunity'
    :rtable_name {base}.{table_name}; e.g. 'lead'
    :keys [['id', 'opportunity_id']]

    returns 'dwh.opportunity.id == dwh.lead.opportunity_id']
    '''
    key_map = []
    for ids in keys:
        if type(ids) == int:
            ltable_id, rtable_id = ids, ids
        else:
            ltable_id, rtable_id = ids[0], ids[1]
        key_pair = '{}.{}.{}=={}.{}.{}'
        key_pair = key_pair.format(base, ltable_name, ltable_id,
                                   base, rtable_name, rtable_id)
        key_map.append(key_pair)

    if len(key_map) > 1:
        key_map = 'and_('+','.join(key_map)+')'
    else:
        key_map = ','.join(key_map)
    return key_map

def _ArgsConstr(m_args):
    if '__table__.c' in m_args:
        return m_args
    else:
        key_columns = str(list(map(lambda x:'__table__.c.'+x, m_args['primary_key']))).replace("'",'')
        return "{\'primary_key\': "+ key_columns+"}"
    
def _UpdInstanceTypeMapping(engine, s):
    # Dict values could be: 
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
                    'jsonb': {'postgresql':sqlalchemy.dialects.postgresql.JSON,
                            'default':sqlalchemy.types.JSON},
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
    
def _UpdInstanceAddColumn(self, col_dict,
                          table_name = None,
                          schema_name = None):    
    '''Two use-cases: self can be a base_object or a table_obj'''
    if schema_name:
        self.set_cur_schema(schema_name)
    if 'BaseInstance' in str(self.__class__):
        loc_engine_obj = self.engine
        if not table_name:
            loc_table_name = self._cur_table.__tablename__
        else:
            loc_table_name = table_name
    else:
        loc_table_name = self.__tablename__
        loc_engine_obj = self._engine_link_
        if not schema_name:
            loc_schema_name = self.get_schema()['schema_name']

    col_type = col_dict['col_type']

    if type(col_type) is str:
        if not _UpdInstanceTypeMapping(loc_engine_obj, col_type):
            print('TYPE DOES NOT FOUND %s' % col_type)
            return None
        col_type = _UpdInstanceTypeMapping(loc_engine_obj, col_type)

    column_obj = Column(col_dict['col_name'], col_type, nullable=True)
    qs ='ALTER TABLE {}."{}" ADD COLUMN {} {};'
    qs = qs.format(self._cur_schema.schema, #One shall not add a quotes around column name
                   loc_table_name,
                   column_obj.compile(dialect=loc_engine_obj.dialect),
                   column_obj.type.compile(loc_engine_obj.dialect))
    try:
        loc_engine_obj.execute(qs)
    except SQLAlchemyError as se:
        #TODO: log there
        pass
    else:
        self._cur_schema.reflect(only=[loc_table_name],
                                 extend_existing=True)
    

class BaseInstance(object):
    def __init__(self, instance = None, js = None):
        self.instance = str(instance)
        self._js = json.loads(json.dumps(js))
        self.engine = None
        self.session = None
        self._cur_schema = None
        self._cur_table = None
        self.status = {}

    def __getitem__(self, table_name):
        return eval('self.{}'.format(table_name))

    def _gen_engine_(self, conn_str, debug=False):
        self.engine = create_engine(conn_str,
                                    echo='debug' if debug else False)
        Session = sessionmaker(bind=self.engine, autocommit=True)
        self.session = Session()
        
    
    def _gen_schema_(self, schema_name, schema_objects):
        schema_obj_name = schema_name+'_meta'
        if schema_name in ('default'):
            setattr(self, schema_obj_name, MetaData(bind=self.engine))
        else:
            setattr(self, schema_obj_name, MetaData(bind=self.engine,
                                                    schema=schema_name))
        self.set_cur_schema(schema_name)

        try:
            if type(schema_objects) != bool:
                self._cur_schema.reflect(only=schema_objects)
            else:
                self._cur_schema.reflect()
        except SQLAlchemyError as e:
            self.status[schema_name] = e._code_str


    def _overload_functions_to_table(self, table_name, m_args):
        self._cur_table = self[table_name]
        if m_args:
            pk = args.get('primary_key', None)
            self._cur_table._mapped_keys_ = pk
        self._cur_table._engine_link_ = self.engine
        self._cur_table.get_columns = MethodType(get_columns,
                                                self._cur_table)
        self._cur_table.get_keys_columns = MethodType(get_keys_columns,
                                                self._cur_table)
        self._cur_table.get_schema = MethodType(get_schema,
                                                self._cur_table)
        self._cur_table.add_column = MethodType(_UpdInstanceAddColumn,
                                                self._cur_table)
    
    def _gen_table(self, table_name,
                    schema_obj = None, t_args = None, m_args = None):
        if not schema_obj:
            schema_obj = self._cur_schema
        Base = declarative_base()
        __table__ = Table(table_name, schema_obj,
            autoload=True, autoload_with=self.engine)

        table_dict = {'__tablename__':table_name,
                      '__table__': __table__}
        if m_args:
            table_dict['__mapper_args__'] = eval(_ArgsConstr(m_args))
        if t_args:
            table_dict['__table_args__'] = eval(_ArgsConstr(t_args))

        try:
            #type(name, bases, dict) -> a new type
            setattr(self, table_name, type(str(table_name),
                                      (Base,),
                                      table_dict
                                      ))
        except ArgumentError as e:
            table_sgn = schema_obj.schema+'.'+table_name
            table_data = schema_obj.tables[table_sgn]
            all_cols = list(c.name for c in table_data.columns)
            self._gen_table(table_name = table_name,
                            schema_obj =schema_obj,
                            m_args = {'primary_key':all_cols})
        else:
            self._overload_functions_to_table(table_name, m_args)
        
        
    def _check_connection_(self):
        try:
            self.engine.connect()
        except:
            eng_name = '{}_engine'.format(self.instance)
            self.status[eng_name] = "No connection"
    
    def gen_instance(self):
        self._gen_engine_(self._js['conn_string'], self._js['debug'])
        self._check_connection_()
        if self._js.get('default_schema', False) is True:
            self._gen_schema_('default', None)

        for schema_name in self._js['tables'].keys():
            schema_objects = self._js['tables'][schema_name]
            self._gen_schema_(schema_name, schema_objects)
            
            if type(schema_objects) != bool:
                for table, m_args in schema_objects.items():
                    self._gen_table(table_name=table, m_args=m_args)
                    
    def log(self, log_path, json_log):
        with open(log_path, 'a') as f:
            json.dump(json_log, f)
    
    def dispose(self):
        self.engine.dispose()
        
    def add_column(self, col_dict, table_name, schema_name):
        """Add column to database by table name.
        add_column(col_dict = {'col_name': 'surname',
                               'col_type': 'varchar'},
                   table_name = 'employee',
                   schema_name='public')"""
        _UpdInstanceAddColumn(self, col_dict,
                              table_name = table_name,
                              schema_name=schema_name)

    def add_table(self, table_name, columns_list,
                  prefixes = [], postfixes = [],
                  schema_obj = None, recreate=False):
        """Add table with columns to database,
        update schema and reflect this table to Base instance.
        Base.create_table(table_name, columns_list, schema_obj = None)
    
        :table_name str
        :columns_list columns_list [{'col_name': 'created_on',
                                     'col_type': 'timestamp',
                                     'is_primary': False}];
                      if you already have table object,
                      get these guys out of get_columns(mode='general')
        :prefixes = []
        :postfixes = []
        :schema_obj; default: last used schema
        :recreate; default: False
        """
        if not schema_obj:
            schema_obj = self._cur_schema

        if recreate:
            drop_q = 'DROP TABLE IF EXISTS {}."{}";'
            drop_q = drop_q.format(schema_obj.schema, table_name)
            self.engine.execute(drop_q)
            if hasattr(self, table_name):
                delattr(self, table_name)
        
        create_q = 'CREATE {} TABLE {}."{}" () {};'
        create_q = create_q.format(' '.join(prefixes),
                                   schema_obj.schema,
                                   table_name,
                                   ' '.join(postfixes))
        self.engine.execute(create_q)
        for col_dict in columns_list:
            col_pair={'col_name': col_dict['col_name'],
                      'col_type': col_dict['col_type']}
            self.add_column(col_pair,
                            table_name=table_name,
                            schema_name=schema_obj.schema)
        try:
            schema_obj.reflect(only=[table_name], extend_existing=True)
        except:
            return False
        else:
            key_names = []
            for d in columns_list:
                if d['is_primary'] == True:
                    key_names.append(d['col_name'])
            self._gen_table(table_name, schema_obj,
                            {'primary_key': key_names})
        return True
    
    def _transfer_cols_to_new_obj(self, table_name, schema_obj,
                                  src_meta_columns):
        dest_table_obj = Table(table_name, schema_obj)
        for column in src_meta_columns:
                dest_table_obj.append_column(column.copy())
        return dest_table_obj

    def mimic_table(self, table, target_name = '',
                    schema_obj = None, recreate=True):
        '''
        :table could be a table object or the name of table
        :target_name specifies name of created object; default: {source_name}_temp
        :schema_obj a schema object; if not given, temporary table will be created
        :recreate table; default: True
        '''
        res = True
        if type(table) == str:
            table_name = table
            table = self[table]
        else:
            table_name = table.__tablename__

        if not target_name:
            target_name = '{}_temp'.format(table_name)

        if recreate:
            s = ''
            if schema_obj:
                s = schema_obj.schema+'.'
            drop_q = 'DROP TABLE IF EXISTS {}"{}";'
            drop_q.format(s, target_name)
            self.engine.execute(drop_q)
            if hasattr(self, target_name):
                delattr(self, target_name)

        if schema_obj is None:
            m_args = None
            col_obj_list = table.get_columns('columns')
            dest_table_obj = Table(target_name,
                                   self.default_meta,
                                   prefixes=['TEMPORARY'])

            for column in col_obj_list:
                dest_table_obj.append_column(column.copy())

            dest_table_obj.create(self.session.connection(),
                                  checkfirst=True)
            #type(name, bases, dict) -> a new type
            for c in dest_table_obj.c:
                setattr(dest_table_obj, c.name, c)
            setattr(self, target_name, dest_table_obj)
            self._overload_functions_to_table(target_name, m_args)
        else:
            col_desc_list = table.get_columns('full')
            res = self.add_table(table_name=target_name,
                                 columns_list=col_desc_list,
                                 schema_obj=schema_obj,
                                 recreate=recreate)
        return res


    def reflect_table(self, table_name,
                      schema_obj = None, m_args = None):
        self._gen_table(table_name=table_name,
                        schema_obj = schema_obj, m_args = m_args)

    def set_cur_schema(self, target_schema_name):
        self._cur_schema = eval('self.'+target_schema_name + '_meta')
        
    def whoami(self):
        print(self.engine.engine)

    def fetch_to_dicts(self, query_obj):
        '''returns [status, [dict(), dict()], If error status == False'''
        try:
            res = self.engine.execute(query_obj)
        except SQLAlchemyError as e:
            return [False, e]
        rows = res.fetchall()
        query_cols = [str(c) for c in query_obj.columns]

        return [True, [dict(zip(query_cols, r)) for r in rows]]
