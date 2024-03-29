# screw_alchemy

SQLAlchemy wrapper for ETL purposes. You can create a database structure out of JSON description, which is handy if the master-data system frequently changes the structure of its tables.

## Initialization example

In this example, we have the database named DB with two schemas: "book_shop" and "history".
With the initialization of the BaseInstance class, this json will be loaded.
With the call of the gen_instance() method connection will be created and all mentioned schemas and tables will be reflected on the MetaData and Table objects respectively, and pointers to them will be set to the Base class as if they were attributes.

```python3
import screw_alchemy as sa
eng1 = {"instance": "dwh",
        "js":{
            "conn_string":"postgresql+psycopg2://user:upassword@instance:5432/DB",
            "debug":False,
            "tables":{
                "book_shop": {
                    "sales": {'primary_key': ['order_id', 'product_id']},
                    "accounts": {'primary_key':['id']}
                    },
                "history": {
                    },
                },
             "default_schema": True
            }
        }

dwh = sa.BaseInstance(**eng1)
dwh.gen_instance()
```
dwh | Attribute | Desc
------------ | ------------- | -------------
 &nbsp; | engine | sqlalchemy.engine.base.Engine
 &nbsp; | session | sqlalchemy.orm.session.Session
 &nbsp; | instance | instance name
 &nbsp; | book_shop_meta | sqlalchemy.sql.schema.MetaData for the book_shop schema |
 &nbsp; | history_meta | MetaData for the history schema |
 &nbsp; | default_meta | MetaData for the temporary tables |
 &nbsp; | sales | sqlalchemy.ext.declarative.api.DeclarativeMeta for book_shop.sales |
 &nbsp; | accounts | sqlalchemy.ext.declarative.api.DeclarativeMeta for book_shop.sales |
 &nbsp; | dispose() | dispose connection 
 &nbsp; | whoami()  | returns engine description
 &nbsp; | reflect_table() | MetaData.reflect
 &nbsp; | mimic_table() | 
 &nbsp; | add_table() | 
 &nbsp; | add_column() | 
 
## Create DB from json
Imagine master system added two new tables: books and book_price. You can ask them to send you the description of the database in the SQLAlchemy-alike format.

```json
{"schema":"book_shop",
  "tables":[
    {"table_name":"books",
      "table_id":2527151,
      "attrs":[{"col_name":"id", "col_type":"uuid", "is_primary":true},
               {"col_name":"created_on", "col_type":"timestamp", "is_primary":false},
               {"col_name":"name", "col_type":"varchar", "is_primary":false}
              ]
    },
    {"table_name":"book_prices",
     "table_id":2527163,
     "attrs":[{"col_name":"id", "col_type":"uuid", "is_primary":true},
              {"col_name":"price", "col_type":"int4", "is_primary":false}
             ]
    }
  ]
}
```
This is an example of schema restoration:
```
def get_col_diff(attrs, table_obj):
    diff = set(d['col_name'] for d in attrs)
    diff -= set(c['name'] for c in table_obj.get_columns())
    return diff
    
js_schema = json.load(js_file)
for cur_table_desc in js_schema['tables']:
    if not dwh.engine.has_table(cur_table_desc['table_name'], 'book_shop'):
        table_name = cur_table_desc['table_name']
        dwh.add_table(table_name, cur_table_desc['attrs'], schema_obj = dwh.book_shop_meta, recreate=True)
        
        key_names = list(d['col_name'] for d in filter(lambda d: d['is_primary'] == True, cur_table_desc['attrs']))
        dwh.reflect_table(table_name, schema_obj = dwh.book_shop_meta, m_args = {'primary_key':key_names})
        
        diff_cols = get_col_diff(cur_table_desc['attrs'], eval('dwh.{}'.format(table_name)))
        if diff_cols:
            for col_dict in cur_table_desc['attrs']:
                #base.add_column(col_dict = {'col_name': 'surname', 'col_type': 'varchar'}, table_name = 'employee')
                dwh.add_column(col_dict = col_dict, table_name = table_name, schema_name = 'book_shop')
```

After runnung the code tables books and book_prices will be added to the database ans then reflected to the dwh instance.

## Query
```python3
from sqlalchemy.sql.expression import case, or_, cast, select, insert, update #, except_, and_
```

Apache License 2.0
