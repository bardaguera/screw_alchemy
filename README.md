# screw_alchemy

SQLAlchemy wrapper for ETL purposes

## Initialization example
```python3
import screw_alchemy as sa
eng1 = {"instance": "dwh",
        "js":{
            "conn_string":"postgresql+psycopg2://user:upassword@instance:5432/database_name",
            "debug":False,
            "tables":{
                "book_shop": {
                    "sales": {'primary_key': ['order_id', 'product_id']},
                    "accounts": {'primary_key':['id']}
                    }
                }
            }
        }

dwh = sa.BaseInstance(**eng1)
dwh.gen_from_js()
```

## Create DB from json
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
    {"table_name":"book_price",
     "table_id":2527163,
     "attrs":[{"col_name":"id", "col_type":"uuid", "is_primary":true},
              {"col_name":"price", "col_type":"int4", "is_primary":false}
             ]
    }
  ]
}
```
## Schema restoration
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
## Query
```python3
from sqlalchemy.sql.expression import case, or_, cast, select, insert, update #, except_, and_
```

Apache License 2.0
