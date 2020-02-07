# screw_alchemy

SQLAlchemy wrapper for ETL purposes

## Initialization example
```python3
import screw_alchemy as sa
eng1 = {"instance": "dev_dwh",
        "js":{
            "conn_string":"postgresql+psycopg2://user:upassword@instance:5432/databaase_name",
            "debug":False,
            "tables":{
                "schema_name": {
                    "table_name": "{\'primary_key\': [__table__.c.column_which_is_part_of_composite_key,\
                                                      __table__.c.second_part_of_composite_key]}",
                    "another_table_name": {'primary_key':['id']}
                    }
                }
            }
        }

dev_dwh = sa.BaseInstance(**eng1)
dev_dwh.gen_from_js()
dev_dwh.dispose()
dev_dwh.gen_from_js()
```

## Create DB from json
```json
{"schema":"public",
  "tables":[
    {"table_name":"Book",
      "table_id":2527151,
      "attrs":[
        {"col_name":"id",
         "col_type":"uuid",
         "is_primary":true},
        {"col_name":"created_on",
         "col_type":"timestamp",
         "is_primary":false},
        {"col_name":"Name",
         "col_type":"varchar",
         "is_primary":false}]},
    {"table_name":"BookPrice",
     "table_id":2527163,
     "attrs":[
        {"col_name":"id",
         "col_type":"uuid",
         "is_primary":true},
        {"col_name":"Price",
         "col_type":"int4",
         "is_primary":false}]
    }
  ]
}
```
base.add_column(col_dict = {'col_name': 'surname', 'col_type': 'varchar'}, table_name = 'employee')
## Query
```python3
from sqlalchemy.sql.expression import case, or_, cast, select, insert, update #, except_, and_
```

Apache License 2.0
