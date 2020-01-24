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
                    "another_table_name": "{\'primary_key\': [__table__.c.primary_key_column]}"
                    }
                }
            }
        }

dev_dwh = sa.BaseInstance(**eng1)
dev_dwh.gen_from_js()
dev_dwh.dispose()
dev_dwh.gen_from_js()
```

## Query
```python3
from sqlalchemy.sql.expression import case, or_, cast, select, insert, update #, except_, and_
```

Apache License 2.0
