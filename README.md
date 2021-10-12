# mariadb-query-manager
MariaDB or other MySQL compatible Database Query Manager

## Usage
See `Usage.ipynb`

### Import library & instantiate class
```python
from MariaDbQueryManager import MariaDbQueryManager

mariadb_query_manager = MariaDbQueryManager(host='', port=3306, username='', password='', default_database='')
```

### Get table from database
```python
test_df = mariadb_query_manager.get_dataframe('daily_prices')
```

### Insert table into database
```python
mariadb_query_manager.insert_dataframe(test_df, 'test')
```

### Execute SQL with result values
```python
mariadb_query_manager.execute_then_fetch_sql('SELECT COUNT(0) FROM `test`')
```

### Execute SQL without result values
```python
mariadb_query_manager.execute_sql('ALTER TABLE `test` MODIFY `code` VARCHAR(8)')
```

### Purge table
```python
mariadb_query_manager.purge_table('test')
```