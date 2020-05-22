## 1. How to specify secondary key root page number?

Currently the framework is not capable of locating secondary key root page number through metadata in System Tablespace.

For example, suppose we have a table with many keys, primary key (clustered index) root page number will be 3, for index named `age`, the root page number will be 8.
```
     mysql> SELECT * FROM INFORMATION_SCHEMA.INNODB_SYS_INDEXES WHERE TABLE_ID = 3399;
     +----------+------------------+----------+------+----------+---------+-------+
     | INDEX_ID | NAME             | TABLE_ID | TYPE | N_FIELDS | PAGE_NO | SPACE |
     +----------+------------------+----------+------+----------+---------+-------+
     |     5969 | PRIMARY          |     3399 |    3 |        1 |       3 |  3385 |
     |     5975 | FTS_DOC_ID_INDEX |     3399 |    2 |        1 |       4 |  3385 |
     |     5976 | empno            |     3399 |    2 |        1 |       5 |  3385 |
     |     5977 | name             |     3399 |    0 |        1 |       6 |  3385 |
     |     5978 | idx_city         |     3399 |    0 |        1 |       7 |  3385 |
     |     5979 | age              |     3399 |    0 |        1 |       8 |  3385 |
     |     5980 | age_2            |     3399 |    0 |        2 |       9 |  3385 |
     |     5981 | key_join_date    |     3399 |    0 |        1 |      10 |  3385 |
```

Now the way to find secondary key root page number is by finding clustered index root page and calculate sk page number based on it. 

Note that if table has ever been altered to add or remove indices, the secondary key root page number may be incorrect, and cause error.

More standard way would be to look up root page number by: 
`SELECT * FROM INFORMATION_SCHEMA.INNODB_SYS_INDEXES;` before 5.7 or `SELECT * FROM INFORMATION_SCHEMA.INNODB_INDEXES;` after 8.0.

If you are using API, you can set secondary key root page number like below.

```
try {
    ThreadContext.init();
    ThreadContext.putSkRootPageNumber(8L);
} finally {
    ThreadContext.clean();
}
```

If you are using API, you can specify `-skrootpage` argument.