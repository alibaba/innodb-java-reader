# Test cases documentation

https://dev.mysql.com/doc/refman/5.7/en/innodb-row-format.html

Innodb-java-reader supports both **COMPACT** and **DYNAMIC** row format. Even row format are the same across
different versions, to make sure Innodb-java-reader works correctly, tests will be run under
all major MySQL versions including 5.6, 5.7 and 8.0, so there are different folders.

Version list:
```
+-----------+
| version() |
+-----------+
| 5.6.39    |
+-----------+

+-----------------------------+
| version()                   |
+-----------------------------+
| 5.7.27-0ubuntu0.16.04.1-log |
+-----------------------------+

+-----------+
| version() |
+-----------+
| 8.0.18    |
+-----------+
```
