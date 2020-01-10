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


```

## simple

### tb01.sql

simple table

## column

Column decoding will be tested.

### tb02.sql

int

```
public static final String UNSIGNED_TINYINT = "TINYINT UNSIGNED";
public static final String UNSIGNED_SMALLINT = "SMALLINT UNSIGNED";
public static final String UNSIGNED_MEDIUMINT = "MEDIUMINT UNSIGNED";
public static final String UNSIGNED_INT = "INT UNSIGNED";
public static final String UNSIGNED_BIGINT = "BIGINT UNSIGNED";

public static final String TINYINT = "TINYINT";
public static final String SMALLINT = "SMALLINT";
public static final String MEDIUMINT = "MEDIUMINT";
public static final String INT = "INT";
public static final String BIGINT = "BIGINT";
```

### tb03.sql

datetime and timestamp

### tb04.sql

```
public static final String CHAR = "CHAR";
public static final String VARCHAR = "VARCHAR";
```

### tb05.sql

check charset utf8 and utf8mb4

### tb06.sql

varchar overflow

### tb07.sql

```
public static final String BINARY = "BINARY";
public static final String VARBINARY = "VARBINARY";
```

### tb08.sql

```
public static final String TINYTEXT = "TINYTEXT";
public static final String TEXT = "TEXT";
public static final String MEDIUMTEXT = "MEDIUMTEXT";
public static final String LONGTEXT = "LONGTEXT";
```

### tb09.sql

```
public static final String TINYBLOB = "TINYBLOB";
public static final String BLOB = "BLOB";
public static final String MEDIUMBLOB = "MEDIUMBLOB";
public static final String LONGBLOB = "LONGBLOB";
```

## mutiple B+ tree level

### tb10.sql

2 levels

### tb11.sql

3 levels.

Note that ibd file is too big, so only test under mysql56

## check null bitmap

### tb12.sql

## test table with insertion and deletion

### tb13.sql


## File preparation

### docker

```
export local_to=/Users/xu/IdeaProjects/innodb-java-reader/innodb-java-reader/src/test/resources/testsuite/mysql57
docker cp 35c18af70f8e:/var/lib/mysql/test/tb01.ibd  ${local_to}/simple
docker cp 35c18af70f8e:/var/lib/mysql/test/tb02.ibd  ${local_to}/column/int
docker cp 35c18af70f8e:/var/lib/mysql/test/tb03.ibd  ${local_to}/column/time
docker cp 35c18af70f8e:/var/lib/mysql/test/tb04.ibd  ${local_to}/column/char
docker cp 35c18af70f8e:/var/lib/mysql/test/tb05.ibd  ${local_to}/column/char
docker cp 35c18af70f8e:/var/lib/mysql/test/tb06.ibd  ${local_to}/column/char
docker cp 35c18af70f8e:/var/lib/mysql/test/tb07.ibd  ${local_to}/column/binary
docker cp 35c18af70f8e:/var/lib/mysql/test/tb08.ibd  ${local_to}/column/text
docker cp 35c18af70f8e:/var/lib/mysql/test/tb09.ibd  ${local_to}/column/blob
docker cp 35c18af70f8e:/var/lib/mysql/test/tb10.ibd  ${local_to}/multiple/level
docker cp 35c18af70f8e:/var/lib/mysql/test/tb12.ibd  ${local_to}/nullcolumn
docker cp 35c18af70f8e:/var/lib/mysql/test/tb13.ibd  ${local_to}/deletion
```

### local file system
```
sudo cp tb03.ibd ~/IdeaProjects/innodb-java-reader/innodb-java-reader/src/test/resources/testsuite/column/time
```