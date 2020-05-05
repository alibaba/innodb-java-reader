# MySQL type to Java Type mapping

| MySQL Type                                              | Java Type  |
| ------------------------------------------------------- | ---------- |
| TINYINT, SMALLINT, MEDIUMINT, INT (including unsigned)  | Integer    |
| BIGINT                                                  | Long       |
| UNSIGNED_BIGINT                                         | BigInteger |
| CHAR, VARCHAR, TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT     | String     |
| BINARY, VARBINARY, TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB | byte[]     |
| DATETIME, TIMESTAMP, TIME, DATE                         | String     |
| YEAR                                                    | Short      |
| FLOAT                                                   | Float      |
| DOUBLE                                                  | Double     |
| DECIMAL                                                 | BigDecimal |
| BOOL, BOOLEAN                                           | Boolean    |
