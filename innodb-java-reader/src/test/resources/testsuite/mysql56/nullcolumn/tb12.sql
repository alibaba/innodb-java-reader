DROP TABLE IF EXISTS `tb12`;
CREATE TABLE `tb12`
(`id` int(11) NOT NULL AUTO_INCREMENT,
`a` bigint(20) default 999,
`b` varchar(32) NOT NULL,
`c` varchar(32),
`d` varchar(32) default 'sorry',
`e` text NOT NULL,
`f` varchar(32),
PRIMARY KEY (`id`))
ENGINE=InnoDB;

insert into tb12(a, b, c, d, e, f) values(1, REPEAT('a1', 16), REPEAT('a1', 16), REPEAT('a1', 16), REPEAT('a1', 16), REPEAT('a1', 16));
insert into tb12(b, c, d, e) values(REPEAT('a2', 16), REPEAT('a2', 16), REPEAT('a2', 16), REPEAT('a2', 16));
insert into tb12(a, b, d, e) values(2, REPEAT('a3', 16), REPEAT('a3', 16), REPEAT('a3', 16));
insert into tb12(a, b, d, e, f) values(3, REPEAT('a4', 16), REPEAT('a4', 16), REPEAT('a4', 16), REPEAT('a4', 16));

mysql> select * from tb12;
+----+------+----------------------------------+----------------------------------+----------------------------------+----------------------------------+----------------------------------+
| id | a    | b                                | c                                | d                                | e                                | f                                |
+----+------+----------------------------------+----------------------------------+----------------------------------+----------------------------------+----------------------------------+
|  1 |    1 | a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1 | a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1 | a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1 | a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1 | a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1 |
|  2 |  999 | a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2 | a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2 | a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2 | a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2 | NULL                             |
|  3 |    2 | a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3 | NULL                             | a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3 | a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3 | NULL                             |
|  4 |    3 | a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4 | NULL                             | a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4 | a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4 | a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4 |
+----+------+----------------------------------+----------------------------------+----------------------------------+----------------------------------+----------------------------------+
4 rows in set (0.00 sec)