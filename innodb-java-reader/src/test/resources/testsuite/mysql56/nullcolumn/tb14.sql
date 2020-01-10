DROP TABLE IF EXISTS `tb14`;
CREATE TABLE `tb14`
(`id` int(11) NOT NULL,
`a1` varchar(10) NOT NULL,
`a2` varchar(10) ,
`a3` varchar(10) NOT NULL,
`a4` varchar(10) ,
`a5` varchar(10) NOT NULL,
`a6` varchar(10) ,
`a7` varchar(10) NOT NULL,
`a8` varchar(10) ,
`a9` varchar(10) NOT NULL,
`a10` varchar(10) ,
`a11` varchar(10) NOT NULL,
`a12` varchar(10) ,
`a13` varchar(10) NOT NULL,
`a14` varchar(10) ,
`a15` varchar(10) NOT NULL,
`a16` varchar(10) ,
`a17` varchar(10) NOT NULL,
`a18` varchar(10) ,
PRIMARY KEY (`id`))
ENGINE=InnoDB;

insert into tb14 values(1, 'a1', null, 'a3', null, 'a5', null, 'a7', null, 'a9', null, 'a11', null,'a13', null,'a15', null,'a17', null);


mysql> select * from tb14;
+----+----+------+----+------+----+------+----+------+----+------+-----+------+-----+------+-----+------+-----+------+
| id | a1 | a2   | a3 | a4   | a5 | a6   | a7 | a8   | a9 | a10  | a11 | a12  | a13 | a14  | a15 | a16  | a17 | a18  |
+----+----+------+----+------+----+------+----+------+----+------+-----+------+-----+------+-----+------+-----+------+
|  1 | a1 | NULL | a3 | NULL | a5 | NULL | a7 | NULL | a9 | NULL | a11 | NULL | a13 | NULL | a15 | NULL | a17 | NULL |
+----+----+------+----+------+----+------+----+------+----+------+-----+------+-----+------+-----+------+-----+------+