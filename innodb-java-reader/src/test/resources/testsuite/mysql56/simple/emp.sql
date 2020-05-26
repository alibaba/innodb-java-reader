DROP TABLE IF EXISTS `emp`;
DROP TABLE IF EXISTS `dept`;

CREATE TABLE `dept`
(`deptno` int(11) NOT NULL,
`name` varchar(64) NOT NULL,
PRIMARY KEY (`deptno`),
INDEX `idx_deptno` (`deptno`)
)
ENGINE=InnoDB;

CREATE TABLE `emp`
(`id` int(11) NOT NULL ,
`empno` bigint(20) NOT NULL,
`name` varchar(64) NOT NULL,
`deptno` int(11) NOT NULL,
`gender` char(1) NOT NULL,
`birthdate` date NOT NULL,
`city` varchar(100) NOT NULL,
`salary` int(11) NOT NULL,
`age` int(11) NOT NULL,
`joindate` timestamp NOT NULL,
`level` int(11) NOT NULL,
`profile` text CHARSET latin1 NOT NULL,
`address` varchar(500) default null COLLATE utf8_bin,
`email` varchar(100) default null,
KEY (`name`),
PRIMARY KEY (`id`),
INDEX `idx_city` (`city`),
INDEX (`age`),
UNIQUE KEY (`empno`),
KEY (`age`, `salary`),
KEY `key_join_date` (`joindate`),
KEY (`deptno`, `level`, `name`),
FULLTEXT (`profile`),
foreign key(deptno) references dept(deptno),
KEY (`deptno`, `level`, `empno`),
KEY `address` (`address`),
KEY `email` (`email`(3))
)
ENGINE=InnoDB DEFAULT CHARSET=latin1;

INSERT INTO dept VALUES (10,'ACCOUNTING');
INSERT INTO dept VALUES (20,'RESEARCH');
INSERT INTO dept VALUES (30,'SALES');
INSERT INTO dept VALUES (40,'OPERATIONS');

SET @@session.time_zone = "+00:00";
INSERT INTO emp VALUES (1, 100, 'Eric', 20, 'M', '1983-10-23', 'New York', 52000, 30, '2020-01-01 18:35:40', 6, '', null, 'eric@test.com');
INSERT INTO emp VALUES (2, 101, 'Neo', 10, 'M', '1986-10-02', 'Berlin', 68000, 33, '2018-04-09 09:00:00', 8, '', 'main street', 'neo@test.com');
INSERT INTO emp VALUES (3, 102, 'Sarah', 20, 'F', '1990-07-25', 'LA', 20000, 27, '2019-11-16 10:26:40', 4, 'Hello world', null, 'sarah@test.com');
INSERT INTO emp VALUES (4, 105, 'Json', 30, 'M', '1959-02-14', 'Beijing', 100000, 60, '2015-03-09 22:16:30', 12, 'Start', null, 'json@test.com');
INSERT INTO emp VALUES (5, 106, 'SMITH', 10, 'M', '1981-01-05', 'Tokyo', 39000, 25, '2018-09-02 12:12:56', 6, '', null, 'smith@test.com');
INSERT INTO emp VALUES (6, 107, 'lucy', 40, 'F', '1989-06-07', 'New York', 40000, 30, '2018-06-01 14:45:00', 5, REPEAT('p', 1000), null, 'lucy@test.com');
INSERT INTO emp VALUES (7, 108, 'JAMES', 20, 'M', '1992-05-06', 'LA', 29000, 20, '2017-08-18 23:11:06', 3, '', null, 'james@test.com');
INSERT INTO emp VALUES (8, 109, 'John', 40, 'M', '1989-06-07', 'New York', 32000, 42, '2018-06-01 14:45:00', 6, '', null, 'john@test.com');
INSERT INTO emp VALUES (9, 110, 'MILLER', 30, 'F', '1982-07-04', 'New York', 68000, 40, '2020-01-02 12:19:00', 7, '', null, 'miller@test.com');
INSERT INTO emp VALUES (10, 111, 'Jane', 20, 'F', '1995-08-29', 'LA', 19000, 22, '2019-09-30 02:14:56', 5, '', null, 'jane@test.com');
INSERT INTO emp VALUES (11, 112, 'Sarah', 40, 'F', '1988-11-23', 'New York', 21000, 26, '2017-04-27 16:27:11', 7, REPEAT('apple', 40), null, 'sarah02@test.com');
INSERT INTO emp VALUES (12, 113, 'Paul', 30, 'M', '1984-11-06', 'Berlin', 20000, 43, '2019-07-28 12:12:12', 9, '', null, 'paul@test.com');
INSERT INTO emp VALUES (13, 114, 'Lara', 20, 'F', '1987-01-21', 'Beijing', 35000, 29, '2018-06-08 12:12:12', 6, '', '老北京胡同Z区', 'lara@test.com');
INSERT INTO emp VALUES (14, 115, 'ADAMS', 20, 'M', '1993-04-15', 'LA', 38000, 35, '2019-06-08 12:12:12', 5, '', 'LA 001', 'adams@test.com');
INSERT INTO emp VALUES (15, 116, 'SMITH', 30, 'M', '1986-07-25', 'Beijing', 55000, 36, '2017-08-17 22:01:37', 8, '', null, 'smith02@test.com');
INSERT INTO emp VALUES (16, 120, 'Scott', 20, 'M', '1990-03-04', 'Berlin', 33000, 31, '2018-08-17 22:01:37', 7, '', null, 'scott@test.com');
INSERT INTO emp VALUES (17, 121, 'MARTIN', 10, 'F', '1975-02-28', 'Tokyo', 63000, 45, '2017-06-09 12:01:37', 9, '', null, 'martin@test.com');
INSERT INTO emp VALUES (18, 122, 'kidd', 20, 'm', '1988-05-17', 'new York', 37000, 29, '2019-12-31 00:15:30', 5, REPEAT('phone', 50), 'Queen zone', 'kidd@test.com');
INSERT INTO emp VALUES (19, 123, 'Yue', 30, 'F', '1979-11-09', 'New York', 57000, 37, '2017-03-04 16:16:32', 7, '', null, 'yue@test.com');
INSERT INTO emp VALUES (20, 124, 'Oscar', 20, 'M', '1988-10-08', 'LA', 36000, 27, '2018-03-04 16:16:32', 6, '', null, 'oscar@test.com');

ALTER TABLE emp ADD INDEX `key_birthdate` (`birthdate`);
ALTER TABLE emp ADD INDEX `key_level` (`level`);
DROP INDEX `key_birthdate` on emp;

