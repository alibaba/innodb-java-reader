DROP TABLE IF EXISTS tb23;
CREATE TABLE tb23 (
c1 VARCHAR(30) NOT NULL,
c2 VARCHAR(30),
c3 VARCHAR(30) NOT NULL,
c4 VARCHAR(30),
c5 VARCHAR(30) NOT NULL,
c6 VARCHAR(30),
c7 VARCHAR(30) NOT NULL,
c8 VARCHAR(30),
c9 VARCHAR(30) NOT NULL,
c10 VARCHAR(30),
c11 VARCHAR(30) NOT NULL,
c12 VARCHAR(30),
PRIMARY KEY (c5, c3, c9)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


insert into tb23 values(
concat('1', REPEAT('c', 1)), concat('2', REPEAT('c', 2)),
concat('3', REPEAT('c', 3)), null,
concat('5', REPEAT('c', 5)), null,
concat('7', REPEAT('c', 7)), concat('8', REPEAT('c', 8)),
concat('9', REPEAT('c', 9)), null,
concat('y', REPEAT('c', 11)), concat('z', REPEAT('c', 12))
);

insert into tb23 values(
concat('1', REPEAT('a', 1)), null,
concat('3', REPEAT('a', 3)), concat('4', REPEAT('a', 4)),
concat('5', REPEAT('a', 5)), concat('6', REPEAT('a', 6)),
concat('7', REPEAT('a', 7)), null,
concat('9', REPEAT('a', 9)), concat('x', REPEAT('a', 10)),
concat('y', REPEAT('a', 11)), concat('z', REPEAT('a', 12))
);

insert into tb23 values(
concat('1', REPEAT('b', 1)), concat('2', REPEAT('b', 2)),
concat('3', REPEAT('b', 3)), null,
concat('5', REPEAT('b', 5)), null,
concat('7', REPEAT('b', 7)), concat('8', REPEAT('b', 8)),
concat('9', REPEAT('b', 9)), concat('x', REPEAT('b', 10)),
concat('y', REPEAT('b', 11)), null
);
