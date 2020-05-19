-- MySQL dump 10.13  Distrib 5.6.39, for osx10.12 (x86_64)
--
-- Host: localhost    Database: test
-- ------------------------------------------------------
-- Server version	5.6.39

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `ad_campaign`
--

DROP TABLE IF EXISTS `ad_campaign`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
# WARNING: Cannot generate character set or collation names without the --server option.
# CAUTION: The diagnostic mode is a best-effort parse of the .frm file. As such, it may not identify all of the components of the table correctly. This is especially true for damaged files. It will also not read the default values for the columns and the resulting statement may not be syntactically correct.
# Reading .frm file for test2.frm:
# The .frm file is a TABLE.
# CREATE TABLE Statement:

CREATE TABLE `ad_campaign` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `user_id` bigint(20) NOT NULL,
  `name` varchar(128) NOT NULL,
  `budget` mediumint(9) NOT NULL,
  `state` smallint(6) NOT NULL,
  `create_time` datetime NOT NULL,
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_user_id` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='ad_campaign';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `date_table`
--

DROP TABLE IF EXISTS `date_table`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `date_table` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `d1` datetime NOT NULL,
  `d2` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `d3` datetime NOT NULL,
  `d4` time NOT NULL,
  `d5` date NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='date_table';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `emps`
--

DROP TABLE IF EXISTS `emps`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `emps` (
  `empid` bigint(20) NOT NULL,
  `deptno` int(11) NOT NULL DEFAULT '0',
  `name` varchar(10) NOT NULL,
  `salary` decimal(6,2) NOT NULL,
  `age` mediumint(9) NOT NULL,
  `join_date` datetime NOT NULL,
  PRIMARY KEY (`deptno`,`empid`,`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `keyword`
--

DROP TABLE IF EXISTS `keyword`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `keyword` (
  `id` bigint(20) NOT NULL,
  `keyword` varchar(200) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_keyword` (`keyword`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='keyword';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `nullable_table`
--

DROP TABLE IF EXISTS `nullable_table`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `nullable_table` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `int_value` int(11) DEFAULT NULL COMMENT '测试字段',
  `long_value` bigint(20) DEFAULT NULL COMMENT '测试字段',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8 COMMENT='nullable_table';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `t`
--

DROP TABLE IF EXISTS `t`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `t` (
  `id` int(11) NOT NULL,
  `a` bigint(20) NOT NULL,
  `b` varchar(64) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `t1`
--

DROP TABLE IF EXISTS `t1`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `t1` (
  `id` int(11) NOT NULL,
  `a` int(11) DEFAULT NULL,
  `b` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `a` (`a`),
  KEY `b` (`b`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `t2`
--

DROP TABLE IF EXISTS `t2`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `t2` (
  `id` int(11) NOT NULL,
  `a` bigint(20) NOT NULL,
  `b` varchar(64) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `t3`
--

DROP TABLE IF EXISTS `t3`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `t3` (
  `id` int(11) NOT NULL,
  `a` int(11) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `t4`
--

DROP TABLE IF EXISTS `t4`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `t4` (
  `id` int(11) NOT NULL,
  `b` varchar(32) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `t5`
--

DROP TABLE IF EXISTS `t5`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `t5` (
  `id` int(11) NOT NULL,
  `b` varchar(32) DEFAULT 'ABC',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `t6`
--

DROP TABLE IF EXISTS `t6`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `t6` (
  `id` int(11) NOT NULL,
  `b` varchar(32) DEFAULT 'ABC',
  `c` varchar(32) NOT NULL,
  `d` varchar(32) DEFAULT 'DEF',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `t7`
--

DROP TABLE IF EXISTS `t7`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `t7` (
  `id` int(11) NOT NULL,
  `b` varchar(32) DEFAULT 'ABC',
  `c` varchar(254) NOT NULL,
  `d` varchar(1024) DEFAULT 'DEF',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `t8`
--

DROP TABLE IF EXISTS `t8`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `t8` (
  `id` int(11) NOT NULL,
  `a` int(11) NOT NULL,
  `b` varchar(64) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `t9`
--

DROP TABLE IF EXISTS `t9`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `t9` (
  `id` int(11) NOT NULL,
  `a` int(11) DEFAULT NULL,
  `b` varchar(64) NOT NULL,
  `c` varchar(1024) DEFAULT 'DEF',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tb01`
--

DROP TABLE IF EXISTS `tb01`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tb01` (
  `id` int(11) NOT NULL,
  `a` bigint(20) NOT NULL,
  `b` varchar(64) NOT NULL,
  `c` varchar(1024) DEFAULT 'THIS_IS_DEFAULT_VALUE',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tb02`
--

DROP TABLE IF EXISTS `tb02`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tb02` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `c_utinyint` tinyint(11) unsigned NOT NULL,
  `c_tinyint` tinyint(11) NOT NULL,
  `c_usmallint` smallint(11) unsigned NOT NULL,
  `c_smallint` smallint(11) NOT NULL,
  `c_umediumint` mediumint(11) unsigned NOT NULL,
  `c_mediumint` mediumint(11) NOT NULL,
  `c_uint` int(11) unsigned NOT NULL,
  `c_int` int(11) NOT NULL,
  `c_ubigint` bigint(20) unsigned NOT NULL,
  `c_bigint` bigint(20) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=109 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tb03`
--

DROP TABLE IF EXISTS `tb03`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tb03` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `a` int(11) NOT NULL,
  `b` datetime NOT NULL,
  `c` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `d` time NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tb04`
--

DROP TABLE IF EXISTS `tb04`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tb04` (
  `id` int(11) NOT NULL,
  `a` varchar(32) NOT NULL,
  `b` varchar(64) NOT NULL,
  `c` varchar(254) NOT NULL,
  `d` varchar(255) NOT NULL,
  `e` varchar(256) NOT NULL,
  `f` varchar(512) NOT NULL,
  `g` varchar(16384) NOT NULL,
  `h` varchar(47474) NOT NULL,
  `i` char(1) NOT NULL,
  `j` char(32) NOT NULL,
  `k` char(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tb04utf8mb4`
--

DROP TABLE IF EXISTS `tb04utf8mb4`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tb04utf8mb4` (
  `id` int(11) NOT NULL,
  `a` varchar(32) NOT NULL,
  `b` varchar(64) NOT NULL,
  `c` varchar(254) NOT NULL,
  `d` varchar(255) NOT NULL,
  `e` varchar(256) NOT NULL,
  `f` varchar(512) NOT NULL,
  `g` varchar(768) NOT NULL,
  `h` varchar(13950) NOT NULL,
  `i` char(1) NOT NULL,
  `j` char(32) NOT NULL,
  `k` char(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tb05`
--

DROP TABLE IF EXISTS `tb05`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tb05` (
  `id` int(11) NOT NULL,
  `a` varchar(9) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tb06`
--

DROP TABLE IF EXISTS `tb06`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tb06` (
  `id` int(11) NOT NULL,
  `a` bigint(20) NOT NULL,
  `b` varchar(16380) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tb07`
--

DROP TABLE IF EXISTS `tb07`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
--CREATE TABLE `tb07` (
--  `id` int(11) NOT NULL,
--  `a` varbinary(32) NOT NULL,
--  `b` varbinary(255) NOT NULL,
--  `c` varbinary(512) NOT NULL,
--  `d` binary(32) NOT NULL,
--  `e` binary(255) NOT NULL,
--  PRIMARY KEY (`id`)
--) ENGINE=InnoDB DEFAULT CHARSET=latin1;
--/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tb08`
--

DROP TABLE IF EXISTS `tb08`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tb08` (
  `id` int(11) NOT NULL,
  `a` tinytext NOT NULL,
  `b` text NOT NULL,
  `c` mediumtext NOT NULL,
  `d` longtext NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tb09`
--

DROP TABLE IF EXISTS `tb09`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tb09` (
  `id` int(11) NOT NULL,
  `a` tinyblob NOT NULL,
  `b` blob NOT NULL,
  `c` mediumblob NOT NULL,
  `d` longblob NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tb10`
--

DROP TABLE IF EXISTS `tb10`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tb10` (
  `id` int(11) NOT NULL,
  `a` bigint(20) NOT NULL,
  `b` varchar(64) NOT NULL,
  `c` varchar(1024) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tb100`
--

DROP TABLE IF EXISTS `tb100`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tb100` (
  `id` int(11) NOT NULL,
  `a` bigint(20) NOT NULL,
  `b` varchar(64) NOT NULL,
  `c` varchar(1024) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tb101`
--

DROP TABLE IF EXISTS `tb101`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tb101` (
  `id` int(11) NOT NULL,
  `a` bigint(20) NOT NULL,
  `b` varchar(64) NOT NULL,
  `c` varchar(1024) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tb11`
--

DROP TABLE IF EXISTS `tb11`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tb11` (
  `id` int(11) NOT NULL,
  `a` bigint(20) NOT NULL,
  `b` varchar(64) NOT NULL,
  `c` varchar(1024) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tb12`
--

DROP TABLE IF EXISTS `tb12`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tb12` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `a` bigint(20) DEFAULT '999',
  `b` varchar(32) NOT NULL,
  `c` varchar(32) DEFAULT NULL,
  `d` varchar(32) DEFAULT 'sorry',
  `e` text NOT NULL,
  `f` varchar(32) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tb13`
--

DROP TABLE IF EXISTS `tb13`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tb13` (
  `id` int(11) NOT NULL,
  `a` bigint(20) NOT NULL,
  `b` varchar(64) NOT NULL,
  `c` varchar(1024) DEFAULT 'THIS_IS_DEFAULT_VALUE',
  PRIMARY KEY (`id`),
  UNIQUE KEY `b_a_idx` (`b`,`a`),
  KEY `a_idx` (`a`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tb14`
--

DROP TABLE IF EXISTS `tb14`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tb14` (
  `id` int(11) NOT NULL,
  `a1` varchar(10) NOT NULL,
  `a2` varchar(10) DEFAULT NULL,
  `a3` varchar(10) NOT NULL,
  `a4` varchar(10) DEFAULT NULL,
  `a5` varchar(10) NOT NULL,
  `a6` varchar(10) DEFAULT NULL,
  `a7` varchar(10) NOT NULL,
  `a8` varchar(10) DEFAULT NULL,
  `a9` varchar(10) NOT NULL,
  `a10` varchar(10) DEFAULT NULL,
  `a11` varchar(10) NOT NULL,
  `a12` varchar(10) DEFAULT NULL,
  `a13` varchar(10) NOT NULL,
  `a14` varchar(10) DEFAULT NULL,
  `a15` varchar(10) NOT NULL,
  `a16` varchar(10) DEFAULT NULL,
  `a17` varchar(10) NOT NULL,
  `a18` varchar(10) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tb15`
--

DROP TABLE IF EXISTS `tb15`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tb15` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `c_float` float NOT NULL,
  `c_double` double NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tb16`
--

DROP TABLE IF EXISTS `tb16`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tb16` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `a` year(4) NOT NULL,
  `b` date NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tb17`
--

DROP TABLE IF EXISTS `tb17`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tb17` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `a` int(11) NOT NULL,
  `b` datetime(3) NOT NULL,
  `c` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  `d` time(5) NOT NULL,
  `e` datetime NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tb18`
--

DROP TABLE IF EXISTS `tb18`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tb18` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `a` tinyint(1) NOT NULL,
  `b` tinyint(1) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tb19`
--

DROP TABLE IF EXISTS `tb19`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tb19` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `a` decimal(6,0) NOT NULL,
  `b` decimal(10,5) NOT NULL,
  `c` decimal(12,0) NOT NULL,
  `d` decimal(10,0) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tb20`
--

DROP TABLE IF EXISTS `tb20`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tb20` (
  `id` int(11) NOT NULL,
  `a` varchar(64) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
  `b` varchar(1024) CHARACTER SET utf8 NOT NULL,
  `c` varchar(256) CHARACTER SET gbk COLLATE gbk_bin DEFAULT '',
  `d` varchar(1024) CHARACTER SET gbk COLLATE gbk_bin DEFAULT '',
  `e` varchar(512) CHARACTER SET ujis NOT NULL,
  `f` varchar(1024) CHARACTER SET ujis DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tb21`
--

DROP TABLE IF EXISTS `tb21`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tb21` (
  `c1` varchar(30) NOT NULL,
  `c2` varchar(30) DEFAULT NULL,
  `c3` varchar(30) NOT NULL,
  `c4` varchar(30) DEFAULT NULL,
  `c5` varchar(30) NOT NULL,
  `c6` varchar(30) DEFAULT NULL,
  `c7` varchar(30) NOT NULL,
  `c8` varchar(30) DEFAULT NULL,
  `c9` varchar(30) NOT NULL,
  `c10` varchar(30) DEFAULT NULL,
  `c11` varchar(30) NOT NULL,
  `c12` varchar(30) DEFAULT NULL,
  PRIMARY KEY (`c5`,`c3`,`c9`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tb22`
--

DROP TABLE IF EXISTS `tb22`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tb22` (
  `a` int(11) NOT NULL,
  `b` varchar(30) NOT NULL,
  `c` varchar(20) NOT NULL,
  PRIMARY KEY (`b`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tb23`
--

DROP TABLE IF EXISTS `tb23`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tb23` (
  `c1` varchar(30) NOT NULL,
  `c2` varchar(30) DEFAULT NULL,
  `c3` varchar(30) NOT NULL,
  `c4` varchar(30) DEFAULT NULL,
  `c5` varchar(30) NOT NULL,
  `c6` varchar(30) DEFAULT NULL,
  `c7` varchar(30) NOT NULL,
  `c8` varchar(30) DEFAULT NULL,
  `c9` varchar(30) NOT NULL,
  `c10` varchar(30) DEFAULT NULL,
  `c11` varchar(30) NOT NULL,
  `c12` varchar(30) DEFAULT NULL,
  PRIMARY KEY (`c5`,`c3`,`c9`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tb24`
--

DROP TABLE IF EXISTS `tb24`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tb24` (
  `c1` varchar(100) NOT NULL,
  `c2` int(10) NOT NULL DEFAULT '0',
  `c3` varchar(100) NOT NULL,
  `c4` varchar(100) NOT NULL,
  `c5` varchar(100) NOT NULL,
  `c6` varchar(100) NOT NULL,
  `c7` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `c8` varchar(100) NOT NULL,
  PRIMARY KEY (`c5`,`c2`,`c6`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user`
--

DROP TABLE IF EXISTS `user`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `name` varchar(128) NOT NULL,
  `gender` tinyint(4) NOT NULL,
  `age` int(10) NOT NULL,
  `phone` varchar(64) NOT NULL,
  `address` varchar(1024) DEFAULT '',
  `profile` mediumtext NOT NULL,
  `birth_day` datetime NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='user';
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2020-05-01 23:24:47
