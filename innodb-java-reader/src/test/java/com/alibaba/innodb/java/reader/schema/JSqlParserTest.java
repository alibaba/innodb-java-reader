package com.alibaba.innodb.java.reader.schema;

import com.google.common.collect.ImmutableList;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 * @see <a href="jsqlparser unit test">https://github.com/JSQLParser/JSqlParser/blob/master/
 * src/test/java/net/sf/jsqlparser/statement/create/CreateTableTest.java</a>
 */
public class JSqlParserTest {

  @Test
  public void testParseSql() throws JSQLParserException {
    CreateTable stmt = (CreateTable) CCJSqlParserUtil.parse("CREATE TABLE  `dbtest`.`product001` (\n"
        + "  `id` bigint(20) NOT NULL COMMENT '主键',\n"
        + "  `user_id` bigint(20) unsigned DEFAULT '0' COMMENT '用户id,不再使用',\n"
        + "  `feed_id` int(11) DEFAULT '0' COMMENT 'feed id,不再使用',\n"
        + "  `outer_id` varchar(1024) NOT NULL COMMENT '用户定义的商品id' KEY,\n"
        + "  `feed_url_id` int(11) NOT NULL DEFAULT '0' COMMENT '抓取信息id',\n"
        + "  `name` varchar(200) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT '商品名称',\n"
        + "  `loc` varchar(1024) NOT NULL COLLATE utf8_bin COMMENT '商品详情页',\n"
        + "  `MWT` decimal(14,6) DEFAULT NULL,\n"
        + "  `content` text COLLATE utf8_general_ci COMMENT '商品属性',\n"
        + "  `content_hash` bigint(20) NOT NULL COMMENT 'content的hashcode',\n"
        + "  `version` bigint(20) NOT NULL COMMENT '版本',\n"
        + "  `name_hash` bigint(20) NOT NULL DEFAULT '0' COMMENT 'name hash',\n"
        + "  `deleted_state` int(11) NOT NULL DEFAULT '0' COMMENT '逻辑状态:0,有效 1，逻辑删除',\n"
        + "  PRIMARY KEY (`id`),\n"
        + "  FOREIGN KEY (user_id) REFERENCES ra_user(id),\n"
        + "  KEY `ddd` (`user_id`), \n"
        + "  UNIQUE key (feed_url_id) COMMENT 'feed',\n"
        + "  INDEX `eee` (`user_id`, `loc`) \n"
        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_bin COMMENT='商品信息'");
    System.out.println(stmt.toString());

    System.out.println(stmt.getTable().getName());
    System.out.println(stmt.getTable().getDatabase());
    System.out.println(stmt.getTable().getFullyQualifiedName());
    assertThat(stmt.getTable().getName(), is("`product001`"));

    System.out.println(stmt.getCreateOptionsStrings());
    assertThat(stmt.getCreateOptionsStrings(), nullValue());

    System.out.println(stmt.getTableOptionsStrings());
    assertThat(stmt.getTableOptionsStrings(),
        is(ImmutableList.of("ENGINE", "=", "InnoDB", "DEFAULT", "CHARSET", "=", "utf8",
            "COLLATE", "utf8_bin", "COMMENT", "=", "'商品信息'")));

    System.out.println(stmt.getIndexes());
    assertThat(stmt.getIndexes().toString(),
        is("[PRIMARY KEY (`id`) COMMENT 'feed', FOREIGN KEY (user_id) REFERENCES ra_user(id), "
            + "KEY `ddd` (`user_id`), UNIQUE key (feed_url_id) COMMENT 'feed', "
            + "INDEX `eee` (`user_id`, `loc`)]"));

    assertThat(stmt.getIndexes().size(), is(5));

    System.out.println(stmt.getIndexes().get(0));
    assertThat(stmt.getIndexes().get(0).getType(), is("PRIMARY KEY"));
    assertThat(stmt.getIndexes().get(0).getName(), nullValue());
    assertThat(stmt.getIndexes().get(0).getColumnsNames(), is(ImmutableList.of("`id`")));
    // bug?
    //assertThat(stmt.getIndexes().get(0).getIndexSpec(), is(ImmutableList.of()));
    assertThat(stmt.getIndexes().get(0).getUsing(), nullValue());

    System.out.println(stmt.getIndexes().get(1));
    assertThat(stmt.getIndexes().get(1).getType(), is("FOREIGN KEY"));
    assertThat(stmt.getIndexes().get(1).getName(), nullValue());
    assertThat(stmt.getIndexes().get(1).getColumnsNames(), is(ImmutableList.of("user_id")));
    assertThat(stmt.getIndexes().get(1).getIndexSpec(), nullValue());
    assertThat(stmt.getIndexes().get(1).getUsing(), nullValue());

    System.out.println(stmt.getIndexes().get(2));
    assertThat(stmt.getIndexes().get(2).getType(), is("KEY"));
    assertThat(stmt.getIndexes().get(2).getName(), is("`ddd`"));
    assertThat(stmt.getIndexes().get(2).getColumnsNames(), is(ImmutableList.of("`user_id`")));
    assertThat(stmt.getIndexes().get(2).getIndexSpec(), nullValue());
    assertThat(stmt.getIndexes().get(2).getUsing(), nullValue());

    System.out.println(stmt.getIndexes().get(3));
    assertThat(stmt.getIndexes().get(3).getType(), is("UNIQUE key"));
    assertThat(stmt.getIndexes().get(3).getName(), nullValue());
    assertThat(stmt.getIndexes().get(3).getColumnsNames(), is(ImmutableList.of("feed_url_id")));
    assertThat(stmt.getIndexes().get(3).getIndexSpec(), is(ImmutableList.of("COMMENT", "'feed'")));
    assertThat(stmt.getIndexes().get(3).getUsing(), nullValue());

    System.out.println(stmt.getIndexes().get(4));
    assertThat(stmt.getIndexes().get(4).getType(), is("INDEX"));
    assertThat(stmt.getIndexes().get(4).getName(), is("`eee`"));
    assertThat(stmt.getIndexes().get(4).getColumnsNames(), is(ImmutableList.of("`user_id`", "`loc`")));
    assertThat(stmt.getIndexes().get(4).getIndexSpec(), nullValue());
    assertThat(stmt.getIndexes().get(4).getUsing(), nullValue());

    for (ColumnDefinition columnDefinition : stmt.getColumnDefinitions()) {
      System.out.println(columnDefinition.toString());
    }

    ColumnDefinition columnDefinition = stmt.getColumnDefinitions().get(0);
    assertThat(columnDefinition.getColumnName(), is("`id`"));
    assertThat(columnDefinition.getColumnSpecStrings(), is(ImmutableList.of("NOT", "NULL", "COMMENT", "'主键'")));
    assertThat(columnDefinition.getColDataType().getDataType(), is("bigint"));
    assertThat(columnDefinition.getColDataType().getArgumentsStringList(), is(ImmutableList.of("20")));
    assertThat(columnDefinition.getColDataType().getCharacterSet(), nullValue());

    columnDefinition = stmt.getColumnDefinitions().get(1);
    assertThat(columnDefinition.getColumnName(), is("`user_id`"));
    assertThat(columnDefinition.getColumnSpecStrings(),
        is(ImmutableList.of("unsigned", "DEFAULT", "'0'", "COMMENT", "'用户id,不再使用'")));
    assertThat(columnDefinition.getColDataType().getDataType(), is("bigint"));
    assertThat(columnDefinition.getColDataType().getArgumentsStringList(), is(ImmutableList.of("20")));
    assertThat(columnDefinition.getColDataType().getCharacterSet(), nullValue());

    columnDefinition = stmt.getColumnDefinitions().get(3);
    assertThat(columnDefinition.getColumnName(), is("`outer_id`"));
    assertThat(columnDefinition.getColumnSpecStrings(),
        is(ImmutableList.of("NOT", "NULL", "COMMENT", "'用户定义的商品id'", "KEY")));
    assertThat(columnDefinition.getColDataType().getDataType(), is("varchar"));
    assertThat(columnDefinition.getColDataType().getArgumentsStringList(), is(ImmutableList.of("1024")));
    assertThat(columnDefinition.getColDataType().getCharacterSet(), nullValue());

    columnDefinition = stmt.getColumnDefinitions().get(5);
    assertThat(columnDefinition.getColumnName(), is("`name`"));
    assertThat(columnDefinition.getColumnSpecStrings(),
        is(ImmutableList.of("COLLATE", "utf8_bin", "NOT", "NULL", "COMMENT", "'商品名称'")));
    assertThat(columnDefinition.getColDataType().getDataType(), is("varchar"));
    assertThat(columnDefinition.getColDataType().getArgumentsStringList(), is(ImmutableList.of("200")));
    assertThat(columnDefinition.getColDataType().getCharacterSet(), is("utf8"));

    columnDefinition = stmt.getColumnDefinitions().get(6);
    assertThat(columnDefinition.getColumnName(), is("`loc`"));
    assertThat(columnDefinition.getColumnSpecStrings(),
        is(ImmutableList.of("NOT", "NULL", "COLLATE", "utf8_bin", "COMMENT", "'商品详情页'")));
    assertThat(columnDefinition.getColDataType().getDataType(), is("varchar"));
    assertThat(columnDefinition.getColDataType().getArgumentsStringList(), is(ImmutableList.of("1024")));
    assertThat(columnDefinition.getColDataType().getCharacterSet(), nullValue());

    columnDefinition = stmt.getColumnDefinitions().get(7);
    assertThat(columnDefinition.getColumnName(), is("`MWT`"));
    assertThat(columnDefinition.getColumnSpecStrings(), is(ImmutableList.of("DEFAULT", "NULL")));
    assertThat(columnDefinition.getColDataType().getDataType(), is("decimal"));
    assertThat(columnDefinition.getColDataType().getArgumentsStringList(), is(ImmutableList.of("14", "6")));
    assertThat(columnDefinition.getColDataType().getCharacterSet(), nullValue());
  }

  @Test
  public void testParseSqlNoBackTick() throws JSQLParserException {
    assertCanBeParsed("CREATE TABLE testtab (test varchar (255))");
  }

  @Test
  public void testParseSqlNoBackTickSemiColonEnding() throws JSQLParserException {
    assertCanBeParsed("CREATE TABLE testtab (test varchar (255));");
  }

  @Test(expected = JSQLParserException.class)
  public void testParseSqlNegate() throws JSQLParserException {
    // can not parse more than one sql
    assertCanBeParsed("CREATE TABLE testtab (test varchar (255));"
        + "CREATE TABLE testtab (test varchar (255));");
  }

  @Test
  public void testParseSqlDoubleQuotedColumn() throws JSQLParserException {
    assertCanBeParsed("CREATE TABLE testtab (\"test\" varchar (255))");
  }

  @Test
  public void testParseSqlDoubleQuotedTableName() throws JSQLParserException {
    assertCanBeParsed("CREATE TABLE \"testtab\" (test varchar (255))");
  }

  @Test
  public void testCreateTableForeignKey3() throws JSQLParserException {
    assertCanBeParsed("CREATE TABLE test (id INT UNSIGNED NOT NULL AUTO_INCREMENT, "
        + "string VARCHAR (20), "
        + "user_id INT UNSIGNED FOREIGN KEY REFERENCES ra_user(id), "
        + "PRIMARY KEY (id))");
  }

  @Test
  public void testPrimaryKeyAtColumn() throws JSQLParserException {
    assertCanBeParsed("CREATE TABLE Activities (_id INTEGER PRIMARY KEY AUTOINCREMENT,"
        + "uuid VARCHAR(255),user_id INTEGER,sound_id INTEGER,sound_type INTEGER,comment_id INTEGER,"
        + "type String,tags VARCHAR(255),created_at INTEGER,content_id INTEGER,sharing_note_text "
        + "VARCHAR(255),sharing_note_created_at INTEGER,UNIQUE (created_at, type, content_id, sound_id, "
        + "user_id))");
  }

  @Test
  public void testParseSql2() throws JSQLParserException {
    assertCanBeParsed("CREATE TABLE parent (\n"
        + "PARENT_ID int(11) NOT NULL AUTO_INCREMENT,\n"
        + "PCN varchar(100) NOT NULL,\n"
        + "IS_DELETED char(1) NOT NULL,\n"
        + "STRUCTURE_ID int(11) NOT NULL,\n"
        + "DIRTY_STATUS char(1) NOT NULL,\n"
        + "BIOLOGICAL char(1) NOT NULL,\n"
        + "STRUCTURE_TYPE int(11) NOT NULL,\n"
        + "CST_ORIGINAL varchar(1000) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,\n"
        + "MWT decimal(14,6) DEFAULT NULL,\n"
        + "RESTRICTED int(11) NOT NULL,\n"
        + "INIT_DATE datetime DEFAULT NULL,\n"
        + "MOD_DATE datetime DEFAULT NULL,\n"
        + "CREATED_BY varchar(255) NOT NULL,\n"
        + "MODIFIED_BY varchar(255) NOT NULL,\n"
        + "CHEMIST_ID varchar(255) NOT NULL,\n"
        + "UNKNOWN_ID int(11) DEFAULT NULL,\n"
        + "STEREOCHEMISTRY varchar(256) DEFAULT NULL,\n"
        + "GEOMETRIC_ISOMERISM varchar(256) DEFAULT NULL,\n"
        + "PRIMARY KEY (PARENT_ID),\n"
        + "UNIQUE KEY PARENT_PCN_IDX (PCN),\n"
        + "KEY PARENT_SID_IDX (STRUCTURE_ID),\n"
        + "KEY PARENT_DIRTY_IDX (DIRTY_STATUS)\n"
        + ") ENGINE=InnoDB AUTO_INCREMENT=2663 DEFAULT CHARSET=utf8");
  }

  @Test
  public void testParseSql3() throws JSQLParserException {
    assertCanBeParsed("CREATE TABLE IF NOT EXISTS \"TABLE_OK\" "
        + "(\"SOME_FIELD\" VARCHAR2 (256 BYTE))");
  }

  @Test
  public void testCreateTableAsSelect() throws JSQLParserException {
    assertCanBeParsed("CREATE TABLE public.sales1 AS (SELECT * FROM public.sales)");
  }

  // TODO JSQLParser does not support binary
  // this will be solved in 3.2-SNAPSHOT, see https://github.com/JSQLParser/JSqlParser
  @Test(expected = JSQLParserException.class)
  public void testCreateTableBinaryType() throws JSQLParserException {
    assertCanBeParsed("CREATE TABLE `tb07` (\n"
        + "      `id` int(11) NOT NULL,\n"
        + "  `a` varbinary(32) NOT NULL,\n"
        + "  `b` varbinary(255) NOT NULL,\n"
        + "  `c` varbinary(512) NOT NULL,\n"
        + "  `d` binary(32) NOT NULL,\n"
        + "  `e` binary(255) NOT NULL,\n"
        + "  PRIMARY KEY (`id`)\n"
        + ") ENGINE=InnoDB DEFAULT CHARSET=latin1;");
  }

  /*!40101 SET character_set_client = @saved_cs_client */;

  private void assertCanBeParsed(String sql) throws JSQLParserException {
    CreateTable stmt = (CreateTable) CCJSqlParserUtil.parse(sql);
    System.out.println(stmt.toString());
  }

}
