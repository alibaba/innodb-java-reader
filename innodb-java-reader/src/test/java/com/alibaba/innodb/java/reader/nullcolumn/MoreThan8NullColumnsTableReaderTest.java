package com.alibaba.innodb.java.reader.nullcolumn;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class MoreThan8NullColumnsTableReaderTest extends AbstractTest {

  String sql = "CREATE TABLE `tb14` (\n"
      + "  `id` int(11) NOT NULL,\n"
      + "  `a1` varchar(10) NOT NULL,\n"
      + "  `a2` varchar(10) DEFAULT NULL,\n"
      + "  `a3` varchar(10) NOT NULL,\n"
      + "  `a4` varchar(10) DEFAULT NULL,\n"
      + "  `a5` varchar(10) NOT NULL,\n"
      + "  `a6` varchar(10) DEFAULT NULL,\n"
      + "  `a7` varchar(10) NOT NULL,\n"
      + "  `a8` varchar(10) DEFAULT NULL,\n"
      + "  `a9` varchar(10) NOT NULL,\n"
      + "  `a10` varchar(10) DEFAULT NULL,\n"
      + "  `a11` varchar(10) NOT NULL,\n"
      + "  `a12` varchar(10) DEFAULT NULL,\n"
      + "  `a13` varchar(10) NOT NULL,\n"
      + "  `a14` varchar(10) DEFAULT NULL,\n"
      + "  `a15` varchar(10) NOT NULL,\n"
      + "  `a16` varchar(10) DEFAULT NULL,\n"
      + "  `a17` varchar(10) NOT NULL,\n"
      + "  `a18` varchar(10) DEFAULT NULL,\n"
      + "  PRIMARY KEY (`id`)\n"
      + ") ENGINE=InnoDB DEFAULT CHARSET=latin1";

  @Test
  public void testMoreThan8NullColumnsMysql56() {
    assertTestOf(this)
        .withMysql56()
        .withSql(sql)
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testMoreThan8NullColumnsMysql57() {
    assertTestOf(this)
        .withMysql57()
        .withSql(sql)
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testMoreThan8NullColumnsMysql80() {
    assertTestOf(this)
        .withMysql80()
        .withSql(sql)
        .checkAllRecordsIs(expected());
  }

  public Consumer<List<GenericRecord>> expected() {
    return recordList -> {

      GenericRecord r1 = recordList.get(0);
      System.out.println(Arrays.asList(r1.getValues()));
      assertThat(r1.get("id"), is(1));
      assertThat(r1.get("a1"), is("a1"));
      assertThat(r1.get("a2"), nullValue());
      assertThat(r1.get("a3"), is("a3"));
      assertThat(r1.get("a4"), nullValue());
    };
  }

}
