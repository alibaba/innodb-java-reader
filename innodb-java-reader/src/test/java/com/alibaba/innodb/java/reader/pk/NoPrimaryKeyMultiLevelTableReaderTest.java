package com.alibaba.innodb.java.reader.pk;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class NoPrimaryKeyMultiLevelTableReaderTest extends AbstractTest {

  String sql = "CREATE TABLE `tb29` (\n"
      + "  `id` int(11) NOT NULL,\n"
      + "  `a` bigint(20) NOT NULL,\n"
      + "  `b` varchar(64) NOT NULL\n"
      + ") ENGINE=InnoDB DEFAULT CHARSET=latin1";

  @Test
  public void testNoPkMysql56() {
    assertTestOf(this)
        .withMysql56()
        .withSql(sql)
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testNoPkQueryAllIteratorMysql56() {
    assertTestOf(this)
        .withMysql56()
        .withSql(sql)
        .checkQueryAllIterator(expectedIterator(true));
  }

  public Consumer<List<GenericRecord>> expected() {
    return recordList -> {

      assertThat(recordList.size(), is(2503));

      // total [1, 5000] rows
      // mysql> delete from tb29 where id < 1000;
      //mysql> delete from tb29 where id > 2000 and id < 2200;
      //mysql> delete from tb29 where id > 3000 and id < 3800;
      //mysql> delete from tb29 where id > 4500;
      int index = 0;
      for (int i = 1000; i <= 2000; i++) {
        GenericRecord r = recordList.get(index++);
        verifyRecord(i, r);
      }
      for (int i = 2200; i <= 3000; i++) {
        GenericRecord r = recordList.get(index++);
        verifyRecord(i, r);
      }
      for (int i = 3800; i <= 4500; i++) {
        GenericRecord r = recordList.get(index++);
        verifyRecord(i, r);
      }
    };
  }

  private void verifyRecord(int i, GenericRecord r) {
    Object[] v = r.getValues();
    // System.out.println(Arrays.toString(v));
    assertThat(r.getPrimaryKey().isEmpty(), is(true));
    assertThat(r.get("id"), is(i));
    assertThat(r.get("a"), is(i * 2L));
    assertThat(r.get("b"), is(StringUtils.repeat(String.valueOf((char) (97 + i % 26)), 16)));
  }

  public Consumer<Iterator<GenericRecord>> expectedIterator(boolean asc) {
    return iterator -> {

      assertThat(iterator.hasNext(), is(true));
      List<GenericRecord> list = new ArrayList<>();
      while (iterator.hasNext()) {
        list.add(iterator.next());
      }
      if (!asc) {
        Collections.reverse(list);
      }
      expected().accept(list);
    };
  }
}
