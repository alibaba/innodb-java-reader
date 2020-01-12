package com.alibaba.innodb.java.reader.util;

import com.alibaba.innodb.java.reader.page.PageType;

import org.junit.Test;

import static com.alibaba.innodb.java.reader.page.PageType.UNDO_LOG;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class EnumUtilTest {

  @Test
  public void testFind() {
    PageType pageType = EnumUtil.find(PageType.class, 2);
    assertThat(pageType, is(UNDO_LOG));

    assertThat(EnumUtil.find(PageType.class, MyEnum.ABC), nullValue());
    assertThat(EnumUtil.find(PageType.class, null), nullValue());
  }

  enum MyEnum {
    ABC, DEF
  }

}
