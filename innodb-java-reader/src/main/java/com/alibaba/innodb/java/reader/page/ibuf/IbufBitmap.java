/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.page.ibuf;

import com.alibaba.innodb.java.reader.page.AbstractPage;
import com.alibaba.innodb.java.reader.page.InnerPage;

/**
 * IBUF BITMAP PAGE.
 *
 * @author xu.zx
 */
public class IbufBitmap extends AbstractPage {

  public IbufBitmap(InnerPage innerPage) {
    super(innerPage);
  }

}
