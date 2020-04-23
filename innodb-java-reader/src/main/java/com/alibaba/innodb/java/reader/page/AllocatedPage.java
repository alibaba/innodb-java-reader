/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.page;

/**
 * Allocated empty page which is unused.
 *
 * @author xu.zx
 */
public class AllocatedPage extends AbstractPage {

  public AllocatedPage(InnerPage innerPage) {
    super(innerPage);
  }

}
