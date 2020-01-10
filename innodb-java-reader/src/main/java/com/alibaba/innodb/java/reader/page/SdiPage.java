/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.page;

/**
 * Since MySQL8.0, there is SDI, a.k.a Serialized Dictionary Information(SDI)
 *
 * @author xu.zx
 */
public class SdiPage extends AbstractPage {

  public SdiPage(InnerPage innerPage) {
    super(innerPage);
  }

}
