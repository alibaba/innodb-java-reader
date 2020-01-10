/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.page.fsphdr;

import com.alibaba.innodb.java.reader.page.AbstractPage;
import com.alibaba.innodb.java.reader.page.InnerPage;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;

/**
 * File space header
 *
 * @author xu.zx
 */
@Data
public class FspHdrXes extends AbstractPage {

  /**
   * 一个FSP_HDR（或者XDES） 最多维护256 extents (or 16,384 pages, 256 MB)，每个extent由一个XDES Entry维护。
   */
  public static final int MAX_SEGMENT_SIZE = 256;

  private FspHeader fspHeader;

  private List<Xdes> xdesList = new ArrayList<>(MAX_SEGMENT_SIZE);

  public FspHdrXes(InnerPage innerPage) {
    super(innerPage);
    this.fspHeader = FspHeader.fromSlice(sliceInput);
    for (int i = 0; i < MAX_SEGMENT_SIZE; i++) {
      Xdes curr = Xdes.fromSlice(sliceInput);
      // TODO 当state为空时退出，非官方做法
      if (curr.getState() == null) {
        break;
      }
      this.xdesList.add(curr);
    }
  }

}
