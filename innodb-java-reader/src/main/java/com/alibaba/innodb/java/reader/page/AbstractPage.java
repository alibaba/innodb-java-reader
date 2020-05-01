/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.page;

import com.alibaba.innodb.java.reader.util.SliceInput;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.codehaus.jackson.annotate.JsonIgnore;

import lombok.Data;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Page base class.
 *
 * @author xu.zx
 */
@Data
public abstract class AbstractPage {

  /**
   * inner page = FIL HEADER (38) + body + FIL TRAILER(8)
   */
  protected InnerPage innerPage;

  /**
   * page byte array.
   */
  @JsonIgnore
  protected SliceInput sliceInput;

  public AbstractPage(InnerPage innerPage) {
    this.innerPage = innerPage;
    this.sliceInput = innerPage.getSliceInput();
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
  }

  public long getPageNumber() {
    checkNotNull(innerPage);
    return innerPage.getPageNumber();
  }

  public FilHeader getFilHeader() {
    checkNotNull(innerPage);
    return innerPage.getFilHeader();
  }

  public PageType pageType() {
    checkNotNull(innerPage);
    return innerPage.pageType();
  }

  public SliceInput getSliceInput() {
    return sliceInput;
  }
}
