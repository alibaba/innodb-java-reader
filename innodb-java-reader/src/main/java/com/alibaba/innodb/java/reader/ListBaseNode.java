/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader;

import com.alibaba.innodb.java.reader.util.SliceInput;

import lombok.Data;

import static com.alibaba.innodb.java.reader.util.Utils.maybeUndefined;

/**
 * ListBaseNode
 *
 * @author xu.zx
 */
@Data
public class ListBaseNode {

  /**
   * 存储链表的长度
   */
  private long length;

  /**
   * 指向链表的第一个节点
   */
  private Long firstPageNumber;
  private int firstOffset;

  /**
   * 指向链表的最后一个节点
   */
  private Long lastPageNumber;
  private int lastOffset;

  public static ListBaseNode fromSlice(SliceInput input) {
    ListBaseNode listBaseNode = new ListBaseNode();
    listBaseNode.setLength(input.readUnsignedInt());
    listBaseNode.setFirstPageNumber(maybeUndefined(input.readUnsignedInt()));
    listBaseNode.setFirstOffset(input.readUnsignedShort());
    listBaseNode.setLastPageNumber(maybeUndefined(input.readUnsignedInt()));
    listBaseNode.setLastOffset(input.readUnsignedShort());
    return listBaseNode;
  }

}
