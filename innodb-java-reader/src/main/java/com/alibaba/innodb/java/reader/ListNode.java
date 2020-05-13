/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader;

import com.alibaba.innodb.java.reader.util.SliceInput;

import lombok.Data;

import static com.alibaba.innodb.java.reader.util.Utils.maybeUndefined;

/**
 * ListNode.
 *
 * @author xu.zx
 */
@Data
public class ListNode {

  /**
   * 指向当前节点的前一个节点
   */
  private Long prevPageNumber;
  private int prevOffset;

  /**
   * 指向当前节点的下一个节点
   */
  private Long nextPageNumber;
  private int nextOffset;

  public static ListNode fromSlice(SliceInput input) {
    ListNode listNode = new ListNode();
    listNode.setPrevPageNumber(maybeUndefined(input.readUnsignedInt()));
    listNode.setPrevOffset(input.readShort());
    listNode.setNextPageNumber(maybeUndefined(input.readUnsignedInt()));
    listNode.setNextOffset(input.readShort());
    return listNode;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("[").append(prevPageNumber);
    sb.append(",").append(prevOffset);
    sb.append("],[").append(nextPageNumber);
    sb.append(",").append(nextOffset);
    sb.append(']');
    return sb.toString();
  }

}
