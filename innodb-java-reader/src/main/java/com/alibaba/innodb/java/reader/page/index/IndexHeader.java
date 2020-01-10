/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.page.index;

import com.alibaba.innodb.java.reader.util.SliceInput;

import lombok.Data;

/**
 * IndexHeader
 * <p/>
 * 36 bytes header
 *
 * @author xu.zx
 */
@Data
public class IndexHeader {

  /**
   * The size of the page directory in “slots”, which are each 16-bit byte offsets.
   * Page directory中的slot个数
   */
  private int numOfDirSlots;

  /**
   * The byte offset of the “end” of the currently used space.
   * All space between the heap top and the end of the page directory is free space.
   * 指向当前Page内已使用的空间的末尾位置，即free space的开始位置
   */
  private int heapTopPosition;

  /**
   * records + infimum and supremum system records, and garbage (deleted) records.
   * Page内所有记录个数，包含用户记录，系统记录以及标记删除的记录，同时当第一个bit设置为1时，表示这个page内是以Compact格式存储的
   */
  private int numOfHeapRecords;

  /**
   * the high bit (0x8000) COMPACT and REDUNDANT.
   */
  private PageFormat format;

  /**
   * A pointer to the first entry in the list of garbage (deleted) records.
   * The list is singly-linked together using the “next record” pointers in each record header.
   */
  private int firstGarbageRecOffset;

  /**
   * 被删除的记录链表上占用的总的字节数，属于可回收的垃圾碎片空间
   */
  private int garbageSpace;

  /**
   * 指向最近一次插入的记录偏移量，主要用于优化顺序插入操作
   */
  private int lastInsertPos;

  /**
   * LEFT, RIGHT, and NO_DIRECTION. sequential inserts (to the left [lower values] or right [higher values]) or random inserts.
   * 用于指示当前记录的插入顺序以及是否正在进行顺序插入，每次插入时，PAGE_LAST_INSERT会和当前记录进行比较，以确认插入方向，据此进行插入优化
   */
  private PageDirection pageDirection;

  /**
   * 当前以相同方向的顺序插入记录个数
   */
  private int numOfInsertsInPageDirection;

  /**
   * non-deleted user records in the page.
   * Page上有效的未被标记删除的用户记录个数
   */
  private int numOfRecs;

  /**
   * 最近一次修改该page记录的事务ID，主要用于辅助判断二级索引记录的可见性。
   */
  private long maxTrxId;

  /**
   * Leaf pages are at level 0, and the level increments up the B+tree from there.
   * In a typical 3-level B+tree, the root will be level 2,
   * some number of internal non-leaf pages will be level 1, and leaf pages will be level 0.
   * 该Page所在的btree level，根节点的level最大，叶子节点的level为0
   */
  private int pageLevel;

  /**
   * 该Page归属的索引ID
   */
  private long indexId;

  public static IndexHeader fromSlice(SliceInput input) {
    IndexHeader indexHeader = new IndexHeader();
    indexHeader.setNumOfDirSlots(input.readUnsignedShort());
    indexHeader.setHeapTopPosition(input.readUnsignedShort());

    int flag = input.readShort();
    // not correct...
    // index[:n_heap] = index[:n_heap_format] & (2**15-1)
    //      index[:format] = (index[:n_heap_format] & 1<<15) == 0 ?
    //        :redundant : :compact
    indexHeader.setNumOfHeapRecords(flag & (0x7fff));
    indexHeader.setFormat(PageFormat.parse((flag & 0x8000) >> 15));

    indexHeader.setFirstGarbageRecOffset(input.readUnsignedShort());
    indexHeader.setGarbageSpace(input.readUnsignedShort());
    indexHeader.setLastInsertPos(input.readUnsignedShort());
    indexHeader.setPageDirection(PageDirection.parse(input.readUnsignedShort()));
    indexHeader.setNumOfInsertsInPageDirection(input.readUnsignedShort());
    indexHeader.setNumOfRecs(input.readUnsignedShort());
    indexHeader.setMaxTrxId(input.readLong());
    indexHeader.setPageLevel(input.readUnsignedShort());
    indexHeader.setIndexId(input.readLong());
    return indexHeader;
  }

}
