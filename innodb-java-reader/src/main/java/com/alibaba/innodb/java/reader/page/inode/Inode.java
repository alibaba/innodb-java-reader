/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.page.inode;

import com.alibaba.innodb.java.reader.page.AbstractPage;
import com.alibaba.innodb.java.reader.page.InnerPage;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * The third page in each space (page 2) will be an INODE page, which is used to store lists related to file
 * segments (groupings of extents plus an array of singly-allocated “fragment” pages). Each INODE page can store
 * 85 INODE entries, and each index requires two INODE entries.
 * <p>
 * 数据文件的第3个page的类型为FIL_PAGE_INODE，用于管理数据文件中的segement，每个索引占用2个segment，分别用于管理叶子节点和非叶子节点。
 * 每个inode页可以存储FSP_SEG_INODES_PER_PAGE（默认为85）个记录。
 *
 * @author xu.zx
 */
@Slf4j
@Data
public class Inode extends AbstractPage {

  public static final int MAX_INODE_ENTRY_SIZE = 85;

  private List<InodeEntry> inodeEntryList = new ArrayList<>(MAX_INODE_ENTRY_SIZE);

  public Inode(InnerPage innerPage) {
    super(innerPage);
    // 12 BYTES. INODE页的链表节点，记录前后Inode Page的位置，BaseNode记录在头Page的FSP_SEG_INODES_FULL或者FSP_SEG_INODES_FREE字段。
    sliceInput.skipBytes(12);
    // Inode记录
    for (int i = 0; i < MAX_INODE_ENTRY_SIZE; i++) {
      InodeEntry inodeEntry = InodeEntry.fromSlice(sliceInput);
      if (inodeEntry.getMagicNumber() == 0 || inodeEntry.getSegmentId() == 0) {
        break;
      }
      inodeEntryList.add(inodeEntry);
    }
  }

  @Override
  public String toString() {
    for (InodeEntry inodeEntry : inodeEntryList) {
      log.debug("{}", inodeEntry);
    }
    return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
  }
}
