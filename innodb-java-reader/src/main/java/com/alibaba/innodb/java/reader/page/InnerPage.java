/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.page;

import com.alibaba.innodb.java.reader.config.ReaderSystemProperty;
import com.alibaba.innodb.java.reader.util.Checksum;
import com.alibaba.innodb.java.reader.util.Slice;
import com.alibaba.innodb.java.reader.util.SliceInput;
import com.alibaba.innodb.java.reader.util.Ut0Crc32;

import org.codehaus.jackson.annotate.JsonIgnore;

import lombok.Getter;

import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_FIL_HEADER;
import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_FIL_TRAILER;
import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_PAGE;
import static com.google.common.base.Preconditions.checkState;

/**
 * page = FIL HEADER (38) + body + FIL TRAILER(8).
 *
 * @author xu.zx
 */
public class InnerPage {

  @Getter
  protected FilHeader filHeader;

  @Getter
  protected FilTrailer filTrailer;

  /**
   * page 0 is located at file offset 0, page 1 at file offset 16384.
   */
  @Getter
  protected long pageNumber;

  /**
   * 16k page byte buffer with fil header and fil trailer.
   */
  @JsonIgnore
  @Getter
  protected SliceInput sliceInput;

  public InnerPage(long pageNumber, Slice slice) {
    this.pageNumber = pageNumber;
    this.sliceInput = slice.input();
    this.filHeader = FilHeader.fromSlice(sliceInput);
    sliceInput.setPosition(SIZE_OF_PAGE - SIZE_OF_FIL_TRAILER);
    this.filTrailer = FilTrailer.fromSlice(sliceInput);
    checkState(this.filHeader.getLow32Lsn() == this.filTrailer.getLow32lsn(), "low32 lsn not match");

    if (ReaderSystemProperty.ENABLE_PAGE_CHECKSUM_CHECK.value()
        && this.filHeader.getPageType() == PageType.INDEX) {
      validateChecksum(slice, filHeader);
    }

    // reset to end of fil header
    this.sliceInput.setPosition(SIZE_OF_FIL_HEADER);
  }

  /**
   * By default, new way to do checksum on pages for MySQL 5.7 or later will be enabled.
   * But if checksum does not match, then we have to do an old way checksum validation for MySQL 5.6.
   * If both fail, then checksum fails.
   * <p>
   * Refer to https://dev.mysql.com/doc/refman/5.7/en/innodb-parameters.html#sysvar_innodb_checksum_algorithm
   * <p>
   * <code>innodb_checksum_algorithm</code> is a configuration to specify how to generate and verify
   * the checksum stored in the disk blocks of InnoDB tablespaces.
   */
  private void validateChecksum(Slice slice, FilHeader filHeader) {
    boolean checksumEquals;
    // buf_calc_page_crc32
    long v1 = Ut0Crc32.crc32(slice.getBytes(), 4, 22);
    long v2 = Ut0Crc32.crc32(slice.getBytes(), 38, SIZE_OF_PAGE - 8 - 38);
    long checksum = (v1 ^ v2) & 0xFFFFFFFFL;
    checksumEquals = checksum == this.filHeader.getChecksum();
    if (!checksumEquals && ReaderSystemProperty.ENABLE_INNODB_PAGE_CHECKSUM_ALGORITHM.value()) {
      // buf_calc_page_new_checksum
      v1 = Checksum.getValue(slice.getBytes(), 4, 26);
      v2 = Checksum.getValue(slice.getBytes(), 38, SIZE_OF_PAGE - 8);
      checksum = (v1 + v2) & 0xFFFFFFFFL;
      checksumEquals = checksum == this.filHeader.getChecksum();
    }
    checkState(checksumEquals, "page checksum " + checksum
        + " not match expected " + this.filHeader.getChecksum() + " " + filHeader);
  }

  public PageType pageType() {
    return filHeader.getPageType();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("Page#");
    sb.append(pageNumber).append("(");
    sb.append("header=").append(filHeader);
    sb.append(')');
    return sb.toString();
  }

}
