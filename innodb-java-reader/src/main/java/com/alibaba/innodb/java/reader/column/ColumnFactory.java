/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.column;

import com.alibaba.innodb.java.reader.exception.ParseException;
import com.alibaba.innodb.java.reader.util.SliceInput;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_BYTE;
import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_INT;
import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_LONG;
import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_MEDIUMINT;
import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_SHORT;

/**
 * Column parser factory. Please refer to <code>DataTypeHandler.cc</code>
 *
 * @author xu.zx
 */
public class ColumnFactory {

  private static final Map<String, ColumnParser<?>> TYPE_TO_COLUMN_PARSER_MAP = new HashMap<>();

  /**
   * The max ulonglong - 0x ff ff ff ff ff ff ff ff
   */
  public static final BigInteger BIGINT_MAX_VALUE = new BigInteger("18446744073709551615");

  private ColumnFactory() {
  }

  public static ColumnParser<?> getColumnParser(String columnType) {
    ColumnParser<?> result = TYPE_TO_COLUMN_PARSER_MAP.get(columnType);
    if (result == null) {
      throw new ParseException("Column parser not supported for type " + columnType);
    }
    return result;
  }

  /**
   * This works the same as <code>RowSetMetaDataImpl#getColumnClassName(int columnIndex)</code> in MySQL JDBC driver
   *
   * @param columnType column type
   * @return column java class
   */
  public static Class<?> getColumnJavaType(String columnType) {
    ColumnParser<?> result = TYPE_TO_COLUMN_PARSER_MAP.get(columnType);
    if (result == null) {
      throw new ParseException("Column parser not supported for type " + columnType);
    }
    return result.typeClass();
  }

  private static final ColumnParser<Integer> UNSIGNED_TINYINT = new AbstractColumnParser<Integer>() {

    @Override
    public Integer readFrom(SliceInput input) {
      return input.readUnsignedByte();
    }

    @Override
    public Class<?> typeClass() {
      return Integer.class;
    }
  };

  private static final ColumnParser<Integer> TINYINT = new AbstractColumnParser<Integer>() {

    @Override
    public Integer readFrom(SliceInput input) {
      return input.readByte() ^ (-1 << (SIZE_OF_BYTE * 8 - 1));
    }

    @Override
    public Class<?> typeClass() {
      return Integer.class;
    }
  };

  private static final ColumnParser<Integer> UNSIGNED_SMALLINT = new AbstractColumnParser<Integer>() {

    @Override
    public Integer readFrom(SliceInput input) {
      return input.readUnsignedShort();
    }

    @Override
    public Class<?> typeClass() {
      return Integer.class;
    }
  };

  private static final ColumnParser<Integer> SMALLINT = new AbstractColumnParser<Integer>() {

    @Override
    public Integer readFrom(SliceInput input) {
      return input.readShort() ^ (-1 << (SIZE_OF_SHORT * 8 - 1));
    }

    @Override
    public Class<?> typeClass() {
      return Integer.class;
    }
  };

  private static final ColumnParser<Integer> UNSIGNED_MEDIUMINT = new AbstractColumnParser<Integer>() {

    @Override
    public Integer readFrom(SliceInput input) {
      return input.readUnsigned3BytesInt();
    }

    @Override
    public Class<?> typeClass() {
      return Integer.class;
    }
  };

  /**
   * <pre>
   * static inline int32 sint3korr(const uchar *A)
   * {
   *   return
   *     ((int32) (((A[2]) & 128) ?
   *               (((uint32) 255L << 24) |
   *                (((uint32) A[2]) << 16) |
   *                (((uint32) A[1]) << 8) |
   *                ((uint32) A[0])) :
   *               (((uint32) A[2]) << 16) |
   *               (((uint32) A[1]) << 8) |
   *               ((uint32) A[0])))
   *     ;
   * }
   * </pre>
   *
   * @see my_byteorder.h
   */
  private static final ColumnParser<Integer> MEDIUMINT = new AbstractColumnParser<Integer>() {

    private static final int CONST_0X800000 = 0x800000;

    @Override
    public Integer readFrom(SliceInput input) {
      int v = (input.read3BytesInt() ^ (-1 << (SIZE_OF_MEDIUMINT * 8 - 1))) & 0xffffff;
      if ((v & CONST_0X800000) != 0) {
        v = (int) ((255L << 24) | v);
      }
      return v;
    }

    @Override
    public Class<?> typeClass() {
      return Integer.class;
    }
  };

  private static final ColumnParser<Long> UNSIGNED_INT = new AbstractColumnParser<Long>() {

    @Override
    public Long readFrom(SliceInput input) {
      return input.readUnsignedInt();
    }

    @Override
    public Class<?> typeClass() {
      return Integer.class;
    }
  };

  private static final ColumnParser<Integer> INT = new AbstractColumnParser<Integer>() {

    @Override
    public Integer readFrom(SliceInput input) {
      return input.readInt() ^ (-1 << (SIZE_OF_INT * 8 - 1));
    }

    @Override
    public Class<?> typeClass() {
      return Integer.class;
    }
  };

  private static final ColumnParser<BigInteger> UNSIGNED_BIGINT = new AbstractColumnParser<BigInteger>() {

    @Override
    public BigInteger readFrom(SliceInput input) {
      long long64 = input.readLong();
      return (long64 >= 0) ? BigInteger.valueOf(long64) : BIGINT_MAX_VALUE.add(BigInteger.valueOf(1 + long64));
    }

    @Override
    public Class<?> typeClass() {
      return BigInteger.class;
    }
  };

  private static final ColumnParser<Long> BIGINT = new AbstractColumnParser<Long>() {

    @Override
    public Long readFrom(SliceInput input) {
      return input.readLong() ^ (-1L << (SIZE_OF_LONG * 8 - 1));
    }

    @Override
    public Class<?> typeClass() {
      return Long.class;
    }
  };

  /**
   * https://dev.mysql.com/doc/refman/5.7/en/innodb-row-format.html
   * <p/>
   * a CHAR(255) column can exceed 768 bytes if the maximum byte length of the character set is greater than 3, as it is with utf8mb4.
   * <p/>
   * 在单字节字符集下，如果存储的是非 NULL 值时，会占满指定的空间。比如 CHAR(10)，存储除 NULL 之外的其它值时，一定会占 10 bytes 空间，不足用 \x20 填充。
   * 在多字节字符集下（测试用的是 utf8mb4），如果存储的是非 NULL 值时
   * 至少占用与指定值相等的字节空间，比如 CHAR(10)，至少会占用 10 bytes 空间，如果存储的内容超过了 10 bytes （对于 ubf8mb4 编码来说，CHAR(10) 最多能存储 40 bytes 内容），那么只占用实际占用的字节数。
   *
   * 总结一下就是，对于 CHAR(n)，如果占用的空间字节数少于 n，会用 \x20 填充，大于等于 n 的话，不需再填充。
   * 会用额外字节来记录 CHAR 类型字段实际占用的字节数，这与 VARCHAR 类似。
   */
  private static final ColumnParser<String> CHAR = new AbstractColumnParser<String>() {

    @Override
    public String readFrom(SliceInput input, int len, String charset) {
      return input.readString(len, charset);
    }

    @Override
    public Class<?> typeClass() {
      return String.class;
    }
  };

  private static final ColumnParser<String> VARCHAR = new AbstractColumnParser<String>() {

    @Override
    public String readFrom(SliceInput input, int len, String charset) {
      return input.readString(len, charset);
    }

    @Override
    public Class<?> typeClass() {
      return String.class;
    }
  };

  private static final ColumnParser<byte[]> BINARY = new AbstractColumnParser<byte[]>() {

    // ignore charset
    @Override
    public byte[] readFrom(SliceInput input, int len, String charset) {
      return input.readByteArray(len);
    }

    @Override
    public Class<?> typeClass() {
      return byte[].class;
    }
  };

  private static final ColumnParser<byte[]> VARBINARY = new AbstractColumnParser<byte[]>() {

    // ignore charset
    @Override
    public byte[] readFrom(SliceInput input, int len, String charset) {
      return input.readByteArray(len);
    }

    @Override
    public Class<?> typeClass() {
      return byte[].class;
    }
  };

  private static final ColumnParser<String> TEXT = new AbstractColumnParser<String>() {

    @Override
    public String readFrom(SliceInput input, int len, String charset) {
      return input.readString(len, charset);
    }

    @Override
    public Class<?> typeClass() {
      return String.class;
    }
  };

  private static final ColumnParser<byte[]> BLOB = new AbstractColumnParser<byte[]>() {

    // ignore charset
    @Override
    public byte[] readFrom(SliceInput input, int len, String charset) {
      return input.readByteArray(len);
    }

    @Override
    public Class<?> typeClass() {
      return byte[].class;
    }
  };

  /**
   * //TODO 暂不支持Fraction
   *
   * As of MySQL 5.6.4 the TIME, TIMESTAMP, and DATETIME types can have a fractional seconds part.
   * Storage for these types is big endian (for memcmp() compatibility purposes), with the nonfractional
   * part followed by the fractional part. (Storage and encoding for the YEAR and DATE types remains unchanged.)
   * <p/>
   * https://dev.mysql.com/doc/internals/en/date-and-time-data-type-representation.html
   * <pre>
   * 1 bit  sign           (1= non-negative, 0= negative)
   * 17 bits year*13+month  (year 0-9999, month 0-12)
   * 5 bits day            (0-31)
   * 5 bits hour           (0-23)
   * 6 bits minute         (0-59)
   * 6 bits second         (0-59)
   * ---------------------------
   * 40 bits = 5 bytes
   * </pre>
   * DATETIME2
   *
   * int dth_decode_datetime2(const NdbDictionary::Column*col,
   * char *&str, const void *buf) {
   * time_helper tm = {0, 0, 0, 0, 0, 0, 0, false};
   * const char *buffer = (const char *)buf;
   *
   * //Read the datetime from the buffer
   * Uint64 packedValue = unpack_bigendian(buffer, 5);
   *
   * //Factor it out
   * tm.second = (packedValue & 0x3F);
   * packedValue >>= 6;
   * tm.minute = (packedValue & 0x3F);
   * packedValue >>= 6;
   * tm.hour = (packedValue & 0x1F);
   * packedValue >>= 5;
   * tm.day = (packedValue & 0x1F);
   * packedValue >>= 5;
   * int yrMo = (packedValue & 0x01FFFF);
   * tm.year = yrMo / 13;
   * tm.month = yrMo % 13;
   *
   * const char *fspbuf = buffer + 5;
   * FractionPrinter fptr (col, readFraction(col, fspbuf));
   *
   * //Stringify it
   * return sprintf(str, "%04d-%02d-%02d %02d:%02d:%02d%s",
   * tm.year,tm.month, tm.day, tm.hour, tm.minute, tm.second,
   * fptr.print());
   * }
   */
  private static final ColumnParser<Date> DATETIME2 = new AbstractColumnParser<Date>() {

    @Override
    public Date readFrom(SliceInput input) {
      byte[] data = input.readByteArray(5);
      long packedValue = ((long) data[4] & 0xff)
          | ((long) data[3] & 0xff) << 8
          | ((long) data[2] & 0xff) << 16
          | ((long) data[1] & 0xff) << 24
          | ((long) data[0] & 0xff) << 32;
      int sec = (int) (packedValue & 0x3fL);
      packedValue >>= 6;
      int min = (int) (packedValue & 0x3fL);
      packedValue >>= 6;
      int hour = (int) (packedValue & 0x1fL);
      packedValue >>= 5;
      int day = (int) (packedValue & 0x1fL);
      packedValue >>= 5;
      int yrMo = (int) (packedValue & 0x01ffffL);
      int month = yrMo % 13;
      int year = yrMo / 13;
      Calendar calendar = Calendar.getInstance();
      calendar.clear();
      calendar.set(Calendar.YEAR, year);
      calendar.set(Calendar.MONTH, month - 1);
      calendar.set(Calendar.DATE, day);
      calendar.set(Calendar.HOUR, hour);
      calendar.set(Calendar.MINUTE, min);
      calendar.set(Calendar.SECOND, sec);
      Date date = calendar.getTime();
      return date;
    }

    @Override
    public Class<?> typeClass() {
      return Date.class;
    }
  };

  private static final ColumnParser<Timestamp> TIMESTAMP = new AbstractColumnParser<Timestamp>() {

    @Override
    public Timestamp readFrom(SliceInput input) {
      long timestamp = input.readUnsignedInt();
      return new Timestamp(timestamp * 1000L);
    }

    @Override
    public Class<?> typeClass() {
      return Timestamp.class;
    }
  };

  private static final ColumnParser<Short> YEAR = new AbstractColumnParser<Short>() {

    @Override
    public Short readFrom(SliceInput input) {
      return (short) (input.readUnsignedByte() + 1900);
    }

    @Override
    public Class<?> typeClass() {
      return Short.class;
    }
  };

  private static final ColumnParser<Date> DATE = new AbstractColumnParser<Date>() {

    @Override
    public Date readFrom(SliceInput input) {
      int encodedDate = (input.read3BytesInt() ^ (-1 << (SIZE_OF_MEDIUMINT * 8 - 1))) & 0xffffff;
      int day = (encodedDate & 31);
      int month = (encodedDate >> 5 & 15);
      int year = (encodedDate >> 9);
      Calendar calendar = Calendar.getInstance();
      calendar.clear();
      calendar.set(Calendar.YEAR, year);
      calendar.set(Calendar.MONTH, month - 1);
      calendar.set(Calendar.DATE, day);
      return calendar.getTime();
    }

    @Override
    public Class<?> typeClass() {
      return Date.class;
    }
  };

  private static final ColumnParser<Float> FLOAT = new AbstractColumnParser<Float>() {

    @Override
    public Float readFrom(SliceInput input) {
      return input.readFloat();
    }

    @Override
    public Class<?> typeClass() {
      return Float.class;
    }
  };

  private static final ColumnParser<Double> DOUBLE = new AbstractColumnParser<Double>() {

    @Override
    public Double readFrom(SliceInput input) {
      return input.readDouble();
    }

    @Override
    public Class<?> typeClass() {
      return Double.class;
    }
  };

  static {
    TYPE_TO_COLUMN_PARSER_MAP.put(ColumnType.TINYINT, TINYINT);
    TYPE_TO_COLUMN_PARSER_MAP.put(ColumnType.UNSIGNED_TINYINT, UNSIGNED_TINYINT);
    TYPE_TO_COLUMN_PARSER_MAP.put(ColumnType.SMALLINT, SMALLINT);
    TYPE_TO_COLUMN_PARSER_MAP.put(ColumnType.UNSIGNED_SMALLINT, UNSIGNED_SMALLINT);
    TYPE_TO_COLUMN_PARSER_MAP.put(ColumnType.MEDIUMINT, MEDIUMINT);
    TYPE_TO_COLUMN_PARSER_MAP.put(ColumnType.UNSIGNED_MEDIUMINT, UNSIGNED_MEDIUMINT);
    TYPE_TO_COLUMN_PARSER_MAP.put(ColumnType.INT, INT);
    TYPE_TO_COLUMN_PARSER_MAP.put(ColumnType.UNSIGNED_INT, UNSIGNED_INT);
    TYPE_TO_COLUMN_PARSER_MAP.put(ColumnType.BIGINT, BIGINT);
    TYPE_TO_COLUMN_PARSER_MAP.put(ColumnType.UNSIGNED_BIGINT, UNSIGNED_BIGINT);
    TYPE_TO_COLUMN_PARSER_MAP.put(ColumnType.CHAR, CHAR);
    TYPE_TO_COLUMN_PARSER_MAP.put(ColumnType.VARCHAR, VARCHAR);
    TYPE_TO_COLUMN_PARSER_MAP.put(ColumnType.BINARY, BINARY);
    TYPE_TO_COLUMN_PARSER_MAP.put(ColumnType.VARBINARY, VARBINARY);
    TYPE_TO_COLUMN_PARSER_MAP.put(ColumnType.TINYTEXT, TEXT);
    TYPE_TO_COLUMN_PARSER_MAP.put(ColumnType.TEXT, TEXT);
    TYPE_TO_COLUMN_PARSER_MAP.put(ColumnType.MEDIUMTEXT, TEXT);
    TYPE_TO_COLUMN_PARSER_MAP.put(ColumnType.LONGTEXT, TEXT);
    TYPE_TO_COLUMN_PARSER_MAP.put(ColumnType.TINYBLOB, BLOB);
    TYPE_TO_COLUMN_PARSER_MAP.put(ColumnType.BLOB, BLOB);
    TYPE_TO_COLUMN_PARSER_MAP.put(ColumnType.MEDIUMBLOB, BLOB);
    TYPE_TO_COLUMN_PARSER_MAP.put(ColumnType.LONGBLOB, BLOB);
    TYPE_TO_COLUMN_PARSER_MAP.put(ColumnType.DATETIME, DATETIME2);
    TYPE_TO_COLUMN_PARSER_MAP.put(ColumnType.TIMESTAMP, TIMESTAMP);
    TYPE_TO_COLUMN_PARSER_MAP.put(ColumnType.YEAR, YEAR);
    TYPE_TO_COLUMN_PARSER_MAP.put(ColumnType.DATE, DATE);
    TYPE_TO_COLUMN_PARSER_MAP.put(ColumnType.FLOAT, FLOAT);
    TYPE_TO_COLUMN_PARSER_MAP.put(ColumnType.DOUBLE, DOUBLE);
  }
}
