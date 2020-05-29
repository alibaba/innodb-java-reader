/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.column;

import com.alibaba.innodb.java.reader.exception.ColumnParseException;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.util.BitLiteral;
import com.alibaba.innodb.java.reader.util.MultiEnumLiteral;
import com.alibaba.innodb.java.reader.util.MysqlDecimal;
import com.alibaba.innodb.java.reader.util.SingleEnumLiteral;
import com.alibaba.innodb.java.reader.util.SliceInput;
import com.alibaba.innodb.java.reader.util.Symbol;
import com.alibaba.innodb.java.reader.util.Utils;

import org.apache.commons.lang3.time.FastDateFormat;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Function;

import static com.alibaba.innodb.java.reader.Constants.MAX_ONE_BYTE_ENUM_COUNT;
import static com.alibaba.innodb.java.reader.Constants.PRECISION_LIMIT;
import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_BYTE;
import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_INT;
import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_LONG;
import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_MEDIUMINT;
import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_SHORT;
import static com.alibaba.innodb.java.reader.config.ReaderSystemProperty.ENABLE_TRIM_CHAR;
import static com.alibaba.innodb.java.reader.util.Utils.formatDate;
import static com.google.common.base.Preconditions.checkPositionIndex;

/**
 * Column parser factory.
 * Some of the code are referred from Mysql source code in
 * <code>DataTypeHandler.cc</code>.
 *
 * @author xu.zx
 */
public class ColumnFactory {

  /**
   * This map should be immutable and initialized when program starts.
   */
  private static final Map<String, ColumnParser<?>> TYPE_TO_COLUMN_PARSER_MAP;

  /**
   * MySQL converts TIMESTAMP values from the current time zone to UTC for storage,
   * and back from UTC to the current time zone for retrieval.
   * (This does not occur for other types such as DATETIME.)
   */
  private static final FastDateFormat TIMESTAMP_FORMAT
      = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", TimeZone.getDefault());

  /**
   * Prevent instantiation.
   */
  private ColumnFactory() {
  }

  /**
   * Get {@link ColumnParser} from column type.
   *
   * @param columnType column type
   * @return column parser
   */
  public static ColumnParser<?> getColumnParser(String columnType) {
    ColumnParser<?> result = TYPE_TO_COLUMN_PARSER_MAP.get(columnType);
    if (result == null) {
      throw new ColumnParseException("Column parser not supported for type " + columnType);
    }
    return result;
  }

  /**
   * This works the same as
   * <code>RowSetMetaDataImpl#getColumnClassName(int columnIndex)</code>
   * in MySQL JDBC driver.
   *
   * @param columnType column type
   * @return column java class
   */
  public static Class<?> getColumnJavaType(String columnType) {
    ColumnParser<?> result = TYPE_TO_COLUMN_PARSER_MAP.get(columnType);
    if (result == null) {
      throw new ColumnParseException("Column parser not supported for type " + columnType);
    }
    return result.typeClass();
  }

  private static final ColumnParser<Short> UNSIGNED_TINYINT = new AbstractColumnParser<Short>() {

    @Override
    public Short readFrom(SliceInput input, Column column) {
      return (short) input.readUnsignedByte();
    }

    @Override
    public void skipFrom(SliceInput input, Column column) {
      input.skipBytes(1);
    }

    @Override
    public Class<?> typeClass() {
      return Short.class;
    }
  };

  /**
   * For example,
   * <ul>
   * <li> Decimal: -128 </li>
   * <li> Hexadecimal: 0x80 </li>
   * </ul>
   * By the following operation, we can get the right result. (no signed extension)
   * <pre>
   *   0x00000080 ^ 0xffffff00 = 0xffffff80
   * </pre>
   * <p>
   * <ul>
   * <li> Decimal: -15 </li>
   * <li> Hexadecimal: 0xf1 </li>
   * </ul>
   * By the following operation, we can get the right result. (no signed extension)
   * <pre>
   *   0x000000f1 ^ 0xffffff00 = 0xfffffff1
   * </pre>
   */
  private static final ColumnParser<Byte> TINYINT = new AbstractColumnParser<Byte>() {

    @Override
    public Byte readFrom(SliceInput input, Column column) {
      return (byte) (input.readByte() ^ (-1 << (SIZE_OF_BYTE * 8 - 1)));
    }

    @Override
    public void skipFrom(SliceInput input, Column column) {
      input.skipBytes(1);
    }

    @Override
    public Class<?> typeClass() {
      return Byte.class;
    }
  };

  private static final ColumnParser<Integer> UNSIGNED_SMALLINT = new AbstractColumnParser<Integer>() {

    @Override
    public Integer readFrom(SliceInput input, Column column) {
      return input.readUnsignedShort();
    }

    @Override
    public void skipFrom(SliceInput input, Column column) {
      input.skipBytes(2);
    }

    @Override
    public Class<?> typeClass() {
      return Integer.class;
    }
  };

  private static final ColumnParser<Short> SMALLINT = new AbstractColumnParser<Short>() {

    @Override
    public Short readFrom(SliceInput input, Column column) {
      return (short) (input.readShort() ^ (-1 << (SIZE_OF_SHORT * 8 - 1)));
    }

    @Override
    public void skipFrom(SliceInput input, Column column) {
      input.skipBytes(2);
    }

    @Override
    public Class<?> typeClass() {
      return Short.class;
    }
  };

  private static final ColumnParser<Integer> UNSIGNED_MEDIUMINT = new AbstractColumnParser<Integer>() {

    @Override
    public Integer readFrom(SliceInput input, Column column) {
      return input.readUnsigned3BytesInt();
    }

    @Override
    public void skipFrom(SliceInput input, Column column) {
      input.skipBytes(3);
    }

    @Override
    public Class<?> typeClass() {
      return Integer.class;
    }
  };

  /**
   * Decoding method works the same way as inline function
   * <code>sint3korr</code> from <code>my_byteorder.h</code>
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
   */
  private static final ColumnParser<Integer> MEDIUMINT = new AbstractColumnParser<Integer>() {

    private static final int CONST_0X800000 = 0x800000;

    @Override
    public Integer readFrom(SliceInput input, Column column) {
      // read signed value
      int v = (input.read3BytesInt() ^ (-1 << (SIZE_OF_MEDIUMINT * 8 - 1))) & 0xffffff;
      if ((v & CONST_0X800000) != 0) {
        v = (int) ((255L << 24) | v);
      }
      return v;
    }

    @Override
    public void skipFrom(SliceInput input, Column column) {
      input.skipBytes(3);
    }

    @Override
    public Class<?> typeClass() {
      return Integer.class;
    }
  };

  private static final ColumnParser<Long> UNSIGNED_INT = new AbstractColumnParser<Long>() {

    @Override
    public Long readFrom(SliceInput input, Column column) {
      return input.readUnsignedInt();
    }

    @Override
    public void skipFrom(SliceInput input, Column column) {
      input.skipBytes(4);
    }

    @Override
    public Class<?> typeClass() {
      return Long.class;
    }
  };

  private static final ColumnParser<Integer> INT = new AbstractColumnParser<Integer>() {

    @Override
    public Integer readFrom(SliceInput input, Column column) {
      return input.readInt() ^ (-1 << (SIZE_OF_INT * 8 - 1));
    }

    @Override
    public void skipFrom(SliceInput input, Column column) {
      input.skipBytes(4);
    }

    @Override
    public Class<?> typeClass() {
      return Integer.class;
    }
  };

  private static final ColumnParser<BigInteger> UNSIGNED_BIGINT = new AbstractColumnParser<BigInteger>() {

    @Override
    public BigInteger readFrom(SliceInput input, Column column) {
      return input.readUnsignedLong();
    }

    @Override
    public void skipFrom(SliceInput input, Column column) {
      input.skipBytes(8);
    }

    @Override
    public Class<?> typeClass() {
      return BigInteger.class;
    }
  };

  private static final ColumnParser<Long> BIGINT = new AbstractColumnParser<Long>() {

    @Override
    public Long readFrom(SliceInput input, Column column) {
      return input.readLong() ^ (-1L << (SIZE_OF_LONG * 8 - 1));
    }

    @Override
    public void skipFrom(SliceInput input, Column column) {
      input.skipBytes(8);
    }

    @Override
    public Class<?> typeClass() {
      return Long.class;
    }
  };

  /**
   * https://dev.mysql.com/doc/refman/5.7/en/innodb-row-format.html
   * <p>
   * a CHAR(255) column can exceed 768 bytes if the maximum byte length of the character
   * set is greater than 3, as it is with utf8mb4.
   * <p>
   * 在单字节字符集下，如果存储的是非 NULL 值时，会占满指定的空间。比如 CHAR(10)，存储除 NULL
   * 之外的其它值时，一定会占 10 bytes 空间，不足用 \x20 填充。在多字节字符集下（测试用的是 utf8mb4）
   * 如果存储的是非 NULL 值时, 至少占用与指定值相等的字节空间，比如 CHAR(10)，至少会占用 10 bytes
   * 空间，如果存储的内容超过了 10 bytes （对于 ubf8mb4 编码来说，CHAR(10) 最多能存储 40 bytes 内容），
   * 那么只占用实际占用的字节数。
   * <p>
   * 总结一下就是，对于 CHAR(n)，如果占用的空间字节数少于 n，会用 \x20 填充，大于等于 n 的话，不需再填充。
   * 会用额外字节来记录 CHAR 类型字段实际占用的字节数，这与 VARCHAR 类似。
   */
  private static final ColumnParser<String> CHAR = new AbstractColumnParser<String>() {

    @Override
    public String readFrom(SliceInput input, int len, String charset) {
      String result = input.readString(len, charset);
      if (ENABLE_TRIM_CHAR.value()) {
        return Utils.tailTrim(result);
      }
      return result;
    }

    @Override
    public void skipFrom(SliceInput input, int len, String charset) {
      input.skipBytes(len);
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
    public void skipFrom(SliceInput input, int len, String charset) {
      input.skipBytes(len);
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
    public void skipFrom(SliceInput input, int len, String charset) {
      input.skipBytes(len);
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
    public void skipFrom(SliceInput input, int len, String charset) {
      input.skipBytes(len);
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
    public void skipFrom(SliceInput input, int len, String charset) {
      input.skipBytes(len);
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
    public void skipFrom(SliceInput input, int len, String charset) {
      input.skipBytes(len);
    }

    @Override
    public Class<?> typeClass() {
      return byte[].class;
    }
  };

  /**
   * As of MySQL 5.6.4 the TIME, TIMESTAMP, and DATETIME types can have a fractional seconds part.
   * Storage for these types is big endian (for memcmp() compatibility purposes), with the
   * nonfractional part followed by the fractional part. (Storage and encoding for the YEAR and
   * DATE types remains unchanged.)
   * <p>
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
  private static final ColumnParser<String> DATETIME2 = new AbstractColumnParser<String>() {

    @Override
    public String readFrom(SliceInput input, Column column) {
      long packedValue = input.unpackBigendian(5);
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
      String fractionStr = getFractionString(input, column);
      return String.format("%04d-%02d-%02d %02d:%02d:%02d%s", year, month, day, hour, min, sec, fractionStr);
    }

    @Override
    public Class<?> typeClass() {
      return String.class;
    }
  };

  private static final ColumnParser<String> TIMESTAMP2 = new AbstractColumnParser<String>() {

    @Override
    public String readFrom(SliceInput input, Column column) {
      long packedValue = input.unpackBigendian(4);
      String fractionStr = getFractionString(input, column);
      return String.format("%s%s", TIMESTAMP_FORMAT.format(new Date(packedValue * 1000L)),
          fractionStr);
    }

    @Override
    public Class<?> typeClass() {
      return String.class;
    }
  };

  private static final ColumnParser<String> TIME2 = new AbstractColumnParser<String>() {

    @Override
    public String readFrom(SliceInput input, Column column) {
      int prec = column.getPrecision();
      int fspSize = (1 + prec) / 2;
      int bufSize = 3 + fspSize;
      int fspBits = fspSize * 8;
      int fspMask = (1 << fspBits) - 1;
      int signPos = fspBits + 23;
      long signVal = 1L << signPos;

      /* Read the integer time from the buffer */
      long packedValue = input.unpackBigendian(bufSize);

      boolean isNegative;
      /* Factor it out */
      if ((packedValue & signVal) == signVal) {
        isNegative = false;
      } else {
        isNegative = true;
        // two's complement
        packedValue = signVal - packedValue;
      }
      int usec = (int) (packedValue & fspMask);
      packedValue >>= fspBits;
      int second = (int) (packedValue & 0x3FL);
      packedValue >>= 6;
      int minute = (int) (packedValue & 0x3FL);
      packedValue >>= 6;
      int hour = (int) (packedValue & 0x03FFL);
      packedValue >>= 10;

      while (prec < PRECISION_LIMIT) {
        usec *= 100;
        prec += 2;
      }

      String fractionStr = column.getPrecision() > 0
          ? String.format(".%06d", usec).substring(0, column.getPrecision() + 1) : Symbol.EMPTY;

      return String.format("%s%02d:%02d:%02d%s", isNegative ? "-" : "", hour, minute, second, fractionStr);
    }

    @Override
    public Class<?> typeClass() {
      return String.class;
    }
  };

  private static final ColumnParser<Short> YEAR = new AbstractColumnParser<Short>() {

    @Override
    public Short readFrom(SliceInput input, Column column) {
      int b = input.readUnsignedByte();
      if (b == 0) {
        return (short) 0;
      }
      return (short) (b + 1900);
    }

    @Override
    public void skipFrom(SliceInput input, Column column) {
      input.skipBytes(1);
    }

    @Override
    public Class<?> typeClass() {
      return Short.class;
    }
  };

  private static final ColumnParser<String> DATE = new AbstractColumnParser<String>() {

    @Override
    public String readFrom(SliceInput input, Column column) {
      int encodedDate = (input.read3BytesInt() ^ (-1 << (SIZE_OF_MEDIUMINT * 8 - 1))) & 0xffffff;
      int day = (encodedDate & 31);
      int month = (encodedDate >> 5 & 15);
      int year = (encodedDate >> 9);
      return formatDate(year, month, day);
    }

    @Override
    public Class<?> typeClass() {
      return String.class;
    }
  };

  private static final ColumnParser<Float> FLOAT = new AbstractColumnParser<Float>() {

    @Override
    public Float readFrom(SliceInput input, Column column) {
      return input.readFloat();
    }

    @Override
    public void skipFrom(SliceInput input, Column column) {
      input.skipBytes(4);
    }

    @Override
    public Class<?> typeClass() {
      return Float.class;
    }
  };

  private static final ColumnParser<Double> DOUBLE = new AbstractColumnParser<Double>() {

    @Override
    public Double readFrom(SliceInput input, Column column) {
      return input.readDouble();
    }

    @Override
    public void skipFrom(SliceInput input, Column column) {
      input.skipBytes(8);
    }

    @Override
    public Class<?> typeClass() {
      return Double.class;
    }
  };

  private static final ColumnParser<BigDecimal> DECIMAL = new AbstractColumnParser<BigDecimal>() {

    @Override
    public BigDecimal readFrom(SliceInput input, Column column) {
      int scale = column.getScale();
      int precision = column.getPrecision();
      MysqlDecimal myDecimal = new MysqlDecimal(precision, scale);
      byte[] decBuf = input.readByteArray(myDecimal.getBinSize());
      myDecimal.parse(decBuf);
      return myDecimal.toDecimal();
    }

    @Override
    public Class<?> typeClass() {
      return BigDecimal.class;
    }
  };

  private static final ColumnParser<Boolean> BOOLEAN = new AbstractColumnParser<Boolean>() {

    @Override
    public Boolean readFrom(SliceInput input, Column column) {
      return (input.readByte() & 0x01) == 1;
    }

    @Override
    public void skipFrom(SliceInput input, Column column) {
      input.skipBytes(1);
    }

    @Override
    public Class<?> typeClass() {
      return Boolean.class;
    }
  };

  private static final ColumnParser<SingleEnumLiteral> ENUM = new AbstractColumnParser<SingleEnumLiteral>() {

    @Override
    public SingleEnumLiteral readFrom(SliceInput input, Column column) {
      List<String> enums = column.getEnums();
      int ordinal = enums.size() > MAX_ONE_BYTE_ENUM_COUNT
          ? input.readUnsignedShort() : (int) input.readByte();
      checkPositionIndex(ordinal, enums.size(), "Ordinal " + ordinal
          + " is out of range for " + enums);
      return new SingleEnumLiteral(ordinal, enums.get(ordinal - 1));
    }

    @Override
    public void skipFrom(SliceInput input, Column column) {
      if (column.getEnums().size() > MAX_ONE_BYTE_ENUM_COUNT) {
        input.skipBytes(2);
      } else {
        input.skipBytes(1);
      }
    }

    @Override
    public Class<?> typeClass() {
      return SingleEnumLiteral.class;
    }
  };

  private static final ColumnParser<MultiEnumLiteral> SET = new AbstractColumnParser<MultiEnumLiteral>() {

    @Override
    public MultiEnumLiteral readFrom(SliceInput input, Column column) {
      List<String> enums = column.getEnums();
      byte[] bytes = readBytes(input, enums);
      MultiEnumLiteral result = new MultiEnumLiteral(enums.size());
      for (int i = bytes.length - 1; i >= 0; i--) {
        int start = (bytes.length - 1 - i) * SIZE_OF_LONG;
        for (int j = 0; j < SIZE_OF_LONG && start + j < enums.size(); j++) {
          if (((bytes[i] >> j) & 0x01) == 1) {
            result.add(start + j, enums.get(start + j));
          }
        }
      }
      return result;
    }

    @Override
    public void skipFrom(SliceInput input, Column column) {
      readBytes(input, column.getEnums());
    }

    private byte[] readBytes(SliceInput input, List<String> enums) {
      int len = enums.size() / SIZE_OF_LONG;
      if (enums.size() % SIZE_OF_LONG != 0) {
        len++;
      }
      return input.readByteArray(len);
    }

    @Override
    public Class<?> typeClass() {
      return MultiEnumLiteral.class;
    }
  };

  private static final ColumnParser<BitLiteral> BIT = new AbstractColumnParser<BitLiteral>() {

    @Override
    public BitLiteral readFrom(SliceInput input, Column column) {
      int len = column.getLength();
      if (column.getLength() == 0) {
        len = 1;
      }
      return new BitLiteral(readBytes(input, len), len);
    }

    @Override
    public void skipFrom(SliceInput input, Column column) {
      readBytes(input, column.getLength());
    }

    private byte[] readBytes(SliceInput input, int length) {
      int len = length / SIZE_OF_LONG;
      if (length % SIZE_OF_LONG != 0) {
        len++;
      }
      return input.readByteArray(len);
    }

    @Override
    public Class<?> typeClass() {
      return BitLiteral.class;
    }
  };

  /**
   * For table without primary key provided, MySQL will use a default 6 bytes integer to
   * represent primary key.
   */
  private static final ColumnParser<Long> ROW_ID = new AbstractColumnParser<Long>() {

    @Override
    public Long readFrom(SliceInput input, Column column) {
      return input.readUnsigned6BytesInt();
    }

    @Override
    public void skipFrom(SliceInput input, Column column) {
      input.skipBytes(6);
    }

    @Override
    public Class<?> typeClass() {
      return Long.class;
    }
  };

  private static String getFractionString(SliceInput input, Column column) {
    if (column.getPrecision() > 0) {
      int fraction = readFraction(input, column.getPrecision());
      return String.format(".%06d", fraction).substring(0, column.getPrecision() + 1);
    }
    return Symbol.EMPTY;
  }

  private static int readFraction(SliceInput input, int precision) {
    long usec = 0;
    if (precision > 0) {
      int bufsz = (1 + precision) / 2;
      usec = input.unpackBigendian(bufsz);
      while (precision < PRECISION_LIMIT) {
        usec *= 100;
        precision += 2;
      }
    }
    return (int) usec;
  }

  /**
   * Get function to parse string to java object from column type.
   *
   * @param columnType column type
   * @return function to parse string to java object
   */
  public static Function<String, ?> getColumnToJavaTypeFunc(String columnType) {
    ColumnParser<?> columnParser = TYPE_TO_COLUMN_PARSER_MAP.get(columnType);
    if (columnParser == null) {
      throw new ColumnParseException("Column parser not supported for type " + columnType);
    }
    Class<?> javaType = columnParser.typeClass();
    if (Integer.class.equals(javaType)) {
      return Integer::parseInt;
    } else if (Long.class.equals(javaType)) {
      return Long::parseLong;
    } else if (Boolean.class.equals(javaType)) {
      return Boolean::parseBoolean;
    } else if (Byte.class.equals(javaType)) {
      return Byte::parseByte;
    } else if (Short.class.equals(javaType)) {
      // year
      return Short::parseShort;
    } else if (Float.class.equals(javaType)) {
      return Float::parseFloat;
    } else if (Double.class.equals(javaType)) {
      return Double::parseDouble;
    } else if (BigInteger.class.equals(javaType)) {
      return BigInteger::new;
    } else if (BigDecimal.class.equals(javaType)) {
      return BigDecimal::new;
    } else if (byte[].class.equals(javaType)) {
      return s -> s;
    } else if (SingleEnumLiteral.class.equals(javaType)) {
      return s -> new SingleEnumLiteral(0, s);
    } else if (MultiEnumLiteral.class.equals(javaType)) {
      return s -> {
        List<String> inputList = Arrays.asList(s.split(Symbol.COMMA));
        MultiEnumLiteral multiEnumLiteral = new MultiEnumLiteral(inputList.size());
        for (String input : inputList) {
          multiEnumLiteral.add(0, input);
        }
        return multiEnumLiteral;
      };
    } else {
      // also for date, time, timestamp and datetime
      // we take them as string
      return s -> s;
    }
  }

  static {
    Map<String, ColumnParser<?>> typeToColumnParserMap = new HashMap<>();
    typeToColumnParserMap.put(ColumnType.TINYINT, TINYINT);
    typeToColumnParserMap.put(ColumnType.UNSIGNED_TINYINT, UNSIGNED_TINYINT);
    typeToColumnParserMap.put(ColumnType.SMALLINT, SMALLINT);
    typeToColumnParserMap.put(ColumnType.UNSIGNED_SMALLINT, UNSIGNED_SMALLINT);
    typeToColumnParserMap.put(ColumnType.MEDIUMINT, MEDIUMINT);
    typeToColumnParserMap.put(ColumnType.UNSIGNED_MEDIUMINT, UNSIGNED_MEDIUMINT);
    typeToColumnParserMap.put(ColumnType.INT, INT);
    typeToColumnParserMap.put(ColumnType.UNSIGNED_INT, UNSIGNED_INT);
    typeToColumnParserMap.put(ColumnType.BIGINT, BIGINT);
    typeToColumnParserMap.put(ColumnType.UNSIGNED_BIGINT, UNSIGNED_BIGINT);
    typeToColumnParserMap.put(ColumnType.CHAR, CHAR);
    typeToColumnParserMap.put(ColumnType.VARCHAR, VARCHAR);
    typeToColumnParserMap.put(ColumnType.BINARY, BINARY);
    typeToColumnParserMap.put(ColumnType.VARBINARY, VARBINARY);
    typeToColumnParserMap.put(ColumnType.TINYTEXT, TEXT);
    typeToColumnParserMap.put(ColumnType.TEXT, TEXT);
    typeToColumnParserMap.put(ColumnType.MEDIUMTEXT, TEXT);
    typeToColumnParserMap.put(ColumnType.LONGTEXT, TEXT);
    typeToColumnParserMap.put(ColumnType.TINYBLOB, BLOB);
    typeToColumnParserMap.put(ColumnType.BLOB, BLOB);
    typeToColumnParserMap.put(ColumnType.MEDIUMBLOB, BLOB);
    typeToColumnParserMap.put(ColumnType.LONGBLOB, BLOB);
    typeToColumnParserMap.put(ColumnType.DATETIME, DATETIME2);
    typeToColumnParserMap.put(ColumnType.TIMESTAMP, TIMESTAMP2);
    typeToColumnParserMap.put(ColumnType.TIME, TIME2);
    typeToColumnParserMap.put(ColumnType.YEAR, YEAR);
    typeToColumnParserMap.put(ColumnType.DATE, DATE);
    typeToColumnParserMap.put(ColumnType.UNSIGNED_FLOAT, FLOAT);
    typeToColumnParserMap.put(ColumnType.UNSIGNED_REAL, FLOAT);
    typeToColumnParserMap.put(ColumnType.UNSIGNED_DOUBLE, DOUBLE);
    typeToColumnParserMap.put(ColumnType.UNSIGNED_DECIMAL, DECIMAL);
    typeToColumnParserMap.put(ColumnType.UNSIGNED_NUMERIC, DECIMAL);
    typeToColumnParserMap.put(ColumnType.FLOAT, FLOAT);
    typeToColumnParserMap.put(ColumnType.REAL, FLOAT);
    typeToColumnParserMap.put(ColumnType.DOUBLE, DOUBLE);
    typeToColumnParserMap.put(ColumnType.DECIMAL, DECIMAL);
    typeToColumnParserMap.put(ColumnType.NUMERIC, DECIMAL);
    typeToColumnParserMap.put(ColumnType.BOOL, BOOLEAN);
    typeToColumnParserMap.put(ColumnType.BOOLEAN, BOOLEAN);
    typeToColumnParserMap.put(ColumnType.ENUM, ENUM);
    typeToColumnParserMap.put(ColumnType.SET, SET);
    typeToColumnParserMap.put(ColumnType.BIT, BIT);
    typeToColumnParserMap.put(ColumnType.ROW_ID, ROW_ID);
    TYPE_TO_COLUMN_PARSER_MAP = Collections.unmodifiableMap(typeToColumnParserMap);
  }

}
