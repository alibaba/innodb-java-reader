/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * <pre>
 *   mysql> SHOW CHARACTER SET;
 * +----------+-----------------------------+---------------------+--------+
 * | Charset  | Description                 | Default collation   | Maxlen |
 * +----------+-----------------------------+---------------------+--------+
 * | big5     | Big5 Traditional Chinese    | big5_chinese_ci     |      2 |
 * | dec8     | DEC West European           | dec8_swedish_ci     |      1 |
 * | cp850    | DOS West European           | cp850_general_ci    |      1 |
 * | hp8      | HP West European            | hp8_english_ci      |      1 |
 * | koi8r    | KOI8-R Relcom Russian       | koi8r_general_ci    |      1 |
 * | latin1   | cp1252 West European        | latin1_swedish_ci   |      1 |
 * | latin2   | ISO 8859-2 Central European | latin2_general_ci   |      1 |
 * | swe7     | 7bit Swedish                | swe7_swedish_ci     |      1 |
 * | ascii    | US ASCII                    | ascii_general_ci    |      1 |
 * | ujis     | EUC-JP Japanese             | ujis_japanese_ci    |      3 |
 * | sjis     | Shift-JIS Japanese          | sjis_japanese_ci    |      2 |
 * | hebrew   | ISO 8859-8 Hebrew           | hebrew_general_ci   |      1 |
 * | tis620   | TIS620 Thai                 | tis620_thai_ci      |      1 |
 * | euckr    | EUC-KR Korean               | euckr_korean_ci     |      2 |
 * | koi8u    | KOI8-U Ukrainian            | koi8u_general_ci    |      1 |
 * | gb2312   | GB2312 Simplified Chinese   | gb2312_chinese_ci   |      2 |
 * | greek    | ISO 8859-7 Greek            | greek_general_ci    |      1 |
 * | cp1250   | Windows Central European    | cp1250_general_ci   |      1 |
 * | gbk      | GBK Simplified Chinese      | gbk_chinese_ci      |      2 |
 * | latin5   | ISO 8859-9 Turkish          | latin5_turkish_ci   |      1 |
 * | armscii8 | ARMSCII-8 Armenian          | armscii8_general_ci |      1 |
 * | utf8     | UTF-8 Unicode               | utf8_general_ci     |      3 |
 * | ucs2     | UCS-2 Unicode               | ucs2_general_ci     |      2 |
 * | cp866    | DOS Russian                 | cp866_general_ci    |      1 |
 * | keybcs2  | DOS Kamenicky Czech-Slovak  | keybcs2_general_ci  |      1 |
 * | macce    | Mac Central European        | macce_general_ci    |      1 |
 * | macroman | Mac West European           | macroman_general_ci |      1 |
 * | cp852    | DOS Central European        | cp852_general_ci    |      1 |
 * | latin7   | ISO 8859-13 Baltic          | latin7_general_ci   |      1 |
 * | utf8mb4  | UTF-8 Unicode               | utf8mb4_general_ci  |      4 |
 * | cp1251   | Windows Cyrillic            | cp1251_general_ci   |      1 |
 * | utf16    | UTF-16 Unicode              | utf16_general_ci    |      4 |
 * | utf16le  | UTF-16LE Unicode            | utf16le_general_ci  |      4 |
 * | cp1256   | Windows Arabic              | cp1256_general_ci   |      1 |
 * | cp1257   | Windows Baltic              | cp1257_general_ci   |      1 |
 * | utf32    | UTF-32 Unicode              | utf32_general_ci    |      4 |
 * | binary   | Binary pseudo charset       | binary              |      1 |
 * | geostd8  | GEOSTD8 Georgian            | geostd8_general_ci  |      1 |
 * | cp932    | SJIS for Windows Japanese   | cp932_japanese_ci   |      2 |
 * | eucjpms  | UJIS for Windows Japanese   | eucjpms_japanese_ci |      3 |
 * +----------+-----------------------------+---------------------+--------+
 * </pre>
 *
 * Note that table charset here is only used for calculating var-len field's max bytes for one char
 *
 * @author xu.zx
 */
public class CharsetMapping {

  public static final Map<String, MySqlCharset> MYSQL_CHARSET_MAP;

  static {
    Map<String, MySqlCharset> map = new HashMap<>();
    map.put("big5", new MySqlCharset(2, "Big5"));
    map.put("dec8", new MySqlCharset(1, "Cp1252"));
    map.put("cp850", new MySqlCharset(1, "Cp850"));
    map.put("hp8", new MySqlCharset(1, "Cp1252"));
    map.put("koi8r", new MySqlCharset(1, "KOI8_R"));
    map.put("latin1", new MySqlCharset(1, "Cp1252"));
    map.put("latin2", new MySqlCharset(1, "ISO8859_2"));
    map.put("swe7", new MySqlCharset(1, "Cp1252"));
    map.put("ascii", new MySqlCharset(1, "ASCII"));
    map.put("ujis", new MySqlCharset(3, "EUC_JP"));
    map.put("sjis", new MySqlCharset(2, "SHIFT_JIS"));
    map.put("hebrew", new MySqlCharset(1, "ISO8859_8"));
    map.put("tis620", new MySqlCharset(1, "TIS620"));
    map.put("euckr", new MySqlCharset(2, "EUC-KR"));
    map.put("koi8u", new MySqlCharset(1, "KOI8_R"));
    map.put("gb2312", new MySqlCharset(2, "GB2312"));
    map.put("greek", new MySqlCharset(1, "ISO8859_7"));
    map.put("cp1250", new MySqlCharset(1, "Cp1250"));
    map.put("gbk", new MySqlCharset(2, "GBK"));
    map.put("latin5", new MySqlCharset(1, "ISO8859_9"));
    map.put("armscii8", new MySqlCharset(1, "Cp1252"));
    map.put("utf8", new MySqlCharset(3, "UTF-8"));
    map.put("ucs2", new MySqlCharset(2, "UnicodeBig"));
    map.put("cp866", new MySqlCharset(1, "Cp866"));
    map.put("keybcs2", new MySqlCharset(1, "Cp852"));
    map.put("macce", new MySqlCharset(1, "MacCentralEurope"));
    map.put("macroman", new MySqlCharset(1, "MacRoman"));
    map.put("cp852", new MySqlCharset(1, "Cp852"));
    map.put("latin7", new MySqlCharset(1, "ISO-8859-13"));
    map.put("utf8mb4", new MySqlCharset(4, "UTF-8"));
    map.put("cp1251", new MySqlCharset(1, "Cp1251"));
    map.put("utf16", new MySqlCharset(4, "UTF-16"));
    map.put("utf16le", new MySqlCharset(4, "UTF-16LE"));
    map.put("cp1256", new MySqlCharset(1, "Cp1256"));
    map.put("cp1257", new MySqlCharset(1, "Cp1257"));
    map.put("utf32", new MySqlCharset(4, "UTF-32"));
    map.put("binary", new MySqlCharset(1, "ISO8859_1"));
    map.put("geostd8", new MySqlCharset(1, "Cp1252"));
    map.put("cp932", new MySqlCharset(2, "WINDOWS-31J"));
    map.put("eucjpms", new MySqlCharset(3, "EUC_JP_Solaris"));

    MYSQL_CHARSET_MAP = Collections.unmodifiableMap(map);
  }

  public static String getJavaEncodingForMysqlCharset(String mysqlCharsetName) {
    MySqlCharset mySqlCharset = MYSQL_CHARSET_MAP.get(mysqlCharsetName);
    if (mySqlCharset == null) {
      throw new UnsupportedOperationException(mysqlCharsetName + " not supported");
    }
    return mySqlCharset.javaCharset;
  }

  public static int getMaxByteLengthForMysqlCharset(String mysqlCharsetName) {
    MySqlCharset mySqlCharset = MYSQL_CHARSET_MAP.get(mysqlCharsetName);
    if (mySqlCharset == null) {
      throw new UnsupportedOperationException(mysqlCharsetName + " not supported");
    }
    return mySqlCharset.maxByteLen;
  }

  static class MySqlCharset {

    private final int maxByteLen;

    private final String javaCharset;

    MySqlCharset(int maxByteLen, String javaCharset) {
      this.maxByteLen = maxByteLen;
      this.javaCharset = javaCharset;
    }
  }

}
