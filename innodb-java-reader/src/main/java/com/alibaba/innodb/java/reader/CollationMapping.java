/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * <pre>
 * mysql&gt; SHOW COLLATION;
 * +--------------------------+----------+-----+---------+----------+---------+
 * | Collation                | Charset  | Id  | Default | Compiled | Sortlen |
 * +--------------------------+----------+-----+---------+----------+---------+
 * | big5_chinese_ci          | big5     |   1 | Yes     | Yes      |       1 |
 * | big5_bin                 | big5     |  84 |         | Yes      |       1 |
 * | dec8_swedish_ci          | dec8     |   3 | Yes     | Yes      |       1 |
 * | dec8_bin                 | dec8     |  69 |         | Yes      |       1 |
 * | cp850_general_ci         | cp850    |   4 | Yes     | Yes      |       1 |
 * | cp850_bin                | cp850    |  80 |         | Yes      |       1 |
 * | hp8_english_ci           | hp8      |   6 | Yes     | Yes      |       1 |
 * | hp8_bin                  | hp8      |  72 |         | Yes      |       1 |
 * | koi8r_general_ci         | koi8r    |   7 | Yes     | Yes      |       1 |
 * | koi8r_bin                | koi8r    |  74 |         | Yes      |       1 |
 * | latin1_german1_ci        | latin1   |   5 |         | Yes      |       1 |
 * | latin1_swedish_ci        | latin1   |   8 | Yes     | Yes      |       1 |
 * | latin1_danish_ci         | latin1   |  15 |         | Yes      |       1 |
 * | latin1_german2_ci        | latin1   |  31 |         | Yes      |       2 |
 * | latin1_bin               | latin1   |  47 |         | Yes      |       1 |
 * | latin1_general_ci        | latin1   |  48 |         | Yes      |       1 |
 * | latin1_general_cs        | latin1   |  49 |         | Yes      |       1 |
 * | latin1_spanish_ci        | latin1   |  94 |         | Yes      |       1 |
 * | latin2_czech_cs          | latin2   |   2 |         | Yes      |       4 |
 * | latin2_general_ci        | latin2   |   9 | Yes     | Yes      |       1 |
 * | latin2_hungarian_ci      | latin2   |  21 |         | Yes      |       1 |
 * | latin2_croatian_ci       | latin2   |  27 |         | Yes      |       1 |
 * | latin2_bin               | latin2   |  77 |         | Yes      |       1 |
 * | swe7_swedish_ci          | swe7     |  10 | Yes     | Yes      |       1 |
 * | swe7_bin                 | swe7     |  82 |         | Yes      |       1 |
 * | ascii_general_ci         | ascii    |  11 | Yes     | Yes      |       1 |
 * | ascii_bin                | ascii    |  65 |         | Yes      |       1 |
 * | ujis_japanese_ci         | ujis     |  12 | Yes     | Yes      |       1 |
 * | ujis_bin                 | ujis     |  91 |         | Yes      |       1 |
 * | sjis_japanese_ci         | sjis     |  13 | Yes     | Yes      |       1 |
 * | sjis_bin                 | sjis     |  88 |         | Yes      |       1 |
 * | hebrew_general_ci        | hebrew   |  16 | Yes     | Yes      |       1 |
 * | hebrew_bin               | hebrew   |  71 |         | Yes      |       1 |
 * | tis620_thai_ci           | tis620   |  18 | Yes     | Yes      |       4 |
 * | tis620_bin               | tis620   |  89 |         | Yes      |       1 |
 * | euckr_korean_ci          | euckr    |  19 | Yes     | Yes      |       1 |
 * | euckr_bin                | euckr    |  85 |         | Yes      |       1 |
 * | koi8u_general_ci         | koi8u    |  22 | Yes     | Yes      |       1 |
 * | koi8u_bin                | koi8u    |  75 |         | Yes      |       1 |
 * | gb2312_chinese_ci        | gb2312   |  24 | Yes     | Yes      |       1 |
 * | gb2312_bin               | gb2312   |  86 |         | Yes      |       1 |
 * | greek_general_ci         | greek    |  25 | Yes     | Yes      |       1 |
 * | greek_bin                | greek    |  70 |         | Yes      |       1 |
 * | cp1250_general_ci        | cp1250   |  26 | Yes     | Yes      |       1 |
 * | cp1250_czech_cs          | cp1250   |  34 |         | Yes      |       2 |
 * | cp1250_croatian_ci       | cp1250   |  44 |         | Yes      |       1 |
 * | cp1250_bin               | cp1250   |  66 |         | Yes      |       1 |
 * | cp1250_polish_ci         | cp1250   |  99 |         | Yes      |       1 |
 * | gbk_chinese_ci           | gbk      |  28 | Yes     | Yes      |       1 |
 * | gbk_bin                  | gbk      |  87 |         | Yes      |       1 |
 * | latin5_turkish_ci        | latin5   |  30 | Yes     | Yes      |       1 |
 * | latin5_bin               | latin5   |  78 |         | Yes      |       1 |
 * | armscii8_general_ci      | armscii8 |  32 | Yes     | Yes      |       1 |
 * | armscii8_bin             | armscii8 |  64 |         | Yes      |       1 |
 * | utf8_general_ci          | utf8     |  33 | Yes     | Yes      |       1 |
 * | utf8_bin                 | utf8     |  83 |         | Yes      |       1 |
 * | utf8_unicode_ci          | utf8     | 192 |         | Yes      |       8 |
 * | utf8_icelandic_ci        | utf8     | 193 |         | Yes      |       8 |
 * | utf8_latvian_ci          | utf8     | 194 |         | Yes      |       8 |
 * | utf8_romanian_ci         | utf8     | 195 |         | Yes      |       8 |
 * | utf8_slovenian_ci        | utf8     | 196 |         | Yes      |       8 |
 * | utf8_polish_ci           | utf8     | 197 |         | Yes      |       8 |
 * | utf8_estonian_ci         | utf8     | 198 |         | Yes      |       8 |
 * | utf8_spanish_ci          | utf8     | 199 |         | Yes      |       8 |
 * | utf8_swedish_ci          | utf8     | 200 |         | Yes      |       8 |
 * | utf8_turkish_ci          | utf8     | 201 |         | Yes      |       8 |
 * | utf8_czech_ci            | utf8     | 202 |         | Yes      |       8 |
 * | utf8_danish_ci           | utf8     | 203 |         | Yes      |       8 |
 * | utf8_lithuanian_ci       | utf8     | 204 |         | Yes      |       8 |
 * | utf8_slovak_ci           | utf8     | 205 |         | Yes      |       8 |
 * | utf8_spanish2_ci         | utf8     | 206 |         | Yes      |       8 |
 * | utf8_roman_ci            | utf8     | 207 |         | Yes      |       8 |
 * | utf8_persian_ci          | utf8     | 208 |         | Yes      |       8 |
 * | utf8_esperanto_ci        | utf8     | 209 |         | Yes      |       8 |
 * | utf8_hungarian_ci        | utf8     | 210 |         | Yes      |       8 |
 * | utf8_sinhala_ci          | utf8     | 211 |         | Yes      |       8 |
 * | utf8_german2_ci          | utf8     | 212 |         | Yes      |       8 |
 * | utf8_croatian_ci         | utf8     | 213 |         | Yes      |       8 |
 * | utf8_unicode_520_ci      | utf8     | 214 |         | Yes      |       8 |
 * | utf8_vietnamese_ci       | utf8     | 215 |         | Yes      |       8 |
 * | utf8_general_mysql500_ci | utf8     | 223 |         | Yes      |       1 |
 * | ucs2_general_ci          | ucs2     |  35 | Yes     | Yes      |       1 |
 * | ucs2_bin                 | ucs2     |  90 |         | Yes      |       1 |
 * | ucs2_unicode_ci          | ucs2     | 128 |         | Yes      |       8 |
 * | ucs2_icelandic_ci        | ucs2     | 129 |         | Yes      |       8 |
 * | ucs2_latvian_ci          | ucs2     | 130 |         | Yes      |       8 |
 * | ucs2_romanian_ci         | ucs2     | 131 |         | Yes      |       8 |
 * | ucs2_slovenian_ci        | ucs2     | 132 |         | Yes      |       8 |
 * | ucs2_polish_ci           | ucs2     | 133 |         | Yes      |       8 |
 * | ucs2_estonian_ci         | ucs2     | 134 |         | Yes      |       8 |
 * | ucs2_spanish_ci          | ucs2     | 135 |         | Yes      |       8 |
 * | ucs2_swedish_ci          | ucs2     | 136 |         | Yes      |       8 |
 * | ucs2_turkish_ci          | ucs2     | 137 |         | Yes      |       8 |
 * | ucs2_czech_ci            | ucs2     | 138 |         | Yes      |       8 |
 * | ucs2_danish_ci           | ucs2     | 139 |         | Yes      |       8 |
 * | ucs2_lithuanian_ci       | ucs2     | 140 |         | Yes      |       8 |
 * | ucs2_slovak_ci           | ucs2     | 141 |         | Yes      |       8 |
 * | ucs2_spanish2_ci         | ucs2     | 142 |         | Yes      |       8 |
 * | ucs2_roman_ci            | ucs2     | 143 |         | Yes      |       8 |
 * | ucs2_persian_ci          | ucs2     | 144 |         | Yes      |       8 |
 * | ucs2_esperanto_ci        | ucs2     | 145 |         | Yes      |       8 |
 * | ucs2_hungarian_ci        | ucs2     | 146 |         | Yes      |       8 |
 * | ucs2_sinhala_ci          | ucs2     | 147 |         | Yes      |       8 |
 * | ucs2_german2_ci          | ucs2     | 148 |         | Yes      |       8 |
 * | ucs2_croatian_ci         | ucs2     | 149 |         | Yes      |       8 |
 * | ucs2_unicode_520_ci      | ucs2     | 150 |         | Yes      |       8 |
 * | ucs2_vietnamese_ci       | ucs2     | 151 |         | Yes      |       8 |
 * | ucs2_general_mysql500_ci | ucs2     | 159 |         | Yes      |       1 |
 * | cp866_general_ci         | cp866    |  36 | Yes     | Yes      |       1 |
 * | cp866_bin                | cp866    |  68 |         | Yes      |       1 |
 * | keybcs2_general_ci       | keybcs2  |  37 | Yes     | Yes      |       1 |
 * | keybcs2_bin              | keybcs2  |  73 |         | Yes      |       1 |
 * | macce_general_ci         | macce    |  38 | Yes     | Yes      |       1 |
 * | macce_bin                | macce    |  43 |         | Yes      |       1 |
 * | macroman_general_ci      | macroman |  39 | Yes     | Yes      |       1 |
 * | macroman_bin             | macroman |  53 |         | Yes      |       1 |
 * | cp852_general_ci         | cp852    |  40 | Yes     | Yes      |       1 |
 * | cp852_bin                | cp852    |  81 |         | Yes      |       1 |
 * | latin7_estonian_cs       | latin7   |  20 |         | Yes      |       1 |
 * | latin7_general_ci        | latin7   |  41 | Yes     | Yes      |       1 |
 * | latin7_general_cs        | latin7   |  42 |         | Yes      |       1 |
 * | latin7_bin               | latin7   |  79 |         | Yes      |       1 |
 * | utf8mb4_general_ci       | utf8mb4  |  45 | Yes     | Yes      |       1 |
 * | utf8mb4_bin              | utf8mb4  |  46 |         | Yes      |       1 |
 * | utf8mb4_unicode_ci       | utf8mb4  | 224 |         | Yes      |       8 |
 * | utf8mb4_icelandic_ci     | utf8mb4  | 225 |         | Yes      |       8 |
 * | utf8mb4_latvian_ci       | utf8mb4  | 226 |         | Yes      |       8 |
 * | utf8mb4_romanian_ci      | utf8mb4  | 227 |         | Yes      |       8 |
 * | utf8mb4_slovenian_ci     | utf8mb4  | 228 |         | Yes      |       8 |
 * | utf8mb4_polish_ci        | utf8mb4  | 229 |         | Yes      |       8 |
 * | utf8mb4_estonian_ci      | utf8mb4  | 230 |         | Yes      |       8 |
 * | utf8mb4_spanish_ci       | utf8mb4  | 231 |         | Yes      |       8 |
 * | utf8mb4_swedish_ci       | utf8mb4  | 232 |         | Yes      |       8 |
 * | utf8mb4_turkish_ci       | utf8mb4  | 233 |         | Yes      |       8 |
 * | utf8mb4_czech_ci         | utf8mb4  | 234 |         | Yes      |       8 |
 * | utf8mb4_danish_ci        | utf8mb4  | 235 |         | Yes      |       8 |
 * | utf8mb4_lithuanian_ci    | utf8mb4  | 236 |         | Yes      |       8 |
 * | utf8mb4_slovak_ci        | utf8mb4  | 237 |         | Yes      |       8 |
 * | utf8mb4_spanish2_ci      | utf8mb4  | 238 |         | Yes      |       8 |
 * | utf8mb4_roman_ci         | utf8mb4  | 239 |         | Yes      |       8 |
 * | utf8mb4_persian_ci       | utf8mb4  | 240 |         | Yes      |       8 |
 * | utf8mb4_esperanto_ci     | utf8mb4  | 241 |         | Yes      |       8 |
 * | utf8mb4_hungarian_ci     | utf8mb4  | 242 |         | Yes      |       8 |
 * | utf8mb4_sinhala_ci       | utf8mb4  | 243 |         | Yes      |       8 |
 * | utf8mb4_german2_ci       | utf8mb4  | 244 |         | Yes      |       8 |
 * | utf8mb4_croatian_ci      | utf8mb4  | 245 |         | Yes      |       8 |
 * | utf8mb4_unicode_520_ci   | utf8mb4  | 246 |         | Yes      |       8 |
 * | utf8mb4_vietnamese_ci    | utf8mb4  | 247 |         | Yes      |       8 |
 * | cp1251_bulgarian_ci      | cp1251   |  14 |         | Yes      |       1 |
 * | cp1251_ukrainian_ci      | cp1251   |  23 |         | Yes      |       1 |
 * | cp1251_bin               | cp1251   |  50 |         | Yes      |       1 |
 * | cp1251_general_ci        | cp1251   |  51 | Yes     | Yes      |       1 |
 * | cp1251_general_cs        | cp1251   |  52 |         | Yes      |       1 |
 * | utf16_general_ci         | utf16    |  54 | Yes     | Yes      |       1 |
 * | utf16_bin                | utf16    |  55 |         | Yes      |       1 |
 * | utf16_unicode_ci         | utf16    | 101 |         | Yes      |       8 |
 * | utf16_icelandic_ci       | utf16    | 102 |         | Yes      |       8 |
 * | utf16_latvian_ci         | utf16    | 103 |         | Yes      |       8 |
 * | utf16_romanian_ci        | utf16    | 104 |         | Yes      |       8 |
 * | utf16_slovenian_ci       | utf16    | 105 |         | Yes      |       8 |
 * | utf16_polish_ci          | utf16    | 106 |         | Yes      |       8 |
 * | utf16_estonian_ci        | utf16    | 107 |         | Yes      |       8 |
 * | utf16_spanish_ci         | utf16    | 108 |         | Yes      |       8 |
 * | utf16_swedish_ci         | utf16    | 109 |         | Yes      |       8 |
 * | utf16_turkish_ci         | utf16    | 110 |         | Yes      |       8 |
 * | utf16_czech_ci           | utf16    | 111 |         | Yes      |       8 |
 * | utf16_danish_ci          | utf16    | 112 |         | Yes      |       8 |
 * | utf16_lithuanian_ci      | utf16    | 113 |         | Yes      |       8 |
 * | utf16_slovak_ci          | utf16    | 114 |         | Yes      |       8 |
 * | utf16_spanish2_ci        | utf16    | 115 |         | Yes      |       8 |
 * | utf16_roman_ci           | utf16    | 116 |         | Yes      |       8 |
 * | utf16_persian_ci         | utf16    | 117 |         | Yes      |       8 |
 * | utf16_esperanto_ci       | utf16    | 118 |         | Yes      |       8 |
 * | utf16_hungarian_ci       | utf16    | 119 |         | Yes      |       8 |
 * | utf16_sinhala_ci         | utf16    | 120 |         | Yes      |       8 |
 * | utf16_german2_ci         | utf16    | 121 |         | Yes      |       8 |
 * | utf16_croatian_ci        | utf16    | 122 |         | Yes      |       8 |
 * | utf16_unicode_520_ci     | utf16    | 123 |         | Yes      |       8 |
 * | utf16_vietnamese_ci      | utf16    | 124 |         | Yes      |       8 |
 * | utf16le_general_ci       | utf16le  |  56 | Yes     | Yes      |       1 |
 * | utf16le_bin              | utf16le  |  62 |         | Yes      |       1 |
 * | cp1256_general_ci        | cp1256   |  57 | Yes     | Yes      |       1 |
 * | cp1256_bin               | cp1256   |  67 |         | Yes      |       1 |
 * | cp1257_lithuanian_ci     | cp1257   |  29 |         | Yes      |       1 |
 * | cp1257_bin               | cp1257   |  58 |         | Yes      |       1 |
 * | cp1257_general_ci        | cp1257   |  59 | Yes     | Yes      |       1 |
 * | utf32_general_ci         | utf32    |  60 | Yes     | Yes      |       1 |
 * | utf32_bin                | utf32    |  61 |         | Yes      |       1 |
 * | utf32_unicode_ci         | utf32    | 160 |         | Yes      |       8 |
 * | utf32_icelandic_ci       | utf32    | 161 |         | Yes      |       8 |
 * | utf32_latvian_ci         | utf32    | 162 |         | Yes      |       8 |
 * | utf32_romanian_ci        | utf32    | 163 |         | Yes      |       8 |
 * | utf32_slovenian_ci       | utf32    | 164 |         | Yes      |       8 |
 * | utf32_polish_ci          | utf32    | 165 |         | Yes      |       8 |
 * | utf32_estonian_ci        | utf32    | 166 |         | Yes      |       8 |
 * | utf32_spanish_ci         | utf32    | 167 |         | Yes      |       8 |
 * | utf32_swedish_ci         | utf32    | 168 |         | Yes      |       8 |
 * | utf32_turkish_ci         | utf32    | 169 |         | Yes      |       8 |
 * | utf32_czech_ci           | utf32    | 170 |         | Yes      |       8 |
 * | utf32_danish_ci          | utf32    | 171 |         | Yes      |       8 |
 * | utf32_lithuanian_ci      | utf32    | 172 |         | Yes      |       8 |
 * | utf32_slovak_ci          | utf32    | 173 |         | Yes      |       8 |
 * | utf32_spanish2_ci        | utf32    | 174 |         | Yes      |       8 |
 * | utf32_roman_ci           | utf32    | 175 |         | Yes      |       8 |
 * | utf32_persian_ci         | utf32    | 176 |         | Yes      |       8 |
 * | utf32_esperanto_ci       | utf32    | 177 |         | Yes      |       8 |
 * | utf32_hungarian_ci       | utf32    | 178 |         | Yes      |       8 |
 * | utf32_sinhala_ci         | utf32    | 179 |         | Yes      |       8 |
 * | utf32_german2_ci         | utf32    | 180 |         | Yes      |       8 |
 * | utf32_croatian_ci        | utf32    | 181 |         | Yes      |       8 |
 * | utf32_unicode_520_ci     | utf32    | 182 |         | Yes      |       8 |
 * | utf32_vietnamese_ci      | utf32    | 183 |         | Yes      |       8 |
 * | binary                   | binary   |  63 | Yes     | Yes      |       1 |
 * | geostd8_general_ci       | geostd8  |  92 | Yes     | Yes      |       1 |
 * | geostd8_bin              | geostd8  |  93 |         | Yes      |       1 |
 * | cp932_japanese_ci        | cp932    |  95 | Yes     | Yes      |       1 |
 * | cp932_bin                | cp932    |  96 |         | Yes      |       1 |
 * | eucjpms_japanese_ci      | eucjpms  |  97 | Yes     | Yes      |       1 |
 * | eucjpms_bin              | eucjpms  |  98 |         | Yes      |       1 |
 * +--------------------------+----------+-----+---------+----------+---------+
 * </pre>
 *
 * Note that table charset here is only used for calculating var-len field's max bytes for one char
 *
 * @author xu.zx
 */
public class CollationMapping {

  public static final Map<String, MySqlCollation> MYSQL_COLLATION_MAP;

  public static final Map<String, MySqlCollation> MYSQL_CHARSET_TO_DEFAULT_COLLATION_MAP;

  static {
    List<MySqlCollation> collationList = new ArrayList<>();
    collationList.add(new MySqlCollation("big5_chinese_ci",
        "big5", 1, true, true, 1));
    collationList.add(new MySqlCollation("big5_bin",
        "big5", 84, false, true, 1));
    collationList.add(new MySqlCollation("dec8_swedish_ci",
        "dec8", 3, true, true, 1));
    collationList.add(new MySqlCollation("dec8_bin",
        "dec8", 69, false, true, 1));
    collationList.add(new MySqlCollation("cp850_general_ci",
        "cp850", 4, true, true, 1));
    collationList.add(new MySqlCollation("cp850_bin",
        "cp850", 80, false, true, 1));
    collationList.add(new MySqlCollation("hp8_english_ci",
        "hp8", 6, true, true, 1));
    collationList.add(new MySqlCollation("hp8_bin",
        "hp8", 72, false, true, 1));
    collationList.add(new MySqlCollation("koi8r_general_ci",
        "koi8r", 7, true, true, 1));
    collationList.add(new MySqlCollation("koi8r_bin",
        "koi8r", 74, false, true, 1));
    collationList.add(new MySqlCollation("latin1_german1_ci",
        "latin1", 5, false, true, 1));
    collationList.add(new MySqlCollation("latin1_swedish_ci",
        "latin1", 8, true, true, 1));
    collationList.add(new MySqlCollation("latin1_danish_ci",
        "latin1", 15, false, true, 1));
    collationList.add(new MySqlCollation("latin1_german2_ci",
        "latin1", 31, false, true, 2));
    collationList.add(new MySqlCollation("latin1_bin",
        "latin1", 47, false, true, 1));
    collationList.add(new MySqlCollation("latin1_general_ci",
        "latin1", 48, false, true, 1));
    collationList.add(new MySqlCollation("latin1_general_cs",
        "latin1", 49, false, true, 1));
    collationList.add(new MySqlCollation("latin1_spanish_ci",
        "latin1", 94, false, true, 1));
    collationList.add(new MySqlCollation("latin2_czech_cs",
        "latin2", 2, false, true, 4));
    collationList.add(new MySqlCollation("latin2_general_ci",
        "latin2", 9, true, true, 1));
    collationList.add(new MySqlCollation("latin2_hungarian_ci",
        "latin2", 21, false, true, 1));
    collationList.add(new MySqlCollation("latin2_croatian_ci",
        "latin2", 27, false, true, 1));
    collationList.add(new MySqlCollation("latin2_bin",
        "latin2", 77, false, true, 1));
    collationList.add(new MySqlCollation("swe7_swedish_ci",
        "swe7", 10, true, true, 1));
    collationList.add(new MySqlCollation("swe7_bin",
        "swe7", 82, false, true, 1));
    collationList.add(new MySqlCollation("ascii_general_ci",
        "ascii", 11, true, true, 1));
    collationList.add(new MySqlCollation("ascii_bin",
        "ascii", 65, false, true, 1));
    collationList.add(new MySqlCollation("ujis_japanese_ci",
        "ujis", 12, true, true, 1));
    collationList.add(new MySqlCollation("ujis_bin",
        "ujis", 91, false, true, 1));
    collationList.add(new MySqlCollation("sjis_japanese_ci",
        "sjis", 13, true, true, 1));
    collationList.add(new MySqlCollation("sjis_bin",
        "sjis", 88, false, true, 1));
    collationList.add(new MySqlCollation("hebrew_general_ci",
        "hebrew", 16, true, true, 1));
    collationList.add(new MySqlCollation("hebrew_bin",
        "hebrew", 71, false, true, 1));
    collationList.add(new MySqlCollation("tis620_thai_ci",
        "tis620", 18, true, true, 4));
    collationList.add(new MySqlCollation("tis620_bin",
        "tis620", 89, false, true, 1));
    collationList.add(new MySqlCollation("euckr_korean_ci",
        "euckr", 19, true, true, 1));
    collationList.add(new MySqlCollation("euckr_bin",
        "euckr", 85, false, true, 1));
    collationList.add(new MySqlCollation("koi8u_general_ci",
        "koi8u", 22, true, true, 1));
    collationList.add(new MySqlCollation("koi8u_bin",
        "koi8u", 75, false, true, 1));
    collationList.add(new MySqlCollation("gb2312_chinese_ci",
        "gb2312", 24, true, true, 1));
    collationList.add(new MySqlCollation("gb2312_bin",
        "gb2312", 86, false, true, 1));
    collationList.add(new MySqlCollation("greek_general_ci",
        "greek", 25, true, true, 1));
    collationList.add(new MySqlCollation("greek_bin",
        "greek", 70, false, true, 1));
    collationList.add(new MySqlCollation("cp1250_general_ci",
        "cp1250", 26, true, true, 1));
    collationList.add(new MySqlCollation("cp1250_czech_cs",
        "cp1250", 34, false, true, 2));
    collationList.add(new MySqlCollation("cp1250_croatian_ci",
        "cp1250", 44, false, true, 1));
    collationList.add(new MySqlCollation("cp1250_bin",
        "cp1250", 66, false, true, 1));
    collationList.add(new MySqlCollation("cp1250_polish_ci",
        "cp1250", 99, false, true, 1));
    collationList.add(new MySqlCollation("gbk_chinese_ci",
        "gbk", 28, true, true, 1));
    collationList.add(new MySqlCollation("gbk_bin",
        "gbk", 87, false, true, 1));
    collationList.add(new MySqlCollation("latin5_turkish_ci",
        "latin5", 30, true, true, 1));
    collationList.add(new MySqlCollation("latin5_bin",
        "latin5", 78, false, true, 1));
    collationList.add(new MySqlCollation("armscii8_general_ci",
        "armscii8", 32, true, true, 1));
    collationList.add(new MySqlCollation("armscii8_bin",
        "armscii8", 64, false, true, 1));
    collationList.add(new MySqlCollation("utf8_general_ci",
        "utf8", 33, true, true, 1));
    collationList.add(new MySqlCollation("utf8_bin",
        "utf8", 83, false, true, 1));
    collationList.add(new MySqlCollation("utf8_unicode_ci",
        "utf8", 192, false, true, 8));
    collationList.add(new MySqlCollation("utf8_icelandic_ci",
        "utf8", 193, false, true, 8));
    collationList.add(new MySqlCollation("utf8_latvian_ci",
        "utf8", 194, false, true, 8));
    collationList.add(new MySqlCollation("utf8_romanian_ci",
        "utf8", 195, false, true, 8));
    collationList.add(new MySqlCollation("utf8_slovenian_ci",
        "utf8", 196, false, true, 8));
    collationList.add(new MySqlCollation("utf8_polish_ci",
        "utf8", 197, false, true, 8));
    collationList.add(new MySqlCollation("utf8_estonian_ci",
        "utf8", 198, false, true, 8));
    collationList.add(new MySqlCollation("utf8_spanish_ci",
        "utf8", 199, false, true, 8));
    collationList.add(new MySqlCollation("utf8_swedish_ci",
        "utf8", 200, false, true, 8));
    collationList.add(new MySqlCollation("utf8_turkish_ci",
        "utf8", 201, false, true, 8));
    collationList.add(new MySqlCollation("utf8_czech_ci",
        "utf8", 202, false, true, 8));
    collationList.add(new MySqlCollation("utf8_danish_ci",
        "utf8", 203, false, true, 8));
    collationList.add(new MySqlCollation("utf8_lithuanian_ci",
        "utf8", 204, false, true, 8));
    collationList.add(new MySqlCollation("utf8_slovak_ci",
        "utf8", 205, false, true, 8));
    collationList.add(new MySqlCollation("utf8_spanish2_ci",
        "utf8", 206, false, true, 8));
    collationList.add(new MySqlCollation("utf8_roman_ci",
        "utf8", 207, false, true, 8));
    collationList.add(new MySqlCollation("utf8_persian_ci",
        "utf8", 208, false, true, 8));
    collationList.add(new MySqlCollation("utf8_esperanto_ci",
        "utf8", 209, false, true, 8));
    collationList.add(new MySqlCollation("utf8_hungarian_ci",
        "utf8", 210, false, true, 8));
    collationList.add(new MySqlCollation("utf8_sinhala_ci",
        "utf8", 211, false, true, 8));
    collationList.add(new MySqlCollation("utf8_german2_ci",
        "utf8", 212, false, true, 8));
    collationList.add(new MySqlCollation("utf8_croatian_ci",
        "utf8", 213, false, true, 8));
    collationList.add(new MySqlCollation("utf8_unicode_520_ci",
        "utf8", 214, false, true, 8));
    collationList.add(new MySqlCollation("utf8_vietnamese_ci",
        "utf8", 215, false, true, 8));
    collationList.add(new MySqlCollation("utf8_general_mysql500_ci",
        "utf8", 223, false, true, 1));
    collationList.add(new MySqlCollation("ucs2_general_ci",
        "ucs2", 35, true, true, 1));
    collationList.add(new MySqlCollation("ucs2_bin",
        "ucs2", 90, false, true, 1));
    collationList.add(new MySqlCollation("ucs2_unicode_ci",
        "ucs2", 128, false, true, 8));
    collationList.add(new MySqlCollation("ucs2_icelandic_ci",
        "ucs2", 129, false, true, 8));
    collationList.add(new MySqlCollation("ucs2_latvian_ci",
        "ucs2", 130, false, true, 8));
    collationList.add(new MySqlCollation("ucs2_romanian_ci",
        "ucs2", 131, false, true, 8));
    collationList.add(new MySqlCollation("ucs2_slovenian_ci",
        "ucs2", 132, false, true, 8));
    collationList.add(new MySqlCollation("ucs2_polish_ci",
        "ucs2", 133, false, true, 8));
    collationList.add(new MySqlCollation("ucs2_estonian_ci",
        "ucs2", 134, false, true, 8));
    collationList.add(new MySqlCollation("ucs2_spanish_ci",
        "ucs2", 135, false, true, 8));
    collationList.add(new MySqlCollation("ucs2_swedish_ci",
        "ucs2", 136, false, true, 8));
    collationList.add(new MySqlCollation("ucs2_turkish_ci",
        "ucs2", 137, false, true, 8));
    collationList.add(new MySqlCollation("ucs2_czech_ci",
        "ucs2", 138, false, true, 8));
    collationList.add(new MySqlCollation("ucs2_danish_ci",
        "ucs2", 139, false, true, 8));
    collationList.add(new MySqlCollation("ucs2_lithuanian_ci",
        "ucs2", 140, false, true, 8));
    collationList.add(new MySqlCollation("ucs2_slovak_ci",
        "ucs2", 141, false, true, 8));
    collationList.add(new MySqlCollation("ucs2_spanish2_ci",
        "ucs2", 142, false, true, 8));
    collationList.add(new MySqlCollation("ucs2_roman_ci",
        "ucs2", 143, false, true, 8));
    collationList.add(new MySqlCollation("ucs2_persian_ci",
        "ucs2", 144, false, true, 8));
    collationList.add(new MySqlCollation("ucs2_esperanto_ci",
        "ucs2", 145, false, true, 8));
    collationList.add(new MySqlCollation("ucs2_hungarian_ci",
        "ucs2", 146, false, true, 8));
    collationList.add(new MySqlCollation("ucs2_sinhala_ci",
        "ucs2", 147, false, true, 8));
    collationList.add(new MySqlCollation("ucs2_german2_ci",
        "ucs2", 148, false, true, 8));
    collationList.add(new MySqlCollation("ucs2_croatian_ci",
        "ucs2", 149, false, true, 8));
    collationList.add(new MySqlCollation("ucs2_unicode_520_ci",
        "ucs2", 150, false, true, 8));
    collationList.add(new MySqlCollation("ucs2_vietnamese_ci",
        "ucs2", 151, false, true, 8));
    collationList.add(new MySqlCollation("ucs2_general_mysql500_ci",
        "ucs2", 159, false, true, 1));
    collationList.add(new MySqlCollation("cp866_general_ci",
        "cp866", 36, true, true, 1));
    collationList.add(new MySqlCollation("cp866_bin",
        "cp866", 68, false, true, 1));
    collationList.add(new MySqlCollation("keybcs2_general_ci",
        "keybcs2", 37, true, true, 1));
    collationList.add(new MySqlCollation("keybcs2_bin",
        "keybcs2", 73, false, true, 1));
    collationList.add(new MySqlCollation("macce_general_ci",
        "macce", 38, true, true, 1));
    collationList.add(new MySqlCollation("macce_bin",
        "macce", 43, false, true, 1));
    collationList.add(new MySqlCollation("macroman_general_ci",
        "macroman", 39, true, true, 1));
    collationList.add(new MySqlCollation("macroman_bin",
        "macroman", 53, false, true, 1));
    collationList.add(new MySqlCollation("cp852_general_ci",
        "cp852", 40, true, true, 1));
    collationList.add(new MySqlCollation("cp852_bin",
        "cp852", 81, false, true, 1));
    collationList.add(new MySqlCollation("latin7_estonian_cs",
        "latin7", 20, false, true, 1));
    collationList.add(new MySqlCollation("latin7_general_ci",
        "latin7", 41, true, true, 1));
    collationList.add(new MySqlCollation("latin7_general_cs",
        "latin7", 42, false, true, 1));
    collationList.add(new MySqlCollation("latin7_bin",
        "latin7", 79, false, true, 1));
    collationList.add(new MySqlCollation("utf8mb4_general_ci",
        "utf8mb4", 45, true, true, 1));
    collationList.add(new MySqlCollation("utf8mb4_bin",
        "utf8mb4", 46, false, true, 1));
    collationList.add(new MySqlCollation("utf8mb4_unicode_ci",
        "utf8mb4", 224, false, true, 8));
    collationList.add(new MySqlCollation("utf8mb4_icelandic_ci",
        "utf8mb4", 225, false, true, 8));
    collationList.add(new MySqlCollation("utf8mb4_latvian_ci",
        "utf8mb4", 226, false, true, 8));
    collationList.add(new MySqlCollation("utf8mb4_romanian_ci",
        "utf8mb4", 227, false, true, 8));
    collationList.add(new MySqlCollation("utf8mb4_slovenian_ci",
        "utf8mb4", 228, false, true, 8));
    collationList.add(new MySqlCollation("utf8mb4_polish_ci",
        "utf8mb4", 229, false, true, 8));
    collationList.add(new MySqlCollation("utf8mb4_estonian_ci",
        "utf8mb4", 230, false, true, 8));
    collationList.add(new MySqlCollation("utf8mb4_spanish_ci",
        "utf8mb4", 231, false, true, 8));
    collationList.add(new MySqlCollation("utf8mb4_swedish_ci",
        "utf8mb4", 232, false, true, 8));
    collationList.add(new MySqlCollation("utf8mb4_turkish_ci",
        "utf8mb4", 233, false, true, 8));
    collationList.add(new MySqlCollation("utf8mb4_czech_ci",
        "utf8mb4", 234, false, true, 8));
    collationList.add(new MySqlCollation("utf8mb4_danish_ci",
        "utf8mb4", 235, false, true, 8));
    collationList.add(new MySqlCollation("utf8mb4_lithuanian_ci",
        "utf8mb4", 236, false, true, 8));
    collationList.add(new MySqlCollation("utf8mb4_slovak_ci",
        "utf8mb4", 237, false, true, 8));
    collationList.add(new MySqlCollation("utf8mb4_spanish2_ci",
        "utf8mb4", 238, false, true, 8));
    collationList.add(new MySqlCollation("utf8mb4_roman_ci",
        "utf8mb4", 239, false, true, 8));
    collationList.add(new MySqlCollation("utf8mb4_persian_ci",
        "utf8mb4", 240, false, true, 8));
    collationList.add(new MySqlCollation("utf8mb4_esperanto_ci",
        "utf8mb4", 241, false, true, 8));
    collationList.add(new MySqlCollation("utf8mb4_hungarian_ci",
        "utf8mb4", 242, false, true, 8));
    collationList.add(new MySqlCollation("utf8mb4_sinhala_ci",
        "utf8mb4", 243, false, true, 8));
    collationList.add(new MySqlCollation("utf8mb4_german2_ci",
        "utf8mb4", 244, false, true, 8));
    collationList.add(new MySqlCollation("utf8mb4_croatian_ci",
        "utf8mb4", 245, false, true, 8));
    collationList.add(new MySqlCollation("utf8mb4_unicode_520_ci",
        "utf8mb4", 246, false, true, 8));
    collationList.add(new MySqlCollation("utf8mb4_vietnamese_ci",
        "utf8mb4", 247, false, true, 8));
    collationList.add(new MySqlCollation("cp1251_bulgarian_ci",
        "cp1251", 14, false, true, 1));
    collationList.add(new MySqlCollation("cp1251_ukrainian_ci",
        "cp1251", 23, false, true, 1));
    collationList.add(new MySqlCollation("cp1251_bin",
        "cp1251", 50, false, true, 1));
    collationList.add(new MySqlCollation("cp1251_general_ci",
        "cp1251", 51, true, true, 1));
    collationList.add(new MySqlCollation("cp1251_general_cs",
        "cp1251", 52, false, true, 1));
    collationList.add(new MySqlCollation("utf16_general_ci",
        "utf16", 54, true, true, 1));
    collationList.add(new MySqlCollation("utf16_bin",
        "utf16", 55, false, true, 1));
    collationList.add(new MySqlCollation("utf16_unicode_ci",
        "utf16", 101, false, true, 8));
    collationList.add(new MySqlCollation("utf16_icelandic_ci",
        "utf16", 102, false, true, 8));
    collationList.add(new MySqlCollation("utf16_latvian_ci",
        "utf16", 103, false, true, 8));
    collationList.add(new MySqlCollation("utf16_romanian_ci",
        "utf16", 104, false, true, 8));
    collationList.add(new MySqlCollation("utf16_slovenian_ci",
        "utf16", 105, false, true, 8));
    collationList.add(new MySqlCollation("utf16_polish_ci",
        "utf16", 106, false, true, 8));
    collationList.add(new MySqlCollation("utf16_estonian_ci",
        "utf16", 107, false, true, 8));
    collationList.add(new MySqlCollation("utf16_spanish_ci",
        "utf16", 108, false, true, 8));
    collationList.add(new MySqlCollation("utf16_swedish_ci",
        "utf16", 109, false, true, 8));
    collationList.add(new MySqlCollation("utf16_turkish_ci",
        "utf16", 110, false, true, 8));
    collationList.add(new MySqlCollation("utf16_czech_ci",
        "utf16", 111, false, true, 8));
    collationList.add(new MySqlCollation("utf16_danish_ci",
        "utf16", 112, false, true, 8));
    collationList.add(new MySqlCollation("utf16_lithuanian_ci",
        "utf16", 113, false, true, 8));
    collationList.add(new MySqlCollation("utf16_slovak_ci",
        "utf16", 114, false, true, 8));
    collationList.add(new MySqlCollation("utf16_spanish2_ci",
        "utf16", 115, false, true, 8));
    collationList.add(new MySqlCollation("utf16_roman_ci",
        "utf16", 116, false, true, 8));
    collationList.add(new MySqlCollation("utf16_persian_ci",
        "utf16", 117, false, true, 8));
    collationList.add(new MySqlCollation("utf16_esperanto_ci",
        "utf16", 118, false, true, 8));
    collationList.add(new MySqlCollation("utf16_hungarian_ci",
        "utf16", 119, false, true, 8));
    collationList.add(new MySqlCollation("utf16_sinhala_ci",
        "utf16", 120, false, true, 8));
    collationList.add(new MySqlCollation("utf16_german2_ci",
        "utf16", 121, false, true, 8));
    collationList.add(new MySqlCollation("utf16_croatian_ci",
        "utf16", 122, false, true, 8));
    collationList.add(new MySqlCollation("utf16_unicode_520_ci",
        "utf16", 123, false, true, 8));
    collationList.add(new MySqlCollation("utf16_vietnamese_ci",
        "utf16", 124, false, true, 8));
    collationList.add(new MySqlCollation("utf16le_general_ci",
        "utf16le", 56, true, true, 1));
    collationList.add(new MySqlCollation("utf16le_bin",
        "utf16le", 62, false, true, 1));
    collationList.add(new MySqlCollation("cp1256_general_ci",
        "cp1256", 57, true, true, 1));
    collationList.add(new MySqlCollation("cp1256_bin",
        "cp1256", 67, false, true, 1));
    collationList.add(new MySqlCollation("cp1257_lithuanian_ci",
        "cp1257", 29, false, true, 1));
    collationList.add(new MySqlCollation("cp1257_bin",
        "cp1257", 58, false, true, 1));
    collationList.add(new MySqlCollation("cp1257_general_ci",
        "cp1257", 59, true, true, 1));
    collationList.add(new MySqlCollation("utf32_general_ci",
        "utf32", 60, true, true, 1));
    collationList.add(new MySqlCollation("utf32_bin",
        "utf32", 61, false, true, 1));
    collationList.add(new MySqlCollation("utf32_unicode_ci",
        "utf32", 160, false, true, 8));
    collationList.add(new MySqlCollation("utf32_icelandic_ci",
        "utf32", 161, false, true, 8));
    collationList.add(new MySqlCollation("utf32_latvian_ci",
        "utf32", 162, false, true, 8));
    collationList.add(new MySqlCollation("utf32_romanian_ci",
        "utf32", 163, false, true, 8));
    collationList.add(new MySqlCollation("utf32_slovenian_ci",
        "utf32", 164, false, true, 8));
    collationList.add(new MySqlCollation("utf32_polish_ci",
        "utf32", 165, false, true, 8));
    collationList.add(new MySqlCollation("utf32_estonian_ci",
        "utf32", 166, false, true, 8));
    collationList.add(new MySqlCollation("utf32_spanish_ci",
        "utf32", 167, false, true, 8));
    collationList.add(new MySqlCollation("utf32_swedish_ci",
        "utf32", 168, false, true, 8));
    collationList.add(new MySqlCollation("utf32_turkish_ci",
        "utf32", 169, false, true, 8));
    collationList.add(new MySqlCollation("utf32_czech_ci",
        "utf32", 170, false, true, 8));
    collationList.add(new MySqlCollation("utf32_danish_ci",
        "utf32", 171, false, true, 8));
    collationList.add(new MySqlCollation("utf32_lithuanian_ci",
        "utf32", 172, false, true, 8));
    collationList.add(new MySqlCollation("utf32_slovak_ci",
        "utf32", 173, false, true, 8));
    collationList.add(new MySqlCollation("utf32_spanish2_ci",
        "utf32", 174, false, true, 8));
    collationList.add(new MySqlCollation("utf32_roman_ci",
        "utf32", 175, false, true, 8));
    collationList.add(new MySqlCollation("utf32_persian_ci",
        "utf32", 176, false, true, 8));
    collationList.add(new MySqlCollation("utf32_esperanto_ci",
        "utf32", 177, false, true, 8));
    collationList.add(new MySqlCollation("utf32_hungarian_ci",
        "utf32", 178, false, true, 8));
    collationList.add(new MySqlCollation("utf32_sinhala_ci",
        "utf32", 179, false, true, 8));
    collationList.add(new MySqlCollation("utf32_german2_ci",
        "utf32", 180, false, true, 8));
    collationList.add(new MySqlCollation("utf32_croatian_ci",
        "utf32", 181, false, true, 8));
    collationList.add(new MySqlCollation("utf32_unicode_520_ci",
        "utf32", 182, false, true, 8));
    collationList.add(new MySqlCollation("utf32_vietnamese_ci",
        "utf32", 183, false, true, 8));
    collationList.add(new MySqlCollation("binary",
        "binary", 63, true, true, 1));
    collationList.add(new MySqlCollation("geostd8_general_ci",
        "geostd8", 92, true, true, 1));
    collationList.add(new MySqlCollation("geostd8_bin",
        "geostd8", 93, false, true, 1));
    collationList.add(new MySqlCollation("cp932_japanese_ci",
        "cp932", 95, true, true, 1));
    collationList.add(new MySqlCollation("cp932_bin",
        "cp932", 96, false, true, 1));
    collationList.add(new MySqlCollation("eucjpms_japanese_ci",
        "eucjpms", 97, true, true, 1));
    collationList.add(new MySqlCollation("eucjpms_bin",
        "eucjpms", 98, false, true, 1));

    MYSQL_COLLATION_MAP = ImmutableMap.copyOf(Maps.uniqueIndex(collationList, new Function<MySqlCollation, String>() {
      @Nullable
      @Override
      public String apply(@Nullable MySqlCollation collation) {
        return collation.collation;
      }
    }));

    ImmutableMap.Builder<String, MySqlCollation> map = ImmutableMap.builder();
    for (MySqlCollation collation : collationList) {
      if (collation.defaultForCharset) {
        map.put(collation.charset, collation);
      }
    }
    MYSQL_CHARSET_TO_DEFAULT_COLLATION_MAP = map.build();
  }

  public static String getDefaultCollation(String mysqlCharsetName) {
    // check charset exist
    CharsetMapping.getJavaCharsetForMysqlCharset(mysqlCharsetName);
    MySqlCollation collation = MYSQL_CHARSET_TO_DEFAULT_COLLATION_MAP.get(mysqlCharsetName);
    if (collation == null) {
      throw new UnsupportedOperationException("Collation " + collation + " not supported");
    }
    return collation.collation;
  }

  public static boolean isCollationCaseSensitive(String collation) {
    if (!MYSQL_COLLATION_MAP.containsKey(collation)) {
      throw new UnsupportedOperationException("Collation " + collation + " not supported");
    }
    return !MYSQL_COLLATION_MAP.get(collation).collation.contains("ci");
  }

  static class MySqlCollation {

    private String collation;
    private String charset;
    private int id;
    private boolean defaultForCharset;
    private boolean compiled;
    private int sortLen;

    MySqlCollation(String collation, String charset, int id,
                   boolean defaultForCharset, boolean compiled, int sortLen) {
      this.collation = collation;
      this.charset = charset;
      this.id = id;
      this.defaultForCharset = defaultForCharset;
      this.compiled = compiled;
      this.sortLen = sortLen;
    }
  }

}
