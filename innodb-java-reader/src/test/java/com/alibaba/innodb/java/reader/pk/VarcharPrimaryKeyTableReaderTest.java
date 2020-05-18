package com.alibaba.innodb.java.reader.pk;

import com.google.common.collect.ImmutableList;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.TableDef;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * <pre>
 * mysql> select * from tb22;
 * +------+---------------------+---------------+
 * | a    | b                   | c             |
 * +------+---------------------+---------------+
 * | 1027 | aBwdPAceTNRye       | cvqAjjwZn     |
 * | 1008 | AFyWQZNUsCMw        | VHAEoTjL      |
 * | 1010 | aRDHzZ              | mqLGQrE       |
 * | 1004 | BDAAtBQgsPYNxN      | HztbZbkphG    |
 * | 1025 | BppuboMjxzkij       | LWeDmbRXHgckd |
 * | 1041 | BsuxwSugTypSAlVNuW  | vJXUm         |
 * | 1020 | dQHWR               | KcTIoBvVX     |
 * | 1014 | Dxstao              | KGmCViUVik    |
 * | 1005 | eUQrFyWRrTffoEE     | pGQp          |
 * | 1048 | EZhmQGLBPU          | zOJzyuAVOQPp  |
 * | 1021 | fdKpnkqd            | ltiebcMQu     |
 * | 1018 | FiEJik              | vaVnmoci      |
 * | 1031 | ijXdMygdbuwFirmVdCu | UrKYzAxEOp    |
 * | 1024 | IRWnWUYPFHYnDZcipSM | iszt          |
 * | 1047 | IVWXADChNxYSUrl     | ipXyhwR       |
 * | 1034 | JalCOrmyiZcbh       | UGjHAtdHzgLdj |
 * | 1049 | JgNZgCGmxaJ         | LNZuzCdotVeLs |
 * | 1015 | jxzaYocdsoiwqIEj    | hHZF          |
 * | 1045 | KivHxrRtiREpX       | CMVcOMYoVD    |
 * | 1037 | KJZGM               | tEIdM         |
 * | 1009 | lfCU                | nKgCThAmR     |
 * | 1001 | lSuFmysH            | phErsIBRloC   |
 * | 1029 | lvhoHZrm            | IRxtAWTSI     |
 * | 1043 | lwNK                | TYwSXB        |
 * | 1035 | LZAUAIGREmbAFPyyg   | VwRnGtXUIjpR  |
 * | 1011 | NQXqmMuiKMvm        | ABAuGOSiMM    |
 * | 1036 | OjxuhkALfprL        | tJsDzFbY      |
 * | 1007 | ONbvbIhoxHwqGaIy    | MNesQfXQyQGxG |
 * | 1016 | OoLRbZ              | WdRChOaZCz    |
 * | 1012 | oOsELNsMbyyZhysXFE  | MrrlOYJHCIhna |
 * | 1044 | PWsZRlF             | kPIvwUmkoIjk  |
 * | 1039 | PxnTGWQEkr          | hGiyjeiHrSh   |
 * | 1030 | sDgKlAccftHLoQ      | HpLYPTMe      |
 * | 1003 | TaGqVrWdlKgTYNCWba  | rcZfkVDdblKU  |
 * | 1006 | tzosxoiwv           | iLliyB        |
 * | 1013 | uOSDgnzfNFvLhSaQ    | LBoRVPPpdgj   |
 * | 1002 | UqAgj               | lrdl          |
 * | 1042 | VPsThOWj            | XvJh          |
 * | 1033 | VVRJkdAOwSuzPP      | QtEeitu       |
 * | 1032 | WBzNubWmZhE         | MLuWN         |
 * | 1028 | wcaSpJgXlIXFSOGo    | JdAYKbstVv    |
 * | 1019 | wstvkeZfaPm         | JnRtSxlYQDY   |
 * | 1040 | WVmwUrAxgnwEqiHDqo  | vQiNxjOFYMXwX |
 * | 1023 | xZKLizOtLmDWiKxZzr  | mTSRY         |
 * | 1038 | YbxiyFEjh           | gIYpYbNIP     |
 * | 1022 | YEwPw               | uWvHGEggILgU  |
 * | 1046 | yxTeVnPY            | mkCCzwmnPi    |
 * | 1026 | ZCmgxMMtmtboJCi     | QBCmJZqFm     |
 * | 1017 | ZEixXsvsOPB         | MGDFIB        |
 * | 1000 | zWCJnf              | XkAZznw       |
 * +------+---------------------+---------------+
 * </pre>
 *
 * @author xu.zx
 */
public class VarcharPrimaryKeyTableReaderTest extends AbstractTest {

  public TableDef getTableDef() {
    return new TableDef()
        .addColumn(new Column().setName("a").setType("int(11)").setNullable(false))
        .addColumn(new Column().setName("b").setType("varchar(10)").setNullable(false))
        .addColumn(new Column().setName("c").setType("varchar(10)").setNullable(false))
        .setPrimaryKeyColumns(ImmutableList.of("b"));
  }

  private List<String> valList = Arrays.asList("zWCJnf",
      "lSuFmysH",
      "UqAgj",
      "TaGqVrWdlKgTYNCWba",
      "BDAAtBQgsPYNxN",
      "eUQrFyWRrTffoEE",
      "tzosxoiwv",
      "ONbvbIhoxHwqGaIy",
      "AFyWQZNUsCMw",
      "lfCU",
      "aRDHzZ",
      "NQXqmMuiKMvm",
      "oOsELNsMbyyZhysXFE",
      "uOSDgnzfNFvLhSaQ",
      "Dxstao",
      "jxzaYocdsoiwqIEj",
      "OoLRbZ",
      "ZEixXsvsOPB",
      "FiEJik",
      "wstvkeZfaPm",
      "dQHWR",
      "fdKpnkqd",
      "YEwPw",
      "xZKLizOtLmDWiKxZzr",
      "IRWnWUYPFHYnDZcipSM",
      "BppuboMjxzkij",
      "ZCmgxMMtmtboJCi",
      "aBwdPAceTNRye",
      "wcaSpJgXlIXFSOGo",
      "lvhoHZrm",
      "sDgKlAccftHLoQ",
      "ijXdMygdbuwFirmVdCu",
      "WBzNubWmZhE",
      "VVRJkdAOwSuzPP",
      "JalCOrmyiZcbh",
      "LZAUAIGREmbAFPyyg",
      "OjxuhkALfprL",
      "KJZGM",
      "YbxiyFEjh",
      "PxnTGWQEkr",
      "WVmwUrAxgnwEqiHDqo",
      "BsuxwSugTypSAlVNuW",
      "VPsThOWj",
      "lwNK",
      "PWsZRlF",
      "KivHxrRtiREpX",
      "yxTeVnPY",
      "IVWXADChNxYSUrl",
      "EZhmQGLBPU",
      "JgNZgCGmxaJ");

  @Before
  public void before() {
    // CASE_INSENSITIVE_ORDER will first consider the lowercase values
    Collections.sort(valList, String.CASE_INSENSITIVE_ORDER);
  }

  @Test
  public void testVarcharPkMysql56() {
    assertTestOf(this)
        .withMysql56()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testVarcharPkMysql57() {
    assertTestOf(this)
        .withMysql57()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  @Test
  public void testVarcharPkMysql80() {
    assertTestOf(this)
        .withMysql80()
        .withTableDef(getTableDef())
        .checkAllRecordsIs(expected());
  }

  public Consumer<List<GenericRecord>> expected() {
    return recordList -> {

      assertThat(recordList.size(), is(50));

      for (int i = 0; i < 50; i++) {
        GenericRecord r = recordList.get(i);
        Object[] v = r.getValues();
        System.out.println(Arrays.asList(v));
        assertThat(r.getPrimaryKey(), is(ImmutableList.of(valList.get(i))));
        // assertThat(r.get("a"), is(i)); not in order
        assertThat(r.get("b"), is(valList.get(i)));
        assertThat(((String) r.get("c")).length(), greaterThan(3));
      }
    };
  }

  @Test
  public void testVarcharPkCaseSensitiveMysql56() {
    assertTestOf(this)
        .withMysql56()
        .withTableDef(getTableDef())
        .checkQueryByPk(expectedSingle(), ImmutableList.of("aBwdPAceTNRye"));

    assertTestOf(this)
        .withMysql56()
        .withTableDef(getTableDef())
        .checkQueryByPk(expectedSingle(), ImmutableList.of("aBwdPAceTNRye".toUpperCase()));
  }

  @Test
  public void testVarcharPkCaseSensitiveMysql57() {
    assertTestOf(this)
        .withMysql57()
        .withTableDef(getTableDef())
        .checkQueryByPk(expectedSingle(), ImmutableList.of("aBwdPAceTNRye"));

    assertTestOf(this)
        .withMysql57()
        .withTableDef(getTableDef())
        .checkQueryByPk(expectedSingle(), ImmutableList.of("aBwdPAceTNRye".toUpperCase()));
  }

  @Test
  public void testVarcharPkCaseSensitiveMysql80() {
    assertTestOf(this)
        .withMysql80()
        .withTableDef(getTableDef())
        .checkQueryByPk(expectedSingle(), ImmutableList.of("aBwdPAceTNRye"));

    assertTestOf(this)
        .withMysql80()
        .withTableDef(getTableDef())
        .checkQueryByPk(expectedSingle(), ImmutableList.of("aBwdPAceTNRye".toUpperCase()));
  }

  public Consumer<GenericRecord> expectedSingle() {
    return record -> {
      Object[] v = record.getValues();
      System.out.println(Arrays.asList(v));
      assertThat(record.getPrimaryKey(), is(ImmutableList.of(valList.get(0))));
      // assertThat(r.get("a"), is(i)); not in order
      assertThat(record.get("b"), is(valList.get(0)));
      assertThat(((String) record.get("c")).length(), greaterThan(3));
    };
  }

  public static void main(String[] args) {
    Random random = new Random();
    String tableName = "tb22";
    String col = ""; // (c1,c2,c3)
    String template = "insert into " + tableName + col + " values(%d, '%s', '%s');";
    List<String> list = new ArrayList<>();
    int start = 1000;
    int size = 50;
    for (int i = start; i < start + size; i++) {
      String c1 = RandomStringUtils.randomAlphabetic(4 + random.nextInt(16));
      String c2 = RandomStringUtils.randomAlphabetic(4 + random.nextInt(10));
      System.out.println(String.format(template, i, c1, c2));
      list.add(c1);
    }
    list.stream().map(s -> "\"" + s + "\",").forEach(System.out::println);
  }
}
