package com.alibaba.innodb.java.reader.util;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * @author xu.zx
 */
public class RandomGeneratorMainTest {

//  public static void main(String[] args) {
//    Random random = new Random();
//    String tableName = "tb22";
//    String col = ""; // (c1,c2,c3)
//    String template = "insert into " + tableName + col + " values(%d, '%s', '%s');";
//    List<String> list = new ArrayList<>();
//    int start = 1000;
//    int size = 50;
//    for (int i = start; i < start + size; i++) {
//      String c1 = RandomStringUtils.randomAlphabetic(4 + random.nextInt(16));
//      String c2 = RandomStringUtils.randomAlphabetic(4 + random.nextInt(10));
//      System.out.println(String.format(template, i, c1, c2));
//      list.add(c1);
//    }
//    list.stream().map(s -> "\"" + s + "\",").forEach(System.out::println);
//  }

  public static void main(String[] args) {
    Random random = new Random();
    String tableName = "tb24";
    String col = ""; // (c1,c2,c3)
    String template = "insert into " + tableName + col + " values('%s', %d, "
        + "'%s', '%s', '%s', '%s', '%s', '%s');";

    List<String> list = new ArrayList<>();
    for (int i = 1000; i < 2000; i++) {
      for (int j = 1; j <= 2; j++) {
        for (int k = 1; k <= 2; k++) {
          list.add(String.format(template,
              StringUtils.repeat((char) (97 + i % 26), i % 20 + 1),
              j,
              StringUtils.repeat((char) (97 + i % 26), i % 10 + 1),
              StringUtils.repeat((char) (97 + i % 26), i % 4 + 1),
              i + StringUtils.repeat((char) (97 + i % 26), i % 10 + 1),
              k + StringUtils.repeat((char) (97 + i % 26), i % 5 + 1),
              "2019-10-02 10:59:59",
              RandomStringUtils.randomAlphabetic(4 + random.nextInt(8)))
          );
        }
      }
    }
    Collections.shuffle(list);
    list.forEach(System.out::println);
  }
}
