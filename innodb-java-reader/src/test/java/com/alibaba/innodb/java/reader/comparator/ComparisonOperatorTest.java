package com.alibaba.innodb.java.reader.comparator;

import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class ComparisonOperatorTest {

  @Test
  public void testParse() {
    assertThat(ComparisonOperator.parse(">="), is(ComparisonOperator.GTE));
    assertThat(ComparisonOperator.parse("<="), is(ComparisonOperator.LTE));
    assertThat(ComparisonOperator.parse(">"), is(ComparisonOperator.GT));
    assertThat(ComparisonOperator.parse("<"), is(ComparisonOperator.LT));
    assertThat(ComparisonOperator.parse("nop"), is(ComparisonOperator.NOP));
    assertThat(ComparisonOperator.parse("="), nullValue());
    assertThat(ComparisonOperator.parse(null), nullValue());
    assertThat(ComparisonOperator.parse(""), nullValue());
    assertThat(ComparisonOperator.parse("NOP"), nullValue());

    assertThat(ComparisonOperator.isLowerBoundOp("NOP"), is(false));
    assertThat(ComparisonOperator.isUpperBoundOp("NOP"), is(false));
    assertThat(ComparisonOperator.isLowerBoundOp(">="), is(true));
    assertThat(ComparisonOperator.isLowerBoundOp(">"), is(true));
    assertThat(ComparisonOperator.isLowerBoundOp("<="), is(false));
    assertThat(ComparisonOperator.isLowerBoundOp("<"), is(false));
    assertThat(ComparisonOperator.isUpperBoundOp(">="), is(false));
    assertThat(ComparisonOperator.isUpperBoundOp(">"), is(false));
    assertThat(ComparisonOperator.isUpperBoundOp("<="), is(true));
    assertThat(ComparisonOperator.isUpperBoundOp("<"), is(true));
    assertThat(ComparisonOperator.isLowerBoundOp(Arrays.asList(">=")), is(true));
    assertThat(ComparisonOperator.isUpperBoundOp(Arrays.asList("<=")), is(true));
    assertThat(ComparisonOperator.isLowerBoundOp(">=", ">"), is(true));
    assertThat(ComparisonOperator.isUpperBoundOp("<=", "<"), is(true));
  }

}
