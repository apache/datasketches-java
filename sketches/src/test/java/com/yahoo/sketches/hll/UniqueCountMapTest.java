/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.SketchesArgumentException;

public class UniqueCountMapTest {
  private static final int k_ = 512;

  @Test
  public void nullKey() {
    UniqueCountMap map = new UniqueCountMap(16, 4, k_);
    double estimate = map.update(null, null);
    Assert.assertEquals(estimate, Double.NaN);
    Assert.assertEquals(map.getEstimate(null), Double.NaN);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void wrongSizeKeyUpdate() {
    UniqueCountMap map = new UniqueCountMap(16, 4, k_);
    byte[] key = new byte[] {0};
    map.update(key, null);
  }

  @SuppressWarnings("unused")
  @Test(expectedExceptions = SketchesArgumentException.class)
  public void wrongSizeKey() {
    UniqueCountMap map = new UniqueCountMap(16, 2, k_);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void wrongSizeKeyGetEstimate() {
    UniqueCountMap map = new UniqueCountMap(16, 4, k_);
    byte[] key = new byte[] {0};
    map.getEstimate(key);
  }

  @SuppressWarnings("unused")
  @Test(expectedExceptions = SketchesArgumentException.class)
  public void wrongSizeK() {
    UniqueCountMap map = new UniqueCountMap(16, 4, 8);
  }

  @Test
  public void emptyMapNullValue() {
    UniqueCountMap map = new UniqueCountMap(16, 4, k_);
    double estimate = map.update("1234".getBytes(), null);
    Assert.assertEquals(estimate, 0.0);
  }

  @Test
  public void oneEntry() {
    UniqueCountMap map = new UniqueCountMap(16, 4, k_);
    double estimate = map.update("1234".getBytes(), "a".getBytes());
    Assert.assertEquals(estimate, 1.0, 0.01);
  }

  @Test
  public void duplicateEntry() {
    UniqueCountMap map = new UniqueCountMap(16, 4, k_);
    byte[] key = "1234".getBytes();
    double estimate = map.update(key, "a".getBytes());
    Assert.assertEquals(estimate, 1.0);
    estimate = map.update(key, "a".getBytes());
    Assert.assertEquals(estimate, 1.0);
    estimate = map.update(key, null);
    Assert.assertEquals(estimate, 1.0);
  }

  @Test
  public void oneKeyTwoValues() {
    UniqueCountMap map = new UniqueCountMap(16, 4, k_);
    double estimate = map.update("1234".getBytes(), "a".getBytes());
    Assert.assertEquals(estimate, 1.0);
    estimate = map.update("1234".getBytes(), "b".getBytes());
    Assert.assertEquals(estimate, 2.0, 0.02);
  }

  @Test
  public void oneKeyThreeValues() {
    UniqueCountMap map = new UniqueCountMap(16, 4, k_);
    byte[] key = "1234".getBytes();
    double estimate = map.update(key, "a".getBytes());
    Assert.assertEquals(estimate, 1.0);
    estimate = map.update(key, "b".getBytes());
    Assert.assertEquals(estimate, 2.0);
    estimate = map.update(key, "c".getBytes());
    Assert.assertEquals(estimate, 3.0);
  }

  @SuppressWarnings("unused")
  @Test
  public void oneKeyManyValues() {
    UniqueCountMap map = new UniqueCountMap(16, 4, k_);
    byte[] key = "1234".getBytes();
    byte[] id = new byte[4];
    for (int i = 1; i <= 1000; i++) {
      id = MapTestingUtil.intToBytes(i, id);
      double estimate = map.update(key, id);
      if (i % 100 == 0) {
        double err = (estimate/i -1.0) * 100;
        String eStr = String.format("%.3f%%", err);
        //println("i: "+i + "\t Est: " + estimate + TAB + eStr);
      }
      Assert.assertEquals(estimate, i, i * 0.10);
      Assert.assertEquals(map.getEstimate(key), estimate);
    }
    String out = map.toString();
    println(out);
  }

  @Test
  public void manyKeys() {
    UniqueCountMap map = new UniqueCountMap(2000, 4, k_);
    for (int i = 1; i <= 1000; i++) {
      byte[] key = String.format("%4s", i).getBytes();
      double estimate = map.update(key, new byte[] {1});
      Assert.assertEquals(estimate, 1.0);
    }
    Assert.assertEquals(1000, map.getActiveEntries());
    for (int i = 1; i <= 1000; i++) {
      byte[] key = String.format("%4s", i).getBytes();
      double estimate = map.update(key, new byte[] {2});
      Assert.assertEquals(estimate, 2.0);
    }
    Assert.assertEquals(1000, map.getActiveEntries());
    for (int i = 1; i <= 1000; i++) {
      byte[] key = String.format("%4s", i).getBytes();
      double estimate = map.update(key, new byte[] {3});
      Assert.assertEquals(estimate, 3.0);
    }
    Assert.assertEquals(1000, map.getActiveEntries());
    String out = map.toString();
    println(out);
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
   //System.out.println(s); //disable here
  }

}
