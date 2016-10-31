/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hllmap;

import org.testng.Assert;
import org.testng.annotations.Test;

public class CouponTraverseMapTest {

  @Test
  public void getEstimateNovelKey() {
    CouponTraverseMap map = CouponTraverseMap.getInstance(4, 1);
    byte[] key = new byte[] {0, 0, 0 ,0};
    Assert.assertEquals(map.getEstimate(key), 0.0);
  }

  @Test
  public void oneKeyOneValue() {
    CouponTraverseMap map = CouponTraverseMap.getInstance(4, 1);
    byte[] key = new byte[] {0, 0, 0 ,0};
    double estimate = map.update(key, 1);
    Assert.assertEquals(estimate, 1.0);
    Assert.assertEquals(map.getEstimate(key), 1.0);
  }

  @Test
  public void delete() {
    CouponTraverseMap map = CouponTraverseMap.getInstance(4, 1);
    double estimate = map.update("1234".getBytes(), 1);
    Assert.assertEquals(estimate, 1.0);
    int index1 = map.findKey("1234".getBytes());
    Assert.assertTrue(index1 >= 0);
    map.deleteKey(index1);
    int index2 = map.findKey("1234".getBytes());
    // should be complement of the same index as before
    Assert.assertEquals(~index2, index1);
    Assert.assertEquals(map.getEstimate("1".getBytes()), 0.0);
  }

  @Test
  public void growAndShrink() {
    CouponTraverseMap map = CouponTraverseMap.getInstance(4, 1);
    long sizeBytes1 = map.getMemoryUsageBytes();
    for (int i = 0; i < 1000; i ++) {
      byte[] key = String.format("%4s", i).getBytes();
      map.update(key, 1);
    }
    long sizeBytes2 = map.getMemoryUsageBytes();
    Assert.assertTrue(sizeBytes2 > sizeBytes1);
    for (int i = 0; i < 1000; i ++) {
      byte[] key = String.format("%4s", i).getBytes();
      int index = map.findKey(key);
      Assert.assertTrue(index >= 0);
      map.deleteKey(index);
    }
    long sizeBytes3 = map.getMemoryUsageBytes();
    Assert.assertTrue(sizeBytes3 < sizeBytes2);
    println(map.toString());
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
