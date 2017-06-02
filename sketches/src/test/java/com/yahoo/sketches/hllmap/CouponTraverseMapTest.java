/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hllmap;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.hllmap.CouponTraverseMap;

public class CouponTraverseMapTest {

  @Test
  public void getEstimateNoEntry() {
    CouponTraverseMap map = CouponTraverseMap.getInstance(4, 1);
    byte[] key = new byte[] {0, 0, 0 ,0};
    Assert.assertEquals(map.getEstimate(key), 0.0);
    Assert.assertEquals(map.getUpperBound(key), 0.0);
    Assert.assertEquals(map.getLowerBound(key), 0.0);
  }

  @Test
  public void oneKeyOneEntry() {
    CouponTraverseMap map = CouponTraverseMap.getInstance(4, 1);
    byte[] key = new byte[] {0, 0, 0 ,0};
    double estimate = map.update(key, (short) 1);
    Assert.assertEquals(estimate, 1.0);
    Assert.assertEquals(map.getEstimate(key), 1.0);
    Assert.assertTrue(map.getUpperBound(key) >= 1.0);
    Assert.assertTrue(map.getLowerBound(key) <= 1.0);
  }

  @Test
  public void delete() {
    CouponTraverseMap map = CouponTraverseMap.getInstance(4, 1);
    double estimate = map.update("1234".getBytes(UTF_8), (short) 1);
    Assert.assertEquals(estimate, 1.0);
    int index1 = map.findKey("1234".getBytes(UTF_8));
    Assert.assertTrue(index1 >= 0);
    map.deleteKey(index1);
    int index2 = map.findKey("1234".getBytes(UTF_8));
    // should be complement of the same index as before
    Assert.assertEquals(~index2, index1);
    Assert.assertEquals(map.getEstimate("1".getBytes(UTF_8)), 0.0);
  }

  @Test
  public void growAndShrink() {
    CouponTraverseMap map = CouponTraverseMap.getInstance(4, 1);
    long sizeBytes1 = map.getMemoryUsageBytes();
    for (int i = 0; i < 1000; i ++) {
      byte[] key = String.format("%4s", i).getBytes(UTF_8);
      map.update(key, (short) 1);
    }
    long sizeBytes2 = map.getMemoryUsageBytes();
    Assert.assertTrue(sizeBytes2 > sizeBytes1);
    for (int i = 0; i < 1000; i ++) {
      byte[] key = String.format("%4s", i).getBytes(UTF_8);
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
