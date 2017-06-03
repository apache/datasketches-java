/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hllmap;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.hllmap.CouponHashMap;
import com.yahoo.sketches.hllmap.Map;

public class CouponHashMapTest {

  @Test
  public void getEstimateNoEntry() {
    CouponHashMap map = CouponHashMap.getInstance(4, 16);
    byte[] key = new byte[] {0, 0, 0, 0};
    Assert.assertEquals(map.getEstimate(key), 0.0);
    Assert.assertEquals(map.getUpperBound(key), 0.0);
    Assert.assertEquals(map.getLowerBound(key), 0.0);
  }

  @Test
  public void oneKeyOneEntry() {
    CouponHashMap map = CouponHashMap.getInstance(4, 16);
    byte[] key = new byte[] {0, 0, 0, 0};
    double estimate = map.update(key, (short) 1);
    Assert.assertEquals(estimate, 1.0);
    Assert.assertEquals(map.getEstimate(key), 1.0);
    Assert.assertTrue(map.getUpperBound(key) > 1.0);
    Assert.assertTrue(map.getLowerBound(key) < 1.0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void keyNotFound() {
    CouponHashMap map = CouponHashMap.getInstance(4, 16);
    byte[] key = new byte[] {0, 0, 0, 0};
    map.update(key, (short) 1);
    map.updateEstimate(map.findKey(new byte[] {1,0,0,0}), 2.0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void wrongCouponsPerKey() {
    CouponHashMap map = CouponHashMap.getInstance(4, 8);
    println(map.toString()); //required otherwise FindBugs will show error
  }

  @Test
  public void delete() {
    CouponHashMap map = CouponHashMap.getInstance(4, 16);
    double estimate = map.update("1234".getBytes(UTF_8), (short) 1);
    Assert.assertEquals(estimate, 1.0);
    int index1 = map.findKey("1234".getBytes(UTF_8));
    Assert.assertTrue(index1 >= 0);
    map.deleteKey(index1);
    int index2 = map.findKey("1234".getBytes(UTF_8));
    // should be complement of the same index as before
    Assert.assertEquals(~index2, index1);
    Assert.assertEquals(map.getEstimate("1234".getBytes(UTF_8)), 0.0);
  }

  @Test
  public void growAndShrink() {
    CouponHashMap map = CouponHashMap.getInstance(4, 16);
    long sizeBytes1 = map.getMemoryUsageBytes();
    for (int i = 0; i < 1000; i ++) {
      byte[] key = String.format("%4s", i).getBytes(UTF_8);
      map.update(key, (short) Map.coupon16(new byte[] {1}));
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
