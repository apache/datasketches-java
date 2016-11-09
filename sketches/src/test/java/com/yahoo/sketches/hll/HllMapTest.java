/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.MapTestingUtil.bytesToLong;
import static com.yahoo.sketches.hll.MapTestingUtil.intToBytes;
import static com.yahoo.sketches.hll.MapTestingUtil.longToBytes;
import static org.testng.Assert.assertEquals;

import org.testng.Assert;
import org.testng.annotations.Test;


public class HllMapTest {

  @SuppressWarnings("unused")
  @Test
  public void singleKeyTest() {
    int k = 1024;
    int u = 1000;
    int keySize = 4;
    HllMap map = HllMap.getInstance(keySize, k);
    Assert.assertEquals(map.getEstimate(null), Double.NaN);
    Assert.assertTrue(map.getEntrySizeBytes() > 800);
    Assert.assertEquals(map.getCapacityEntries(), 147);
    Assert.assertEquals(map.getTableEntries(), 157);
    Assert.assertTrue(map.getMemoryUsageBytes() < 140000);
//    println("Entry bytes   : " + map.getEntrySizeBytes());
//    println("Capacity      : " + map.getCapacityEntries());
//    println("Table Entries : " + map.getTableEntries());
//    println("Est Arr Size  : " + (map.getEntrySizeBytes() * map.getTableEntries()));
//    println("Size of Arrays: "+ map.getMemoryUsageBytes());

    byte[] key = new byte[4];
    byte[] id = new byte[4];
    double est;
    key = intToBytes(1, key);
    for (int i=1; i<= u; i++) {
      id = intToBytes(i, id);
      int coupon = Map.coupon16(id);
      est = map.update(key, coupon);
      if (i % 100 == 0) {
        double err = (est/i -1.0) * 100;
        String eStr = String.format("%.3f%%", err);
        //println("i: "+i + "\t Est: " + est + TAB + eStr);
      }
    }
    byte[] key2 = intToBytes(2, key);
    Assert.assertEquals(0.0, map.getEstimate(key2));
    Assert.assertEquals(4, map.getKeySizeBytes());

//    println("Table Entries : " + map.getTableEntries());
    Assert.assertEquals(map.getCurrentCountEntries(), 1);
//    println("Cur Count     : " + map.getCurrentCountEntries());
//    println("RSE           : " + (1/Math.sqrt(k)));
    //map.printEntry(key);
  }

  @SuppressWarnings("unused")
  @Test
  public void resizeTest() {
    int k = 1024;
    int u = 257;
    int keys = 200;
    int keySize = 4;
    long v = 0;
    HllMap map = HllMap.getInstance(keySize, k);
    Assert.assertTrue(map.getEntrySizeBytes() > 800);
    Assert.assertEquals(map.getCapacityEntries(), 147);
    Assert.assertEquals(map.getTableEntries(), 157);
    Assert.assertTrue(map.getMemoryUsageBytes() < 140000);
//    println("Entry bytes   : " + map.getEntrySizeBytes());
//    println("Capacity      : " + map.getCapacityEntries());
//    println("Table Entries : " + map.getTableEntries());
//    println("Size of Arrays: " + map.getMemoryUsageBytes());
    byte[] key = new byte[4];
    byte[] id = new byte[8];
    int i, j;
    for (j=1; j<=keys; j++) {
      key = intToBytes(j, key);
      for (i=0; i< u; i++) {
        id = longToBytes(++v, id);
        assertEquals(v, bytesToLong(id)); //TODO remove
        int coupon = Map.coupon16(id);
        map.update(key, coupon);
      }
      double est = map.getEstimate(key);
      double err = (est/u -1.0) * 100;
      String eStr = String.format("%.3f%%", err);
      //println("key: " + j + "\tu: "+u + "\t Est: " + est + TAB + eStr);
    }
    Assert.assertEquals(317, map.getTableEntries());
//    println("Table Entries  : " + map.getTableEntries());
    Assert.assertEquals(200, map.getCurrentCountEntries());
//    println("Cur Count      : " + map.getCurrentCountEntries());
//    println("Theoretical RSE: " + (1/Math.sqrt(k)));
    for (j=1; j<=keys; j++) {
      key = intToBytes(j, key);
      double est = map.getEstimate(key);
      double err = (est/u -1.0) * 100;
      String eStr = String.format("%.3f%%", err);
      //println("key: " + j + "\tu: "+u + "\t Est: " + est + TAB + eStr);
    }
    //println(map.toString());
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
