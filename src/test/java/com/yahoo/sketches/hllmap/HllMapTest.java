/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hllmap;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.Util;

public class HllMapTest {

  @Test
  public void singleKeyTest() {
    int k = 1024;
    int u = 1000;
    int keySize = 4;
    HllMap map = HllMap.getInstance(keySize, k);
    Assert.assertTrue(Double.isNaN(map.getEstimate(null)));
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
    key = Util.intToBytes(1, key);
    for (int i=1; i<= u; i++) {
      id = Util.intToBytes(i, id);
      short coupon = (short) Map.coupon16(id);
      est = map.update(key, coupon);
      if ((i % 100) == 0) {
        double err = ((est/i) -1.0) * 100;
        String eStr = String.format("%.3f%%", err);
        println("i: "+i + "\t Est: " + est + "\t" + eStr);
      }
    }
    byte[] key2 = Util.intToBytes(2, key);
    Assert.assertEquals(map.getEstimate(key2), 0.0);
    Assert.assertEquals(map.getKeySizeBytes(), 4);

//    println("Table Entries : " + map.getTableEntries());
    Assert.assertEquals(map.getCurrentCountEntries(), 1);
//    println("Cur Count     : " + map.getCurrentCountEntries());
//    println("RSE           : " + (1/Math.sqrt(k)));
    //map.printEntry(key);
  }

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
      key = Util.intToBytes(j, key);
      for (i=0; i< u; i++) {
        id = Util.longToBytes(++v, id);
        short coupon = (short) Map.coupon16(id);
        map.update(key, coupon);
      }
      double est = map.getEstimate(key);
      Assert.assertTrue(map.getUpperBound(key) > est);
      Assert.assertTrue(map.getLowerBound(key) < est);
      double err = ((est/u) -1.0) * 100;
      String eStr = String.format("%.3f%%", err);
      println("key: " + j + "\tu: "+u + "\t Est: " + est + "\t" + eStr);
    }
    Assert.assertEquals(317, map.getTableEntries());
//    println("Table Entries  : " + map.getTableEntries());
    Assert.assertEquals(200, map.getCurrentCountEntries());
//    println("Cur Count      : " + map.getCurrentCountEntries());
//    println("Theoretical RSE: " + (1/Math.sqrt(k)));
    for (j=1; j<=keys; j++) {
      key = Util.intToBytes(j, key);
      double est = map.getEstimate(key);
      double err = ((est/u) -1.0) * 100;
      String eStr = String.format("%.3f%%", err);
      println("key: " + j + "\tu: "+u + "\t Est: " + est + "\t" + eStr);
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
