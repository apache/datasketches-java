/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hllmap;

import static com.yahoo.sketches.hash.MurmurHash3.hash;
import static java.nio.charset.StandardCharsets.UTF_8;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;

public class UniqueCountMapTest {
  private final static int INIT_ENTRIES = 211;
  @Test
  public void nullKey() {
    UniqueCountMap map = new UniqueCountMap(4);
    double estimate = map.update(null, null);
    Assert.assertTrue(Double.isNaN(estimate));
    Assert.assertTrue(Double.isNaN(map.getEstimate(null)));
    Assert.assertTrue(Double.isNaN(map.getUpperBound(null)));
    Assert.assertTrue(Double.isNaN(map.getLowerBound(null)));
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void wrongSizeKeyUpdate() {
    UniqueCountMap map = new UniqueCountMap(INIT_ENTRIES, 4);
    byte[] key = new byte[] {0};
    map.update(key, null);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void wrongSizeKey() {
    UniqueCountMap map = new UniqueCountMap(INIT_ENTRIES, 2);
    println(map.toString());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void wrongSizeKeyGetEstimate() {
    UniqueCountMap map = new UniqueCountMap(INIT_ENTRIES, 4);
    byte[] key = new byte[] {0};
    map.getEstimate(key);
  }

  @Test
  public void emptyMapNullValue() {
    UniqueCountMap map = new UniqueCountMap(INIT_ENTRIES, 4);
    double estimate = map.update("1234".getBytes(UTF_8), null);
    Assert.assertEquals(estimate, 0.0);
  }

  @Test
  public void oneEntry() {
    UniqueCountMap map = new UniqueCountMap(INIT_ENTRIES, 4);
    double estimate = map.update("1234".getBytes(UTF_8), "a".getBytes(UTF_8));
    Assert.assertEquals(estimate, 1.0, 0.01);
  }

  @Test
  public void duplicateEntry() {
    UniqueCountMap map = new UniqueCountMap(INIT_ENTRIES, 4);
    byte[] key = "1234".getBytes(UTF_8);
    double estimate = map.update(key, "a".getBytes(UTF_8));
    Assert.assertEquals(estimate, 1.0);
    estimate = map.update(key, "a".getBytes(UTF_8));
    Assert.assertEquals(estimate, 1.0);
    estimate = map.update(key, null);
    Assert.assertEquals(estimate, 1.0);
  }

  @Test
  public void oneKeyTwoValues() {
    UniqueCountMap map = new UniqueCountMap(INIT_ENTRIES, 4);
    double estimate = map.update("1234".getBytes(UTF_8), "a".getBytes(UTF_8));
    Assert.assertEquals(estimate, 1.0);
    estimate = map.update("1234".getBytes(UTF_8), "b".getBytes(UTF_8));
    Assert.assertEquals(estimate, 2.0, 0.02);
  }

  @Test
  public void oneKeyThreeValues() {
    UniqueCountMap map = new UniqueCountMap(INIT_ENTRIES, 4);
    byte[] key = "1234".getBytes(UTF_8);
    double estimate = map.update(key, "a".getBytes(UTF_8));
    Assert.assertEquals(estimate, 1.0);
    estimate = map.update(key, "b".getBytes(UTF_8));
    Assert.assertEquals(estimate, 2.0);
    estimate = map.update(key, "c".getBytes(UTF_8));
    Assert.assertEquals(estimate, 3.0);
  }

  @Test
  public void oneKeyManyValues() {
    UniqueCountMap map = new UniqueCountMap(INIT_ENTRIES, 4);
    byte[] key = "1234".getBytes(UTF_8);
    byte[] id = new byte[4];
    for (int i = 1; i <= 1000; i++) {
      id = Util.intToBytes(i, id);
      double estimate = map.update(key, id);
      if ((i % 100) == 0) {
        double err = ((estimate/i) -1.0) * 100;
        String eStr = String.format("%.3f%%", err);
        println("i: "+i + "\t Est: " + estimate + "\t" + eStr);
      }
      Assert.assertEquals(estimate, i, i * 0.10);
      Assert.assertEquals(map.getEstimate(key), estimate);
      Assert.assertTrue(map.getUpperBound(key) >= estimate);
      Assert.assertTrue(map.getLowerBound(key) <= estimate);
    }
    String out = map.toString();
    println(out);
  }

  @Test
  public void manyKeys() {
    UniqueCountMap map = new UniqueCountMap(2000, 4);
    for (int i = 1; i <= 1000; i++) {
      byte[] key = String.format("%4s", i).getBytes(UTF_8);
      double estimate = map.update(key, new byte[] {1});
      Assert.assertEquals(estimate, 1.0);
    }
    Assert.assertEquals(1000, map.getActiveEntries());
    for (int i = 1; i <= 1000; i++) {
      byte[] key = String.format("%4s", i).getBytes(UTF_8);
      double estimate = map.update(key, new byte[] {2});
      Assert.assertEquals(estimate, 2.0);
    }
    Assert.assertEquals(1000, map.getActiveEntries());
    for (int i = 1; i <= 1000; i++) {
      byte[] key = String.format("%4s", i).getBytes(UTF_8);
      double estimate = map.update(key, new byte[] {3});
      Assert.assertEquals(estimate, 3.0);
    }
    Assert.assertEquals(1000, map.getActiveEntries());
    String out = map.toString();
    println(out);
  }

  @Test
  public void forceDeletesAndReuse() {
    UniqueCountMap map = new UniqueCountMap(156, 4);
    byte[] key = new byte[4];
    byte[] id = new byte[8];
    for (int v = 1; v <= 200; v++) {
      long h = (int) hash(new long[]{v}, 0L)[0];
      id = Util.longToBytes(h, id);
      for(int k = 1; k <= 147; k++) {
        key = Util.intToBytes(k, key);
        map.update(key, id);
      }
    }
    //reuse
    for (int v = 1; v <= 200; v++) {
      long h = (int) hash(new long[]{v}, 0L)[0];
      id = Util.longToBytes(h, id);
      for(int k = 1+147; k <= (2 * 147); k++) {
        key = Util.intToBytes(k, key);
        map.update(key, id);
      }
    }
    Assert.assertNotNull(map.getBaseMap());
    Assert.assertNotNull(map.getHllMap());
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
