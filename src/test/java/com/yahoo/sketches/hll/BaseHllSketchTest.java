/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.yahoo.memory.WritableMemory;

/**
 * @author Lee Rhodes
 *
 */
public class BaseHllSketchTest {

  @Test
  public void checkUpdateTypes() {
    HllSketch sk = new HllSketch(10);
    byte[] byteArr = null;
    sk.update(byteArr);
    sk.update(new byte[] {});
    sk.update(new byte[] {0, 1, 2, 3});
    char[] charArr = null;
    sk.update(charArr);
    sk.update(new char[] {});
    sk.update(new char[] {0, 1, 2, 3});
    sk.update(1.0);
    sk.update(-0.0);
    int[] intArr = null;
    sk.update(intArr);
    sk.update(new int[] {});
    sk.update(new int[] {0, 1, 2, 3});
    sk.update(1234L);
    long[] longArr = null;
    sk.update(longArr);
    sk.update(new long[] {});
    sk.update(new long[] {0, 1, 2, 3});
    String s = null;
    sk.update(s);
    s = "";
    sk.update(s);
    sk.update("1234");

    Union u = new Union(10);
    byte[] byteArr1 = null;
    u.update(byteArr1);
    u.update(new byte[] {});
    u.update(new byte[] {0, 1, 2, 3});
    char[] charArr1 = null;
    u.update(charArr1);
    u.update(new char[] {});
    u.update(new char[] {0, 1, 2, 3});
    u.update(1.0);
    u.update(-0.0);
    int[] intArr1 = null;
    u.update(intArr1);
    u.update(new int[] {});
    u.update(new int[] {0, 1, 2, 3});
    u.update(1234L);
    long[] longArr1 = null;
    u.update(longArr1);
    u.update(new long[] {});
    u.update(new long[] {0, 1, 2, 3});
    String s1 = null;
    u.update(s1);
    s1 = "";
    u.update(s);
    u.update("1234");
  }

  @Test
  public void misc() {
    HllSketch sk = new HllSketch(10, TgtHllType.HLL_4);
    assertTrue(sk.isEstimationMode());
    sk.reset();
    assertEquals(BaseHllSketch.getSerializationVersion(), PreambleUtil.SER_VER);
    WritableMemory wmem = WritableMemory.wrap(sk.toCompactByteArray());
    assertEquals(BaseHllSketch.getSerializationVersion(wmem), PreambleUtil.SER_VER);
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }
}
