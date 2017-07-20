/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 *
 */
public class HllArrayTest {

  @Test
  public void checkCompositeEst() {
    testComposite(4, TgtHllType.HLL_8, 1000);
    testComposite(5, TgtHllType.HLL_8, 1000);
    testComposite(6, TgtHllType.HLL_8, 1000);
    testComposite(13, TgtHllType.HLL_8, 10000);
  }

  @Test
  public void checkBigHipGetRse() {
    HllSketch sk = new HllSketch(13, TgtHllType.HLL_8);
    for (int i = 0; i < 10000; i++) {
      sk.update(i);
    }
    sk.getRelErr(1);
    sk.getRelErrFactor(1);
  }

  private static void testComposite(int lgK, TgtHllType tgtHllType, int n) {
    Union u = new Union(lgK);
    HllSketch sk = new HllSketch(lgK, tgtHllType);
    for (int i = 0; i < n; i++) {
      u.update(i);
      sk.update(i);
    }
    u.update(sk); //merge
    HllSketch res = u.getResult(TgtHllType.HLL_8);
    res.getCompositeEstimate();
    res.getRelErr(1);
    res.getRelErrFactor(1);
  }

  @Test
  public void toByteArray_Heapify() {
    int lgK = 4;
    int u = 8;
    toByteArrayHeapify(lgK, TgtHllType.HLL_4, u);
    toByteArrayHeapify(lgK, TgtHllType.HLL_6, u);
    toByteArrayHeapify(lgK, TgtHllType.HLL_8, u);

    lgK = 16;
    u = (((1 << (lgK - 3))/4) * 3) + (1 << 20);
    toByteArrayHeapify(lgK, TgtHllType.HLL_4, u);
    toByteArrayHeapify(lgK, TgtHllType.HLL_6, u);
    toByteArrayHeapify(lgK, TgtHllType.HLL_8, u);

    lgK = 21;
    u = (((1 << (lgK - 3))/4) * 3) + 1000;
    toByteArrayHeapify(lgK, TgtHllType.HLL_4, u);
    toByteArrayHeapify(lgK, TgtHllType.HLL_6, u);
    toByteArrayHeapify(lgK, TgtHllType.HLL_8, u);
  }

  private static void toByteArrayHeapify(int lgK, TgtHllType tgtHllType, int u) {
    HllSketch sk1 = new HllSketch(lgK, tgtHllType);

    for (int i = 0; i < u; i++) {
      sk1.update(i);
    }
    //sk1.update(u);
    double est1 = sk1.getEstimate();
    assertEquals(est1, u, u * .03);

    byte[] byteArray = sk1.toCompactByteArray();
    HllSketch sk2 = HllSketch.heapify(byteArray);
    double est2 = sk2.getEstimate();
    assertEquals(est2, est1, 0.0);

    byteArray = sk1.toUpdatableByteArray();
    sk2 = HllSketch.heapify(byteArray);
    est2 = sk2.getEstimate();
    assertEquals(est2, est1, 0.0);
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
