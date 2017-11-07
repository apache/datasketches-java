/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.CurMode.HLL;
import static com.yahoo.sketches.hll.CurMode.LIST;
import static com.yahoo.sketches.hll.CurMode.SET;
import static com.yahoo.sketches.hll.TgtHllType.HLL_4;
import static com.yahoo.sketches.hll.TgtHllType.HLL_6;
import static com.yahoo.sketches.hll.TgtHllType.HLL_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.yahoo.memory.WritableMemory;

/**
 * @author Lee Rhodes
 */
public class DirectUnionCaseTest {
  long v = 0;

  @Test
  public void checkCase0() { //src: LIST, gadget: LIST, cases 0, 0
    int n1 = 2;
    int n2 = 3;
    int n3 = 2;
    int sum = n1 + n2 + n3;
    Union u = buildUnion(12, n1);
    HllSketch h2 = build(11, HLL_6, n2);
    HllSketch h3 = build(10, HLL_8, n3);
    u.update(h2);
    println(u.toString());
    assertEquals(u.getCurMode(), LIST);
    u.update(h3);
    println(u.toString());
    assertEquals(u.getCurMode(), LIST);
    assertEquals(u.getLgConfigK(), 12);
    assertFalse(u.isOutOfOrderFlag());
    double err = sum * errorFactor(u.getLgConfigK(), u.isOutOfOrderFlag(), 2.0);
    println("ErrToll: " + err);
    assertEquals(u.getEstimate(), sum, err);
  }

  @Test
  public void checkCase1() { //src: SET, gadget: LIST, cases 0, 1
    int n1 = 5;
    int n2 = 2;
    int n3 = 16;
    int sum = n1 + n2 + n3;
    Union u = buildUnion(12, n1);        //LIST, 5
    HllSketch h2 = build(11, HLL_6, n2); //LIST, 2
    HllSketch h3 = build(10, HLL_8, n3); //SET
    u.update(h2);
    println(u.toString());
    assertEquals(u.getCurMode(), LIST);
    u.update(h3);
    println(u.toString());
    assertEquals(u.getCurMode(), SET);
    assertEquals(u.getLgConfigK(), 12);
    assertTrue(u.isOutOfOrderFlag());
    double err = sum * errorFactor(u.getLgConfigK(), u.isOutOfOrderFlag(), 2.0);
    println("ErrToll: " + err);
    assertEquals(u.getEstimate(), sum, err);

  }

  @Test
  public void checkCase2() { //src: HLL, gadget: LIST, swap, cases 0, 2
    int n1 = 5;
    int n2 = 2;
    int n3 = 97;
    int sum = n1 + n2 + n3;
    Union u = buildUnion(12, n1);
    HllSketch h2 = build(11, HLL_8, n2);
    HllSketch h3 = build(10, HLL_4, n3);
    u.update(h2);
    println(u.toString());
    assertEquals(u.getCurMode(), LIST);
    u.update(h3);
    println(u.toString());
    assertEquals(u.getCurMode(), HLL);
    assertEquals(u.getLgConfigK(), 10);
    assertFalse(u.isOutOfOrderFlag());
    double err = sum * errorFactor(u.getLgConfigK(), u.isOutOfOrderFlag(), 2.0);
    println("ErrToll: " + err);
    assertEquals(u.getEstimate(), sum, err);
  }

  @Test
  public void checkCase2B() { //src: HLL, gadget: LIST, swap, cases 0, 2; different lgKs
    int n1 = 5;
    int n2 = 2;
    int n3 = 769;
    int sum = n1 + n2 + n3;
    Union u = buildUnion(12, n1);
    HllSketch h2 = build(11, HLL_8, n2);
    HllSketch h3 = build(13, HLL_4, n3);
    u.update(h2);
    println(u.toString());
    assertEquals(u.getCurMode(), LIST);
    u.update(h3);
    println(u.toString());
    assertEquals(u.getCurMode(), HLL);
    assertEquals(u.getLgConfigK(), 12);
    assertFalse(u.isOutOfOrderFlag());
    double err = sum * errorFactor(u.getLgConfigK(), u.isOutOfOrderFlag(), 2.0);
    println("ErrToll: " + err);
    assertEquals(u.getEstimate(), sum, err);
  }

  @Test
  public void checkCase4() { //src: LIST, gadget: SET, cases 0, 4
    int n1 = 6;
    int n2 = 10;
    int n3 = 6;
    int sum = n1 + n2 + n3;
    Union u = buildUnion(12, n1);
    HllSketch h2 = build(11, HLL_6, n2); //SET
    HllSketch h3 = build(10, HLL_8, n3);
    u.update(h2);
    println(u.toString());
    assertEquals(u.getCurMode(), SET);
    u.update(h3);
    println(u.toString());
    assertEquals(u.getCurMode(), SET);
    assertEquals(u.getLgConfigK(), 12);
    assertTrue(u.isOutOfOrderFlag());
    double err = sum * errorFactor(u.getLgConfigK(), u.isOutOfOrderFlag(), 2.0);
    println("ErrToll: " + err);
    assertEquals(u.getEstimate(), sum, err);
  }

  @Test
  public void checkCase5() { //src: SET, gadget: SET, cases 0, 5
    int n1 = 6;
    int n2 = 10;
    int n3 = 16;
    int sum = n1 + n2 + n3;
    Union u = buildUnion(12, n1);
    HllSketch h2 = build(11, HLL_6, n2);
    HllSketch h3 = build(10, HLL_8, n3);
    u.update(h2);
    println(u.toString());
    assertEquals(u.getCurMode(), SET);
    u.update(h3);
    println(u.toString());
    assertEquals(u.getCurMode(), SET);
    assertEquals(u.getLgConfigK(), 12);
    assertTrue(u.isOutOfOrderFlag());
    double err = sum * errorFactor(u.getLgConfigK(), u.isOutOfOrderFlag(), 2.0);
    println("ErrToll: " + err);
    assertEquals(u.getEstimate(), sum, err);
  }

  @Test
  public void checkCase6() { //src: HLL, gadget: SET, swap, cases 1, 6
    int n1 = 2;
    int n2 = 192;
    int n3 = 97;
    int sum = n1 + n2 + n3;
    Union u = buildUnion(12, n1);
    HllSketch h2 = build(11, HLL_8, n2);
    HllSketch h3 = build(10, HLL_4, n3);
    u.update(h2);
    println(u.toString());
    assertEquals(u.getCurMode(), SET);
    u.update(h3);
    println(u.toString());
    assertEquals(u.getCurMode(), HLL);
    assertEquals(u.getLgConfigK(), 10);
    assertTrue(u.isOutOfOrderFlag());
    double err = sum * errorFactor(u.getLgConfigK(), u.isOutOfOrderFlag(), 2.0);
    println("ErrToll: " + err);
    assertEquals(u.getEstimate(), sum, err);
  }

  @Test
  public void checkCase6B() { //src: HLL, gadget: SET, swap, downsize, cases 1, 6
    int n1 = 6;
    int n2 = 20;
    int n3 = 769;
    int sum = n1 + n2 + n3;
    Union u = buildUnion(12, n1);
    HllSketch h2 = build(11, HLL_8, n2);
    HllSketch h3 = build(13, HLL_4, n3);
    u.update(h2);
    println(u.toString());
    assertEquals(u.getCurMode(), SET);
    u.update(h3);
    println(u.toString());
    assertEquals(u.getCurMode(), HLL);
    assertEquals(u.getLgConfigK(), 12);
    assertTrue(u.isOutOfOrderFlag());
    double err = sum * errorFactor(u.getLgConfigK(), u.isOutOfOrderFlag(), 2.0);
    println("ErrToll: " + err);
    assertEquals(u.getEstimate(), sum, err);
  }

  @Test
  public void checkCase8() { //src: LIST, gadget: HLL, cases 2 (swap), 8
    int n1 = 6;
    int n2 = 193;
    int n3 = 7;
    int sum = n1 + n2 + n3;
    Union u = buildUnion(12, n1); //LIST
    HllSketch h2 = build(11, HLL_6, n2); //HLL
    HllSketch h3 = build(10, HLL_8, n3); //LIST
    u.update(h2); //SET
    println(u.toString());
    assertEquals(u.getCurMode(), HLL);
    u.update(h3);
    println(u.toString());
    assertEquals(u.getCurMode(), HLL);
    assertEquals(u.getLgConfigK(), 11);
    assertFalse(u.isOutOfOrderFlag());
    double err = sum * errorFactor(u.getLgConfigK(), u.isOutOfOrderFlag(), 2.0);
    println("ErrToll: " + err);
    assertEquals(u.getEstimate(), sum, err);
  }

  @Test
  public void checkCase9() { //src: SET, gadget: HLL, cases 2 (swap), 9
    int n1 = 6;
    int n2 = 193;
    int n3 = 16;
    int sum = n1 + n2 + n3;
    Union u = buildUnion(12, n1); //LIST
    HllSketch h2 = build(11, HLL_6, n2); //HLL
    HllSketch h3 = build(10, HLL_8, n3);
    u.update(h2);
    println(u.toString());
    assertEquals(u.getCurMode(), HLL);
    u.update(h3);
    println(u.toString());
    assertEquals(u.getCurMode(), HLL);
    assertEquals(u.getLgConfigK(), 11);
    assertTrue(u.isOutOfOrderFlag());
    double err = sum * errorFactor(u.getLgConfigK(), u.isOutOfOrderFlag(), 2.0);
    println("ErrToll: " + err);
    assertEquals(u.getEstimate(), sum, err);
  }

  @Test
  public void checkCase10() { //src: HLL, gadget: HLL, cases 2 (swap), 10, downsample
    int n1 = 6;
    int n2 = 193;
    int n3 = 97;
    int sum = n1 + n2 + n3;
    Union u = buildUnion(12, n1); //LIST
    HllSketch h2 = build(11, HLL_6, n2); //HLL
    HllSketch h3 = build(10, HLL_8, n3);
    u.update(h2);
    println(u.toString());
    assertEquals(u.getCurMode(), HLL);
    u.update(h3);
    println(u.toString());
    assertEquals(u.getCurMode(), HLL);
    assertEquals(u.getLgConfigK(), 10);
    assertTrue(u.isOutOfOrderFlag());
    double err = sum * errorFactor(u.getLgConfigK(), u.isOutOfOrderFlag(), 2.0);
    println("ErrToll: " + err);
    assertEquals(u.getEstimate(), sum, err);
  }

  @Test
  public void checkCase10B() { //src: HLL, gadget: HLL, cases 2 (swap), 10, copy to HLL_8
    int n1 = 6;
    int n2 = 193;
    int n3 = 193;
    int sum = n1 + n2 + n3;
    Union u = buildUnion(12, n1); //LIST
    HllSketch h2 = build(11, HLL_6, n2); //HLL_6
    HllSketch h3 = build(11, HLL_8, n3);
    u.update(h2);
    println(u.toString());
    assertEquals(u.getCurMode(), HLL);
    u.update(h3);
    println(u.toString());
    assertEquals(u.getCurMode(), HLL);
    assertEquals(u.getLgConfigK(), 11);
    assertTrue(u.isOutOfOrderFlag());
    double err = sum * errorFactor(u.getLgConfigK(), u.isOutOfOrderFlag(), 2.0);
    println("ErrToll: " + err);
    assertEquals(u.getEstimate(), sum, err);
  }

  @Test
  public void checkCase12() { //src: LIST, gadget: empty, case 12
    int n1 = 0;
    int n2 = 0;
    int n3 = 7;
    int sum = n1 + n2 + n3;
    Union u = buildUnion(12, n1);   //LIST empty
    HllSketch h2 = build(11, HLL_6, n2);   //LIST empty, ignored
    HllSketch h3 = build(10, HLL_8, n3);   //Src LIST
    u.update(h2);
    println(u.toString());
    assertEquals(u.getCurMode(), LIST);
    u.update(h3);
    println(u.toString());
    assertEquals(u.getCurMode(), LIST);
    assertEquals(u.getLgConfigK(), 12);
    assertFalse(u.isOutOfOrderFlag());
    double err = sum * errorFactor(u.getLgConfigK(), u.isOutOfOrderFlag(), 2.0);
    println("ErrToll: " + err);
    assertEquals(u.getEstimate(), sum, err);
  }

  @Test
  public void checkCase13() { //src: SET, gadget: empty, case 13
    int n1 = 0;
    int n2 = 0;
    int n3 = 16;
    int sum = n1 + n2 + n3;
    Union u = buildUnion(12, n1);        //LIST empty
    HllSketch h2 = build(11, HLL_6, n2);   //LIST empty, ignored
    HllSketch h3 = build(10, HLL_8, n3);   // Src Set
    u.update(h2);
    println(u.toString());
    assertEquals(u.getCurMode(), LIST);
    u.update(h3);
    println(u.toString());
    assertEquals(u.getCurMode(), SET);
    assertEquals(u.getLgConfigK(), 12);
    assertTrue(u.isOutOfOrderFlag());
    double err = sum * errorFactor(u.getLgConfigK(), u.isOutOfOrderFlag(), 2.0);
    println("ErrToll: " + err);
    assertEquals(u.getEstimate(), sum, err);
  }

  @Test
  public void checkCase14() { //src: HLL, gadget: empty, case 14
    int n1 = 0;
    int n2 = 0;
    int n3 = 97;
    int sum = n1 + n2 + n3;
    Union u = buildUnion(12, n1);        //LIST empty
    HllSketch h2 = build(11, HLL_6, n2);   //LIST empty
    HllSketch h3 = build(10, HLL_8, n3);   // Src HLL
    u.update(h2);
    println(u.toString());
    assertEquals(u.getCurMode(), LIST);
    u.update(h3);
    println(u.toString());
    assertEquals(u.getCurMode(), HLL);
    assertEquals(u.getLgConfigK(), 10);
    assertFalse(u.isOutOfOrderFlag());
    double err = sum * errorFactor(u.getLgConfigK(), u.isOutOfOrderFlag(), 2.0);
    println("ErrToll: " + err);
    assertEquals(u.getEstimate(), sum, err);
  }

  @Test
  public void checkCase14B() { //src: HLL, gadget: empty, case 14, downsize
    int n1 = 0;
    int n2 = 0;
    int n3 = 385;
    int sum = n1 + n2 + n3;
    Union u = buildUnion(12, n1);        //LIST empty
    HllSketch h2 = build(11, HLL_6, n2);   //LIST empty
    HllSketch h3 = build(12, HLL_8, n3);
    u.update(h2);
    println(u.toString());
    assertEquals(u.getCurMode(), LIST);
    u.update(h3);
    println(u.toString());
    assertEquals(u.getCurMode(), HLL);
    assertEquals(u.getLgConfigK(), 12);
    assertFalse(u.isOutOfOrderFlag());
    double err = sum * errorFactor(u.getLgConfigK(), u.isOutOfOrderFlag(), 2.0);
    println("ErrToll: " + err);
    assertEquals(u.getEstimate(), sum, err);
  }

  @Test
  public void checkMisc() {
    Union u = buildUnion(12, 0);
    int bytes = u.getCompactSerializationBytes();
    assertEquals(bytes, 8);
    bytes = Union.getMaxSerializationBytes(7);
    assertEquals(bytes, 40 + 128);
    double v = u.getEstimate();
    assertEquals(v, 0.0, 0.0);
    v = u.getLowerBound(1);
    assertEquals(v, 0.0, 0.0);
    v = u.getUpperBound(1);
    assertEquals(v, 0.0, 0.0);
    assertTrue(u.isEmpty());
    u.reset();
    assertTrue(u.isEmpty());
    println(u.toString(true, false, false, false));
    byte[] bArr = u.toCompactByteArray();
    assertEquals(bArr.length, 8);
  }

  private static double errorFactor(int lgK, boolean oooFlag, double numStdDev) {
    double f;
    if (oooFlag) {
      f = (1.2 * numStdDev) / Math.sqrt(1 << lgK);
    } else {
      f = (0.9 * numStdDev) / Math.sqrt(1 << lgK);
    }
    return f;
  }

  private Union buildUnion(int lgMaxK, int n) {
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgMaxK, TgtHllType.HLL_8);
    WritableMemory wmem = WritableMemory.allocate(bytes);
    Union u = new Union(lgMaxK, wmem);
    for (int i = 0; i < n; i++) { u.update(i + v); }
    v += n;
    return u;
  }

  private HllSketch build(int lgK, TgtHllType tgtHllType, int n) {
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK, tgtHllType);
    WritableMemory wmem = WritableMemory.allocate(bytes);
    HllSketch sk = new HllSketch(lgK, tgtHllType, wmem);
    for (int i = 0; i < n; i++) { sk.update(i + v); }
    v += n;
    return sk;
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
