/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.hll;

import static org.apache.datasketches.hll.CurMode.LIST;
import static org.apache.datasketches.hll.CurMode.SET;
import static org.apache.datasketches.hll.HllUtil.HLL_HIP_RSE_FACTOR;
import static org.apache.datasketches.hll.HllUtil.HLL_NON_HIP_RSE_FACTOR;
import static org.apache.datasketches.hll.TgtHllType.HLL_4;
import static org.apache.datasketches.hll.TgtHllType.HLL_6;
import static org.apache.datasketches.hll.TgtHllType.HLL_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.datasketches.SketchesStateException;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class UnionCaseTest {
  private static final String LS = System.getProperty("line.separator");
  long v = 0;
  final static int maxLgK = 12;
  HllSketch source;
  //Union union;
  String hfmt = "%10s%10s%10s%10s%10s%10s%10s%10s%10s%10s%10s" + LS;
  String hdr = String.format(hfmt, "caseNum","srcLgKStr","gdtLgKStr","srcType","gdtType",
      "srcMem","gdtMem","srcMode","gdtMode","srcOoof","gdtOoof");

  @Test
  public void checkAllCases() {
    print(hdr);
    for (int i = 0; i < 24; i++) {
      checkCase(i, HLL_4, false);
    }
    println("");

    print(hdr);
    for (int i = 0; i < 24; i++) {
      checkCase(i, HLL_6, false);
    }
    println("");

    print(hdr);
    for (int i = 0; i < 24; i++) {
      checkCase(i, HLL_8, false);
    }
    println("");

    print(hdr);
    for (int i = 0; i < 24; i++) {
      checkCase(i, HLL_4, true);
    }
    println("");

    print(hdr);
    for (int i = 0; i < 24; i++) {
      checkCase(i, HLL_6, true);
    }
    println("");

    print(hdr);
    for (int i = 0; i < 24; i++) {
      checkCase(i, HLL_8, true);
    }
    println("");
  }

  private void checkCase(int caseNum, TgtHllType srcType, boolean srcMem) {
    source = getSource(caseNum, srcType, srcMem);
    boolean gdtMem = (caseNum & 1) > 0;
    Union union = getUnion(caseNum, gdtMem);
    union.update(source);
    int totalU = getSrcCount(caseNum, maxLgK) + getUnionCount(caseNum);
    output(caseNum, source, union, totalU);
  }

  private void output(int caseNum, HllSketch source, Union union, int totalU) {
    double estU = union.getEstimate();
    double err = Math.abs((estU / totalU) - 1.0);
    int gdtLgK = union.getLgConfigK();
    boolean uooof = union.isOutOfOrder();
    double rseFactor = (uooof) ? HLL_NON_HIP_RSE_FACTOR : HLL_HIP_RSE_FACTOR;
    double rse = (rseFactor * 3) / Math.sqrt(1 << gdtLgK); //99.7% conf

    //output other parameters
    String caseNumStr = Integer.toString(caseNum);
    String srcLgKStr = Integer.toString(source.getLgConfigK());
    String gdtLgKStr = Integer.toString(union.getLgConfigK());
    String srcType = source.getTgtHllType().toString();
    String gdtType = union.getTgtHllType().toString();
    String srcMem = Boolean.toString(source.isMemory());
    String gdtMem = Boolean.toString(union.isMemory());
    String srcMode = source.getCurMode().toString();
    String gdtMode = union.getCurMode().toString();
    String srcOoof = Boolean.toString(source.isOutOfOrder());
    String gdtOoof = Boolean.toString(union.isOutOfOrder());
    printf(hfmt, caseNumStr, srcLgKStr, gdtLgKStr, srcType, gdtType, srcMem, gdtMem,
        srcMode, gdtMode, srcOoof, gdtOoof);
    assertTrue(err < rse, "Err: " + err + ", RSE: " + rse);
  }

  private HllSketch getSource(int caseNum, TgtHllType tgtHllType, boolean memory) {
    int srcLgK = getSrcLgK(caseNum, maxLgK);
    int srcU = getSrcCount(caseNum, maxLgK);
    if (memory) {
      return buildMemorySketch(srcLgK, tgtHllType, srcU);
    } else {
      return buildHeapSketch(srcLgK, tgtHllType, srcU);
    }
  }

  private Union getUnion(int caseNum, boolean memory) {
    int unionU = getUnionCount(caseNum);
    return (memory) ? buildMemoryUnion(maxLgK, unionU) : buildHeapUnion(maxLgK, unionU);
  }

  private static int getUnionCount(int caseNum) {
    int gdtMode = (caseNum >> 1) & 3; //list, set, hll, empty
    return (gdtMode == 0) ? 4 : (gdtMode == 1) ? 380 : (gdtMode == 2) ? 400 : 0;
  }

  private static int getSrcCount(int caseNum, int maxLgK) {
    int srcLgK = getSrcLgK(caseNum, maxLgK);
    return (((1 << srcLgK) * 3) / 4) + 100; //always HLL
  }

  private static int getSrcLgK(int caseNum, int maxLgK) {
    int srcLgK = maxLgK;
    int bits34 = (caseNum >> 3) & 3;
    if (bits34 == 1) { srcLgK = maxLgK - 1;}
    if (bits34 == 2) { srcLgK = maxLgK + 1;}
    return srcLgK;
  }

  @Test
  public void checkMisc() {
    Union u = buildHeapUnion(12, 0);
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

  @Test
  public void checkSrcListList() { //src: LIST, gadget: LIST
    int n1 = 2;
    int n2 = 3;
    int n3 = 2;
    int sum = n1 + n2 + n3;
    Union u = buildHeapUnion(12, n1); //gdt = list
    HllSketch h2 = buildHeapSketch(11, HLL_6, n2); //src = list
    HllSketch h3 = buildHeapSketch(10, HLL_8, n3); //src = list
    u.update(h2);
    println(u.toString());
    assertEquals(u.getCurMode(), LIST);
    u.update(h3);
    println(u.toString());
    assertEquals(u.getCurMode(), LIST);
    assertEquals(u.getLgConfigK(), 12);
    assertFalse(u.isOutOfOrder());
    double err = sum * errorFactor(u.getLgConfigK(), u.isOutOfOrder(), 3.0);
    println("ErrToll: " + err);
    assertEquals(u.getEstimate(), sum, err);
  }

  @Test
  public void checkSrcListSet() { //src: SET, gadget: LIST
    int n1 = 5;
    int n2 = 2;
    int n3 = 16;
    int sum = n1 + n2 + n3;
    Union u = buildHeapUnion(12, n1);        //LIST, 5
    HllSketch h2 = buildHeapSketch(11, HLL_6, n2); //LIST, 2
    HllSketch h3 = buildHeapSketch(10, HLL_8, n3); //SET, 16
    u.update(h2);
    println(u.toString());
    assertEquals(u.getCurMode(), LIST);
    u.update(h3);
    println(u.toString());
    assertEquals(u.getCurMode(), SET);
    assertEquals(u.getLgConfigK(), 12);
    assertFalse(u.isOutOfOrder());
    double err = sum * errorFactor(u.getLgConfigK(), u.isOutOfOrder(), 3.0);
    println("ErrToll: " + err);
    assertEquals(u.getEstimate(), sum, err);
  }

  @Test
  public void checkSrcSetList() { //src: LIST, gadget: SET
    int n1 = 6;
    int n2 = 10;
    int n3 = 6;
    int sum = n1 + n2 + n3;
    Union u = buildHeapUnion(12, n1);
    HllSketch h2 = buildHeapSketch(11, HLL_6, n2); //SET
    HllSketch h3 = buildHeapSketch(10, HLL_8, n3); //LIST
    u.update(h2);
    println(u.toString());
    assertEquals(u.getCurMode(), SET);
    u.update(h3);
    println(u.toString());
    assertEquals(u.getCurMode(), SET);
    assertEquals(u.getLgConfigK(), 12);
    assertFalse(u.isOutOfOrder());
    double err = sum * errorFactor(u.getLgConfigK(), u.isOutOfOrder(), 3.0);
    println("ErrToll: " + err);
    assertEquals(u.getEstimate(), sum, err);
  }

  @Test
  public void checkSrcSetSet() { //src: SET, gadget: SET
    int n1 = 6;
    int n2 = 10;
    int n3 = 16;
    int sum = n1 + n2 + n3;
    Union u = buildHeapUnion(12, n1);
    HllSketch h2 = buildHeapSketch(11, HLL_6, n2); //src: SET
    HllSketch h3 = buildHeapSketch(10, HLL_8, n3); //src: SET
    u.update(h2);
    println(u.toString());
    assertEquals(u.getCurMode(), SET);
    u.update(h3);
    println(u.toString());
    assertEquals(u.getCurMode(), SET);
    assertEquals(u.getLgConfigK(), 12);
    assertFalse(u.isOutOfOrder());
    double err = sum * errorFactor(u.getLgConfigK(), u.isOutOfOrder(), 3.0);
    println("ErrToll: " + err);
    assertEquals(u.getEstimate(), sum, err);
  }

  @Test
  public void checkSrcEmptyList() { //src: LIST, gadget: empty
    int n1 = 0;
    int n2 = 0;
    int n3 = 7;
    int sum = n1 + n2 + n3;
    Union u = buildHeapUnion(12, n1);   //LIST empty
    HllSketch h2 = buildHeapSketch(11, HLL_6, n2);   //src: LIST empty, ignored
    HllSketch h3 = buildHeapSketch(10, HLL_8, n3);   //src: LIST
    u.update(h2);
    println(u.toString());
    assertEquals(u.getCurMode(), LIST);
    u.update(h3);
    println(u.toString());
    assertEquals(u.getCurMode(), LIST);
    assertEquals(u.getLgConfigK(), 12);
    assertFalse(u.isOutOfOrder());
    double err = sum * errorFactor(u.getLgConfigK(), u.isOutOfOrder(), 3.0);
    println("ErrToll: " + err);
    assertEquals(u.getEstimate(), sum, err);
  }

  @Test
  public void checkSrcEmptySet() {
    int n1 = 0;
    int n2 = 0;
    int n3 = 16;
    int sum = n1 + n2 + n3;
    Union u = buildHeapUnion(12, n1);        //LIST empty
    HllSketch h2 = buildHeapSketch(11, HLL_6, n2);   //LIST empty, ignored
    HllSketch h3 = buildHeapSketch(10, HLL_8, n3);   // Src Set
    u.update(h2);
    println(u.toString());
    assertEquals(u.getCurMode(), LIST);
    u.update(h3);
    println(u.toString());
    assertEquals(u.getCurMode(), SET);
    assertEquals(u.getLgConfigK(), 12);
    assertFalse(u.isOutOfOrder());
    double err = sum * errorFactor(u.getLgConfigK(), u.isOutOfOrder(), 3.0);
    println("ErrToll: " + err);
    assertEquals(u.getEstimate(), sum, err);
  }

  @SuppressWarnings("unused")
  @Test
  public void checkSpecialMergeCase4() {
    Union u = buildHeapUnion(12, 1 << 9);
    HllSketch sk = buildHeapSketch(12, HLL_8, 1 << 9);

    u.update(sk);
    assertTrue(u.isRebuildCurMinNumKxQFlag());
    u.getCompositeEstimate();
    assertFalse(u.isRebuildCurMinNumKxQFlag());

    u.update(sk);
    assertTrue(u.isRebuildCurMinNumKxQFlag());
    u.getLowerBound(2);
    assertFalse(u.isRebuildCurMinNumKxQFlag());

    u.update(sk);
    assertTrue(u.isRebuildCurMinNumKxQFlag());
    u.getUpperBound(2);
    assertFalse(u.isRebuildCurMinNumKxQFlag());

    u.update(sk);
    assertTrue(u.isRebuildCurMinNumKxQFlag());
    u.getResult();
    assertFalse(u.isRebuildCurMinNumKxQFlag());

    u.update(sk);
    assertTrue(u.isRebuildCurMinNumKxQFlag());
    byte[] ba = u.toCompactByteArray();
    assertFalse(u.isRebuildCurMinNumKxQFlag());

    u.update(sk);
    assertTrue(u.isRebuildCurMinNumKxQFlag());
    ba = u.toUpdatableByteArray();
    assertFalse(u.isRebuildCurMinNumKxQFlag());

    u.putRebuildCurMinNumKxQFlag(true);
    assertTrue(u.isRebuildCurMinNumKxQFlag());
    u.putRebuildCurMinNumKxQFlag(false);
    assertFalse(u.isRebuildCurMinNumKxQFlag());
  }

  @Test
  public void checkRebuildCurMinNumKxQFlag1() {
    HllSketch sk = buildHeapSketch(4, HLL_8, 16);
    HllArray hllArr = (HllArray)(sk.hllSketchImpl);
    hllArr.putRebuildCurMinNumKxQFlag(true); //corrupt the flag
    Union union = buildHeapUnion(4, 0);
    union.update(sk);
  }

  @Test
  public void checkRebuildCurMinNumKxQFlag2() {
    HllSketch sk = buildMemorySketch(4, HLL_8, 16);
    DirectHllArray hllArr = (DirectHllArray)(sk.hllSketchImpl);
    hllArr.putRebuildCurMinNumKxQFlag(true); //corrupt the flag
    WritableMemory wmem = sk.getWritableMemory();
    Union.writableWrap(wmem);
  }

  @Test(expectedExceptions = SketchesStateException.class)
  public void checkHllMergeToException() {
    HllSketch src = buildHeapSketch(4, HLL_8, 16);
    HllSketch tgt = buildHeapSketch(4, HLL_8, 16);
    AbstractHllArray absHllArr = (AbstractHllArray)(src.hllSketchImpl);
    absHllArr.mergeTo(tgt);
  }


  private static double errorFactor(int lgK, boolean oooFlag, double numStdDev) {
    double f;
    if (oooFlag) {
      f = (1.04 * numStdDev) / Math.sqrt(1 << lgK);
    } else {
      f = (0.9 * numStdDev) / Math.sqrt(1 << lgK);
    }
    return f;
  }

  //BUILDERS
  private Union buildHeapUnion(int lgMaxK, int n) {
    Union u = new Union(lgMaxK);
    for (int i = 0; i < n; i++) { u.update(i + v); }
    v += n;
    return u;
  }

  private Union buildMemoryUnion(int lgMaxK, int n) {
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgMaxK, TgtHllType.HLL_8);
    WritableMemory wmem = WritableMemory.allocate(bytes);
    Union u = new Union(lgMaxK, wmem);
    for (int i = 0; i < n; i++) { u.update(i + v); }
    v += n;
    return u;
  }

  private HllSketch buildHeapSketch(int lgK, TgtHllType tgtHllType, int n) {
    HllSketch sk = new HllSketch(lgK, tgtHllType);
    for (int i = 0; i < n; i++) { sk.update(i + v); }
    v += n;
    return sk;
  }

  private HllSketch buildMemorySketch(int lgK, TgtHllType tgtHllType, int n) {
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK,tgtHllType);
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
   * @param o value to print
   */
  static void println(Object o) {
    print(o.toString() + "\n");
  }

  /**
   * @param o value to print
   */
  static void print(Object o) {
    //System.out.print(o.toString()); //disable here
  }

  /**
   * @param fmt format
   * @param args arguments
   */
  static void printf(String fmt, Object...args) {
    //System.out.printf(fmt, args); //disable here
  }

}
