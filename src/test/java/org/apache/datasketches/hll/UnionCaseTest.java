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

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.SketchesStateException;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class UnionCaseTest {
  private static final String LS = System.getProperty("line.separator");
  long v = 0;
  final static int maxLgK = 12;
  HllSketch source;
  //HllUnion union;
  String hfmt = "%10s%10s%10s%10s%10s%10s%10s%10s%10s%10s%10s" + LS;
  String hdr = String.format(hfmt, "caseNum","srcLgKStr","gdtLgKStr","srcType","gdtType",
      "srcSeg","gdtSeg","srcMode","gdtMode","srcOoof","gdtOoof");

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

  private void checkCase(final int caseNum, final TgtHllType srcType, final boolean srcSeg) {
    source = getSource(caseNum, srcType, srcSeg);
    final boolean gdtSeg = (caseNum & 1) > 0;
    final HllUnion union = getUnion(caseNum, gdtSeg);
    union.update(source);
    final int totalU = getSrcCount(caseNum, maxLgK) + getUnionCount(caseNum);
    output(caseNum, source, union, totalU);
  }

  private void output(final int caseNum, final HllSketch source, final HllUnion union, final int totalU) {
    final double estU = union.getEstimate();
    final double err = Math.abs((estU / totalU) - 1.0);
    final int gdtLgK = union.getLgConfigK();
    final boolean uooof = union.isOutOfOrder();
    final double rseFactor = (uooof) ? HLL_NON_HIP_RSE_FACTOR : HLL_HIP_RSE_FACTOR;
    final double rse = (rseFactor * 3) / Math.sqrt(1 << gdtLgK); //99.7% conf

    //output other parameters
    final String caseNumStr = Integer.toString(caseNum);
    final String srcLgKStr = Integer.toString(source.getLgConfigK());
    final String gdtLgKStr = Integer.toString(union.getLgConfigK());
    final String srcType = source.getTgtHllType().toString();
    final String gdtType = union.getTgtHllType().toString();
    final String srcSeg = Boolean.toString(source.hasMemorySegment());
    final String gdtSeg = Boolean.toString(union.hasMemorySegment());
    final String srcMode = source.getCurMode().toString();
    final String gdtMode = union.getCurMode().toString();
    final String srcOoof = Boolean.toString(source.isOutOfOrder());
    final String gdtOoof = Boolean.toString(union.isOutOfOrder());
    printf(hfmt, caseNumStr, srcLgKStr, gdtLgKStr, srcType, gdtType, srcSeg, gdtSeg,
        srcMode, gdtMode, srcOoof, gdtOoof);
    assertTrue(err < rse, "Err: " + err + ", RSE: " + rse);
  }

  private HllSketch getSource(final int caseNum, final TgtHllType tgtHllType, final boolean useMemorySegment) {
    final int srcLgK = getSrcLgK(caseNum, maxLgK);
    final int srcU = getSrcCount(caseNum, maxLgK);
    if (useMemorySegment) {
      return buildMemorySegmentSketch(srcLgK, tgtHllType, srcU);
    } else {
      return buildHeapSketch(srcLgK, tgtHllType, srcU);
    }
  }

  private HllUnion getUnion(final int caseNum, final boolean useMemorySegment) {
    final int unionU = getUnionCount(caseNum);
    return (useMemorySegment) ? buildMemorSegmentUnion(maxLgK, unionU) : buildHeapUnion(maxLgK, unionU);
  }

  private static int getUnionCount(final int caseNum) {
    final int gdtMode = (caseNum >> 1) & 3; //list, set, hll, empty
    return (gdtMode == 0) ? 4 : (gdtMode == 1) ? 380 : (gdtMode == 2) ? 400 : 0;
  }

  private static int getSrcCount(final int caseNum, final int maxLgK) {
    final int srcLgK = getSrcLgK(caseNum, maxLgK);
    return (((1 << srcLgK) * 3) / 4) + 100; //always HLL
  }

  private static int getSrcLgK(final int caseNum, final int maxLgK) {
    int srcLgK = maxLgK;
    final int bits34 = (caseNum >> 3) & 3;
    if (bits34 == 1) { srcLgK = maxLgK - 1;}
    if (bits34 == 2) { srcLgK = maxLgK + 1;}
    return srcLgK;
  }

  @Test
  public void checkMisc() {
    final HllUnion u = buildHeapUnion(12, 0);
    int bytes = u.getCompactSerializationBytes();
    assertEquals(bytes, 8);
    bytes = HllUnion.getMaxSerializationBytes(7);
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
    final byte[] bArr = u.toCompactByteArray();
    assertEquals(bArr.length, 8);
  }

  @Test
  public void checkSrcListList() { //src: LIST, gadget: LIST
    final int n1 = 2;
    final int n2 = 3;
    final int n3 = 2;
    final int sum = n1 + n2 + n3;
    final HllUnion u = buildHeapUnion(12, n1); //gdt = list
    final HllSketch h2 = buildHeapSketch(11, HLL_6, n2); //src = list
    final HllSketch h3 = buildHeapSketch(10, HLL_8, n3); //src = list
    u.update(h2);
    println(u.toString());
    assertEquals(u.getCurMode(), LIST);
    u.update(h3);
    println(u.toString());
    assertEquals(u.getCurMode(), LIST);
    assertEquals(u.getLgConfigK(), 12);
    assertFalse(u.isOutOfOrder());
    final double err = sum * errorFactor(u.getLgConfigK(), u.isOutOfOrder(), 3.0);
    println("ErrToll: " + err);
    assertEquals(u.getEstimate(), sum, err);
  }

  @Test
  public void checkSrcListSet() { //src: SET, gadget: LIST
    final int n1 = 5;
    final int n2 = 2;
    final int n3 = 16;
    final int sum = n1 + n2 + n3;
    final HllUnion u = buildHeapUnion(12, n1);        //LIST, 5
    final HllSketch h2 = buildHeapSketch(11, HLL_6, n2); //LIST, 2
    final HllSketch h3 = buildHeapSketch(10, HLL_8, n3); //SET, 16
    u.update(h2);
    println(u.toString());
    assertEquals(u.getCurMode(), LIST);
    u.update(h3);
    println(u.toString());
    assertEquals(u.getCurMode(), SET);
    assertEquals(u.getLgConfigK(), 12);
    assertFalse(u.isOutOfOrder());
    final double err = sum * errorFactor(u.getLgConfigK(), u.isOutOfOrder(), 3.0);
    println("ErrToll: " + err);
    assertEquals(u.getEstimate(), sum, err);
  }

  @Test
  public void checkSrcSetList() { //src: LIST, gadget: SET
    final int n1 = 6;
    final int n2 = 10;
    final int n3 = 6;
    final int sum = n1 + n2 + n3;
    final HllUnion u = buildHeapUnion(12, n1);
    final HllSketch h2 = buildHeapSketch(11, HLL_6, n2); //SET
    final HllSketch h3 = buildHeapSketch(10, HLL_8, n3); //LIST
    u.update(h2);
    println(u.toString());
    assertEquals(u.getCurMode(), SET);
    u.update(h3);
    println(u.toString());
    assertEquals(u.getCurMode(), SET);
    assertEquals(u.getLgConfigK(), 12);
    assertFalse(u.isOutOfOrder());
    final double err = sum * errorFactor(u.getLgConfigK(), u.isOutOfOrder(), 3.0);
    println("ErrToll: " + err);
    assertEquals(u.getEstimate(), sum, err);
  }

  @Test
  public void checkSrcSetSet() { //src: SET, gadget: SET
    final int n1 = 6;
    final int n2 = 10;
    final int n3 = 16;
    final int sum = n1 + n2 + n3;
    final HllUnion u = buildHeapUnion(12, n1);
    final HllSketch h2 = buildHeapSketch(11, HLL_6, n2); //src: SET
    final HllSketch h3 = buildHeapSketch(10, HLL_8, n3); //src: SET
    u.update(h2);
    println(u.toString());
    assertEquals(u.getCurMode(), SET);
    u.update(h3);
    println(u.toString());
    assertEquals(u.getCurMode(), SET);
    assertEquals(u.getLgConfigK(), 12);
    assertFalse(u.isOutOfOrder());
    final double err = sum * errorFactor(u.getLgConfigK(), u.isOutOfOrder(), 3.0);
    println("ErrToll: " + err);
    assertEquals(u.getEstimate(), sum, err);
  }

  @Test
  public void checkSrcEmptyList() { //src: LIST, gadget: empty
    final int n1 = 0;
    final int n2 = 0;
    final int n3 = 7;
    final int sum = n1 + n2 + n3;
    final HllUnion u = buildHeapUnion(12, n1);   //LIST empty
    final HllSketch h2 = buildHeapSketch(11, HLL_6, n2);   //src: LIST empty, ignored
    final HllSketch h3 = buildHeapSketch(10, HLL_8, n3);   //src: LIST
    u.update(h2);
    println(u.toString());
    assertEquals(u.getCurMode(), LIST);
    u.update(h3);
    println(u.toString());
    assertEquals(u.getCurMode(), LIST);
    assertEquals(u.getLgConfigK(), 12);
    assertFalse(u.isOutOfOrder());
    final double err = sum * errorFactor(u.getLgConfigK(), u.isOutOfOrder(), 3.0);
    println("ErrToll: " + err);
    assertEquals(u.getEstimate(), sum, err);
  }

  @Test
  public void checkSrcEmptySet() {
    final int n1 = 0;
    final int n2 = 0;
    final int n3 = 16;
    final int sum = n1 + n2 + n3;
    final HllUnion u = buildHeapUnion(12, n1);        //LIST empty
    final HllSketch h2 = buildHeapSketch(11, HLL_6, n2);   //LIST empty, ignored
    final HllSketch h3 = buildHeapSketch(10, HLL_8, n3);   // Src Set
    u.update(h2);
    println(u.toString());
    assertEquals(u.getCurMode(), LIST);
    u.update(h3);
    println(u.toString());
    assertEquals(u.getCurMode(), SET);
    assertEquals(u.getLgConfigK(), 12);
    assertFalse(u.isOutOfOrder());
    final double err = sum * errorFactor(u.getLgConfigK(), u.isOutOfOrder(), 3.0);
    println("ErrToll: " + err);
    assertEquals(u.getEstimate(), sum, err);
  }

  @SuppressWarnings("unused")
  @Test
  public void checkSpecialMergeCase4() {
    final HllUnion u = buildHeapUnion(12, 1 << 9);
    final HllSketch sk = buildHeapSketch(12, HLL_8, 1 << 9);

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
    final HllSketch sk = buildHeapSketch(4, HLL_8, 16);
    final HllArray hllArr = (HllArray)(sk.hllSketchImpl);
    hllArr.putRebuildCurMinNumKxQFlag(true); //corrupt the flag
    final HllUnion union = buildHeapUnion(4, 0);
    union.update(sk);
  }

  @Test
  public void checkRebuildCurMinNumKxQFlag2() {
    final HllSketch sk = buildMemorySegmentSketch(4, HLL_8, 16);
    final DirectHllArray hllArr = (DirectHllArray)(sk.hllSketchImpl);
    hllArr.putRebuildCurMinNumKxQFlag(true); //corrupt the flag
    final MemorySegment wseg = sk.getMemorySegment();
    HllUnion.writableWrap(wseg);
  }

  @Test(expectedExceptions = SketchesStateException.class)
  public void checkHllMergeToException() {
    final HllSketch src = buildHeapSketch(4, HLL_8, 16);
    final HllSketch tgt = buildHeapSketch(4, HLL_8, 16);
    final AbstractHllArray absHllArr = (AbstractHllArray)(src.hllSketchImpl);
    absHllArr.mergeTo(tgt);
  }


  private static double errorFactor(final int lgK, final boolean oooFlag, final double numStdDev) {
    double f;
    if (oooFlag) {
      f = (1.04 * numStdDev) / Math.sqrt(1 << lgK);
    } else {
      f = (0.9 * numStdDev) / Math.sqrt(1 << lgK);
    }
    return f;
  }

  //BUILDERS
  private HllUnion buildHeapUnion(final int lgMaxK, final int n) {
    final HllUnion u = new HllUnion(lgMaxK);
    for (int i = 0; i < n; i++) { u.update(i + v); }
    v += n;
    return u;
  }

  private HllUnion buildMemorSegmentUnion(final int lgMaxK, final int n) {
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgMaxK, TgtHllType.HLL_8);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    final HllUnion u = new HllUnion(lgMaxK, wseg);
    for (int i = 0; i < n; i++) { u.update(i + v); }
    v += n;
    return u;
  }

  private HllSketch buildHeapSketch(final int lgK, final TgtHllType tgtHllType, final int n) {
    final HllSketch sk = new HllSketch(lgK, tgtHllType);
    for (int i = 0; i < n; i++) { sk.update(i + v); }
    v += n;
    return sk;
  }

  private HllSketch buildMemorySegmentSketch(final int lgK, final TgtHllType tgtHllType, final int n) {
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK,tgtHllType);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    final HllSketch sk = new HllSketch(lgK, tgtHllType, wseg);
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
  static void println(final Object o) {
    print(o.toString() + "\n");
  }

  /**
   * @param o value to print
   */
  static void print(final Object o) {
    //System.out.print(o.toString()); //disable here
  }

  /**
   * @param fmt format
   * @param args arguments
   */
  static void printf(final String fmt, final Object...args) {
    //System.out.printf(fmt, args); //disable here
  }

}
