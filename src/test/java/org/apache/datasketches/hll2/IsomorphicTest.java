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

package org.apache.datasketches.hll2;

import static org.apache.datasketches.hll2.CurMode.HLL;
import static org.apache.datasketches.hll2.CurMode.LIST;
import static org.apache.datasketches.hll2.CurMode.SET;
import static org.apache.datasketches.hll2.TgtHllType.HLL_4;
import static org.apache.datasketches.hll2.TgtHllType.HLL_6;
import static org.apache.datasketches.hll2.TgtHllType.HLL_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("unused")
public class IsomorphicTest {
  long v = 0;

  @Test
  //Merges a type1 to an empty union (heap, HLL_8), and gets result as type1, checks binary equivalence
  public void isomorphicUnionUpdatableHeap() {
    for (int lgK = 4; lgK <= 21; lgK++) { //All LgK
      for (int cm = 0; cm <= 2; cm++) { //List, Set, Hll
        if ((lgK < 8) && (cm == 1)) { continue; } //lgk < 8 list transistions directly to HLL
        final CurMode curMode = CurMode.fromOrdinal(cm);
        for (int t = 0; t <= 2; t++) { //HLL_4, HLL_6, HLL_8
          final TgtHllType tgtHllType1 = TgtHllType.fromOrdinal(t);
          final HllSketch sk1 = buildHeapSketch(lgK, tgtHllType1, curMode);
          final byte[] sk1bytes = sk1.toUpdatableByteArray(); //UPDATABLE
          final Union union = buildHeapUnion(lgK, null); //UNION
          union.update(sk1);
          final HllSketch sk2 = union.getResult(tgtHllType1);
          final byte[] sk2bytes = sk2.toUpdatableByteArray(); //UPDATABLE
          final String comb = "LgK=" + lgK + ", CurMode=" + curMode.toString() + ", Type:" + tgtHllType1;
          checkArrays(sk1bytes, sk2bytes, comb, false);
        }
      }
    }
  }

  @Test
  //Merges a type1 to an empty union (heap, HLL_8), and gets result as type1, checks binary equivalence
  public void isomorphicUnionCompactHeap() {
    for (int lgK = 4; lgK <= 21; lgK++) { //All LgK
      for (int cm = 0; cm <= 2; cm++) { //List, Set, Hll
        if ((lgK < 8) && (cm == 1)) { continue; } //lgk < 8 list transistions directly to HLL
        final CurMode curMode = CurMode.fromOrdinal(cm);
        for (int t = 0; t <= 2; t++) { //HLL_4, HLL_6, HLL_8
          final TgtHllType tgtHllType1 = TgtHllType.fromOrdinal(t);
          final HllSketch sk1 = buildHeapSketch(lgK, tgtHllType1, curMode);
          final byte[] sk1bytes = sk1.toCompactByteArray(); //COMPACT
          final Union union = buildHeapUnion(lgK, null); //UNION
          union.update(sk1);
          final HllSketch sk2 = union.getResult(tgtHllType1);
          final byte[] sk2bytes = sk2.toCompactByteArray(); //COMPACT
          final String comb = "LgK=" + lgK + ", CurMode=" + curMode.toString() + ", Type:" + tgtHllType1;
          checkArrays(sk1bytes, sk2bytes, comb, false);
        }
      }
    }
  }

  @Test
  //Converts a type1 to a different type and converts back to type1 to check binary equivalence.
  public void isomorphicCopyAsUpdatableHeap() {
    for (int lgK = 4; lgK <= 21; lgK++) { //All LgK
      for (int cm = 0; cm <= 2; cm++) { //List, Set, Hll
        if ((lgK < 8) && (cm == 1)) { continue; } //lgk < 8 list transistions directly to HLL
        final CurMode curMode = CurMode.fromOrdinal(cm);
        for (int t1 = 0; t1 <= 2; t1++) { //HLL_4, HLL_6, HLL_8
          final TgtHllType tgtHllType1 = TgtHllType.fromOrdinal(t1);
          final HllSketch sk1 = buildHeapSketch(lgK, tgtHllType1, curMode);
          final byte[] sk1bytes = sk1.toUpdatableByteArray(); //UPDATABLE
          for (int t2 = 0; t2 <= 2; t2++) { //HLL_4, HLL_6, HLL_8
            if (t2 == t1) { continue; }
            final TgtHllType tgtHllType2 = TgtHllType.fromOrdinal(t2);
            final HllSketch sk2 = sk1.copyAs(tgtHllType2); //COPY AS
            final HllSketch sk1B = sk2.copyAs(tgtHllType1); //COPY AS
            final byte[] sk1Bbytes = sk1B.toUpdatableByteArray(); //UPDATABLE
            final String comb = "LgK= " + lgK + ", CurMode= " + curMode.toString()
            + ", Type1: " + tgtHllType1 + ", Type2: " + tgtHllType2;
            checkArrays(sk1bytes, sk1Bbytes, comb, false);
          }
        }
      }
    }
  }

  @Test
  //Converts a type1 to a different type and converts back to type1 to check binary equivalence.
  public void isomorphicCopyAsCompactHeap() {
    for (int lgK = 4; lgK <= 21; lgK++) { //All LgK
      for (int cm = 0; cm <= 2; cm++) { //List, Set, Hll
        if ((lgK < 8) && (cm == 1)) { continue; } //lgk < 8 list transistions directly to HLL
        final CurMode curMode = CurMode.fromOrdinal(cm);
        for (int t1 = 0; t1 <= 2; t1++) { //HLL_4, HLL_6, HLL_8
          final TgtHllType tgtHllType1 = TgtHllType.fromOrdinal(t1);
          final HllSketch sk1 = buildHeapSketch(lgK, tgtHllType1, curMode);
          final byte[] sk1bytes = sk1.toCompactByteArray(); //COMPACT
          for (int t2 = 0; t2 <= 2; t2++) { //HLL_4, HLL_6, HLL_8
            if (t2 == t1) { continue; }
            final TgtHllType tgtHllType2 = TgtHllType.fromOrdinal(t2);
            final HllSketch sk2 = sk1.copyAs(tgtHllType2); //COPY AS
            final HllSketch sk3 = sk2.copyAs(tgtHllType1); //COPY AS
            final byte[] sk3bytes = sk3.toCompactByteArray(); //COMPACT
            final String comb = "LgK= " + lgK + ", CurMode= " + curMode.toString()
            + ", Type1: " + tgtHllType1 + ", Type2: " + tgtHllType2;
            checkArrays(sk1bytes, sk3bytes, comb, false);
          }
        }
      }
    }
  }

  @Test
  //Compares two HLL to HLL merges. The input sketch varies by tgtHllType.
  //The LgKs can be equal or the source sketch is one larger.
  //The result of the union is converted to HLL_8 and checked between different combinations of
  //heap, MemorySegment for binary equivalence.
  public void isomorphicHllMerges() {
    for (int uLgK = 4; uLgK <= 20; uLgK++) { //All LgK
      int skLgK = uLgK;
      for (int t1 = 0; t1 <= 2; t1++) { //HLL_4, HLL_6, HLL_8
        final TgtHllType tgtHllType = TgtHllType.fromOrdinal(t1);
        innerLoop(uLgK, skLgK, tgtHllType);
      }
      skLgK = uLgK + 1;
      for (int t1 = 0; t1 <= 2; t1++) { //HLL_4, HLL_6, HLL_8
        final TgtHllType tgtHllType = TgtHllType.fromOrdinal(t1);
        innerLoop(uLgK, skLgK, tgtHllType);
      }
    }
  }

  private static void innerLoop(final int uLgK, final int skLgK, final TgtHllType tgtHllType) {
    Union u;
    HllSketch sk;
    final HllSketch skOut;

    //CASE 1 Heap Union, Heap sketch
    u = buildHeapUnionHllMode(uLgK, 0);
    sk = buildHeapSketchHllMode(skLgK, tgtHllType, 1 << uLgK);
    u.update(sk);
    final byte[] bytesOut1 = u.getResult(HLL_8).toUpdatableByteArray();

    //CASE 2 Heap Union, MemorySegment sketch
    u = buildHeapUnionHllMode(uLgK, 0);
    sk = buildMemorySegmentSketchHllMode(skLgK, tgtHllType, 1 << uLgK);
    u.update(sk);
    final byte[] bytesOut2 = u.getResult(HLL_8).toUpdatableByteArray();

    //println("Uheap/SkHeap    HIP: " + bytesToDouble(bytesOut1, 8)); //HipAccum
    //println("Uheap/SkSegment HIP: " + bytesToDouble(bytesOut2, 8)); //HipAccum
    String comb = "uLgK: " + uLgK + ", skLgK: " + skLgK
        + ", SkType: " + tgtHllType.toString()
        + ", Case1: Heap Union, Heap sketch; Case2: /Heap Union, MemorySegment sketch";
    checkArrays(bytesOut1, bytesOut2, comb, false);

    //CASE 3 Offheap Union, Heap sketch
    u = buildMemorySegmentUnionHllMode(uLgK, 0);
    sk = buildHeapSketchHllMode(skLgK, tgtHllType, 1 << uLgK);
    u.update(sk);
    final byte[] bytesOut3 = u.getResult(HLL_8).toUpdatableByteArray();

    //println("Uheap/SkSegment HIP:  " + bytesToDouble(bytesOut2, 8)); //HipAccum
    //println("Usegment/SkHeap HIP:  " + bytesToDouble(bytesOut3, 8)); //HipAccum
    comb = "LgK: " + uLgK + ", skLgK: " + skLgK
        + ", SkType: " + tgtHllType.toString()
        + ", Case2: Heap Union, MemorySegment sketch; Case3: /MemorySegment Union, Heap sketch";
    checkArrays(bytesOut2, bytesOut3, comb, false);

    //Case 4 MemorySegment Union, MemorySegment sketch
    u = buildMemorySegmentUnionHllMode(uLgK, 0);
    sk = buildMemorySegmentSketchHllMode(skLgK, tgtHllType, 1 << uLgK);
    u.update(sk);
    final byte[] bytesOut4 = u.getResult(HLL_8).toUpdatableByteArray();

    comb = "LgK: " + uLgK + ", skLgK: " + skLgK
        + ", SkType: " + tgtHllType.toString()
        + ", Case2: Heap Union, MemorySegment sketch; Case4: /MemorySegment Union, MemorySegment sketch";
    checkArrays(bytesOut2, bytesOut4, comb, false);
  }

  @Test
  //Creates a binary reference: HLL_8 merged with union, HLL_8 result binary.
  //Case 1: HLL_6 merged with a union, HLL_8 result binary compared with the reference.
  //Case 2: HLL_4 merged with a union, Hll_8 result binary compared with the reference.
  //Both Case 1 and 2 should differ in the binary output compared with the reference only for the
  //HllAccum register.
  public void isomorphicHllMerges2() {
    byte[] bytesOut8, bytesOut6, bytesOut4;
    String comb;
    Union u;
    HllSketch sk;
    for (int lgK = 4; lgK <= 4; lgK++) { //All LgK
      u = buildHeapUnionHllMode(lgK, 0);
      sk = buildHeapSketchHllMode(lgK, HLL_8, 1 << lgK);
      u.update(sk);
      bytesOut8 = u.getResult(HLL_8).toUpdatableByteArray(); //The reference

      u = buildHeapUnionHllMode(lgK, 0);
      sk = buildHeapSketchHllMode(lgK, HLL_6, 1 << lgK);
      u.update(sk);
      bytesOut6 = u.getResult(HLL_8).toUpdatableByteArray();//should be identical except for HllAccum

      comb = "LgK: " + lgK + ", SkType: HLL_6, Compared with SkType HLL_8";
      checkArrays(bytesOut8, bytesOut6, comb, false);

      u = buildHeapUnionHllMode(lgK, 0);
      sk = buildHeapSketchHllMode(lgK, HLL_4, 1 << lgK);
      u.update(sk);
      bytesOut4 = u.getResult(HLL_8).toUpdatableByteArray();//should be identical except for HllAccum

      comb = "LgK: " + lgK + ", SkType: HLL_4, Compared with SkType HLL_8";
      checkArrays(bytesOut8, bytesOut4, comb, false);
    }
  }

  private static void checkArrays(final byte[] sk1, final byte[] sk2, final String comb, final boolean omitHipAccum) {
    final int len = sk1.length;
    if (len != sk2.length) {
      println("Sketch images not the same length: " + comb);
      return;
    }
    print(comb + ": ");
    for (int i = 0; i < len; i++) {
      if ((omitHipAccum && (i >= 8) && (i <= 15)) || (sk1[i] == sk2[i])) { continue; }
      print(i + " ");
      fail();
    }
    println("");
  }

  //BUILDERS
  private Union buildHeapUnion(final int lgMaxK, final CurMode curMode) {
    final Union u = new Union(lgMaxK);
    final int n = (curMode == null) ? 0 : getN(lgMaxK, curMode);
    for (int i = 0; i < n; i++) { u.update(i + v); }
    v += n;
    return u;
  }

  private Union buildMemorySegmentUnion(final int lgMaxK, final CurMode curMode) {
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgMaxK, TgtHllType.HLL_8);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    final Union u = new Union(lgMaxK, wseg);
    final int n = (curMode == null) ? 0 : getN(lgMaxK, curMode);
    for (int i = 0; i < n; i++) { u.update(i + v); }
    v += n;
    return u;
  }

  private HllSketch buildHeapSketch(final int lgK, final TgtHllType tgtHllType, final CurMode curMode) {
    final HllSketch sk = new HllSketch(lgK, tgtHllType);
    final int n = (curMode == null) ? 0 : getN(lgK, curMode);
    for (int i = 0; i < n; i++) { sk.update(i + v); }
    v += n;
    return sk;
  }

  private HllSketch buildMemorySegmentSketch(final int lgK, final TgtHllType tgtHllType, final CurMode curMode) {
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK,tgtHllType);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    final HllSketch sk = new HllSketch(lgK, tgtHllType, wseg);
    final int n = (curMode == null) ? 0 : getN(lgK, curMode);
    for (int i = 0; i < n; i++) { sk.update(i + v); }
    v += n;
    return sk;
  }

  private static Union buildHeapUnionHllMode(final int lgMaxK, final int startN) {
    final Union u = new Union(lgMaxK);
    final int n = getN(lgMaxK, HLL);
    for (int i = 0; i < n; i++) { u.update(i + startN); }
    return u;
  }

  private static Union buildMemorySegmentUnionHllMode(final int lgMaxK, final int startN) {
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgMaxK, TgtHllType.HLL_8);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    final Union u = new Union(lgMaxK, wseg);
    final int n = getN(lgMaxK, HLL);
    for (int i = 0; i < n; i++) { u.update(i + startN); }
    return u;
  }

  private static HllSketch buildHeapSketchHllMode(final int lgK, final TgtHllType tgtHllType, final int startN) {
    final HllSketch sk = new HllSketch(lgK, tgtHllType);
    final int n = getN(lgK, HLL);
    for (int i = 0; i < n; i++) { sk.update(i + startN); }
    return sk;
  }

  private static HllSketch buildMemorySegmentSketchHllMode(final int lgK, final TgtHllType tgtHllType, final int startN) {
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK,tgtHllType);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    final HllSketch sk = new HllSketch(lgK, tgtHllType, wseg);
    final int n = getN(lgK, HLL);
    for (int i = 0; i < n; i++) { sk.update(i + startN); }
    return sk;
  }

  //if lgK >= 8, curMode != SET!
  private static int getN(final int lgK, final CurMode curMode) {
    if (curMode == LIST) { return 4; }
    if (curMode == SET) { return 1 << (lgK - 4); }
    return ((lgK < 8) && (curMode == HLL)) ? (1 << lgK) : 1 << (lgK - 3);
  }

  @Test
  public void checkCurMinConversion() {
    final TgtHllType hll8 = HLL_8;
    final TgtHllType hll4 = HLL_4;
    for (int lgK = 4; lgK <= 21; lgK++) {
      final HllSketch sk8 = new HllSketch(lgK, hll8);
      //The Coupon Collector Problem predicts that all slots will be filled by k Log(k).
      final int n = (1 << lgK) * lgK;
      for (int i = 0; i < n; i++) { sk8.update(i); }
      final double est8 = sk8.getEstimate();
      final AbstractHllArray aharr8 = (AbstractHllArray)sk8.hllSketchImpl;
      final int curMin8 = aharr8.getCurMin();
      final int numAtCurMin8 = aharr8.getNumAtCurMin();
      final HllSketch sk4 = sk8.copyAs(hll4);
      final AbstractHllArray aharr4 = (AbstractHllArray)sk4.hllSketchImpl;
      final int curMin4 = ((AbstractHllArray)sk4.hllSketchImpl).getCurMin();
      final int numAtCurMin4 =aharr4.getNumAtCurMin();
      final double est4 = sk4.getEstimate();
      assertEquals(est4, est8, 0.0);
      assertEquals(curMin4, 1);
      //println("Est 8 = " + est8 + ", CurMin = " + curMin8 + ", #CurMin + " + numAtCurMin8);
      //println("Est 4 = " + est4 + ", CurMin = " + curMin4 + ", #CurMin + " + numAtCurMin4);
    }
  }

  private static double bytesToDouble(final byte[] arr, final int offset) {
    long v = 0;
    for (int i = offset; i < (offset + 8); i++) {
      v |= (arr[i] & 0XFFL) << (i * 8);
    }
    return Double.longBitsToDouble(v);
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
