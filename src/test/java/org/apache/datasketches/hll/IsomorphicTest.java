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

import static org.apache.datasketches.hll.CurMode.HLL;
import static org.apache.datasketches.hll.CurMode.LIST;
import static org.apache.datasketches.hll.CurMode.SET;
import static org.apache.datasketches.hll.TgtHllType.HLL_4;
import static org.apache.datasketches.hll.TgtHllType.HLL_6;
import static org.apache.datasketches.hll.TgtHllType.HLL_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings({"javadoc", "unused"})
public class IsomorphicTest {
  long v = 0;

  @Test
  //Merges a type1 to an empty union (heap, HLL_8), and gets result as type1, checks binary equivalence
  public void isomorphicUnionUpdatableHeap() {
    for (int lgK = 4; lgK <= 21; lgK++) { //All LgK
      for (int cm = 0; cm <= 2; cm++) { //List, Set, Hll
        if ((lgK < 8) && (cm == 1)) { continue; } //lgk < 8 list transistions directly to HLL
        CurMode curMode = CurMode.fromOrdinal(cm);
        for (int t = 0; t <= 2; t++) { //HLL_4, HLL_6, HLL_8
          TgtHllType tgtHllType1 = TgtHllType.fromOrdinal(t);
          HllSketch sk1 = buildHeapSketch(lgK, tgtHllType1, curMode);
          byte[] sk1bytes = sk1.toUpdatableByteArray(); //UPDATABLE
          Union union = buildHeapUnion(lgK, null); //UNION
          union.update(sk1);
          HllSketch sk2 = union.getResult(tgtHllType1);
          byte[] sk2bytes = sk2.toUpdatableByteArray(); //UPDATABLE
          String comb = "LgK=" + lgK + ", CurMode=" + curMode.toString() + ", Type:" + tgtHllType1;
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
        CurMode curMode = CurMode.fromOrdinal(cm);
        for (int t = 0; t <= 2; t++) { //HLL_4, HLL_6, HLL_8
          TgtHllType tgtHllType1 = TgtHllType.fromOrdinal(t);
          HllSketch sk1 = buildHeapSketch(lgK, tgtHllType1, curMode);
          byte[] sk1bytes = sk1.toCompactByteArray(); //COMPACT
          Union union = buildHeapUnion(lgK, null); //UNION
          union.update(sk1);
          HllSketch sk2 = union.getResult(tgtHllType1);
          byte[] sk2bytes = sk2.toCompactByteArray(); //COMPACT
          String comb = "LgK=" + lgK + ", CurMode=" + curMode.toString() + ", Type:" + tgtHllType1;
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
        CurMode curMode = CurMode.fromOrdinal(cm);
        for (int t1 = 0; t1 <= 2; t1++) { //HLL_4, HLL_6, HLL_8
          TgtHllType tgtHllType1 = TgtHllType.fromOrdinal(t1);
          HllSketch sk1 = buildHeapSketch(lgK, tgtHllType1, curMode);
          byte[] sk1bytes = sk1.toUpdatableByteArray(); //UPDATABLE
          for (int t2 = 0; t2 <= 2; t2++) { //HLL_4, HLL_6, HLL_8
            if (t2 == t1) { continue; }
            TgtHllType tgtHllType2 = TgtHllType.fromOrdinal(t2);
            HllSketch sk2 = sk1.copyAs(tgtHllType2); //COPY AS
            HllSketch sk1B = sk2.copyAs(tgtHllType1); //COPY AS
            byte[] sk1Bbytes = sk1B.toUpdatableByteArray(); //UPDATABLE
            String comb = "LgK= " + lgK + ", CurMode= " + curMode.toString()
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
        CurMode curMode = CurMode.fromOrdinal(cm);
        for (int t1 = 0; t1 <= 2; t1++) { //HLL_4, HLL_6, HLL_8
          TgtHllType tgtHllType1 = TgtHllType.fromOrdinal(t1);
          HllSketch sk1 = buildHeapSketch(lgK, tgtHllType1, curMode);
          byte[] sk1bytes = sk1.toCompactByteArray(); //COMPACT
          for (int t2 = 0; t2 <= 2; t2++) { //HLL_4, HLL_6, HLL_8
            if (t2 == t1) { continue; }
            TgtHllType tgtHllType2 = TgtHllType.fromOrdinal(t2);
            HllSketch sk2 = sk1.copyAs(tgtHllType2); //COPY AS
            HllSketch sk3 = sk2.copyAs(tgtHllType1); //COPY AS
            byte[] sk3bytes = sk3.toCompactByteArray(); //COMPACT
            String comb = "LgK= " + lgK + ", CurMode= " + curMode.toString()
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
  //heap, memory for binary equivalence.
  public void isomorphicHllMerges() {
    for (int uLgK = 4; uLgK <= 20; uLgK++) { //All LgK
      int skLgK = uLgK;
      for (int t1 = 0; t1 <= 2; t1++) { //HLL_4, HLL_6, HLL_8
        TgtHllType tgtHllType = TgtHllType.fromOrdinal(t1);
        innerLoop(uLgK, skLgK, tgtHllType);
      }
      skLgK = uLgK + 1;
      for (int t1 = 0; t1 <= 2; t1++) { //HLL_4, HLL_6, HLL_8
        TgtHllType tgtHllType = TgtHllType.fromOrdinal(t1);
        innerLoop(uLgK, skLgK, tgtHllType);
      }
    }
  }

  private static void innerLoop(int uLgK, int skLgK, TgtHllType tgtHllType) {
    Union u;
    HllSketch sk, skOut;

    //CASE 1 Heap Union, Heap sketch
    u = buildHeapUnionHllMode(uLgK, 0);
    sk = buildHeapSketchHllMode(skLgK, tgtHllType, 1 << uLgK);
    u.update(sk);
    byte[] bytesOut1 = u.getResult(HLL_8).toUpdatableByteArray();

    //CASE 2 Heap Union, Memory sketch
    u = buildHeapUnionHllMode(uLgK, 0);
    sk = buildMemorySketchHllMode(skLgK, tgtHllType, 1 << uLgK);
    u.update(sk);
    byte[] bytesOut2 = u.getResult(HLL_8).toUpdatableByteArray();

    //println("Uheap/SkHeap   HIP: " + bytesToDouble(bytesOut1, 8)); //HipAccum
    //println("Uheap/SkMemory HIP: " + bytesToDouble(bytesOut2, 8)); //HipAccum
    String comb = "uLgK: " + uLgK + ", skLgK: " + skLgK
        + ", SkType: " + tgtHllType.toString()
        + ", Case1: Heap Union, Heap sketch; Case2: /Heap Union, Memory sketch";
    checkArrays(bytesOut1, bytesOut2, comb, false);

    //CASE 3 Offheap Union, Heap sketch
    u = buildMemoryUnionHllMode(uLgK, 0);
    sk = buildHeapSketchHllMode(skLgK, tgtHllType, 1 << uLgK);
    u.update(sk);
    byte[] bytesOut3 = u.getResult(HLL_8).toUpdatableByteArray();

    //println("Uheap/SkMemory HIP: " + bytesToDouble(bytesOut2, 8)); //HipAccum
    //println("Umemory/SkHeap HIP: " + bytesToDouble(bytesOut3, 8)); //HipAccum
    comb = "LgK: " + uLgK + ", skLgK: " + skLgK
        + ", SkType: " + tgtHllType.toString()
        + ", Case2: Heap Union, Memory sketch; Case3: /Memory Union, Heap sketch";
    checkArrays(bytesOut2, bytesOut3, comb, false);

    //Case 4 Memory Union, Memory sketch
    u = buildMemoryUnionHllMode(uLgK, 0);
    sk = buildMemorySketchHllMode(skLgK, tgtHllType, 1 << uLgK);
    u.update(sk);
    byte[] bytesOut4 = u.getResult(HLL_8).toUpdatableByteArray();

    comb = "LgK: " + uLgK + ", skLgK: " + skLgK
        + ", SkType: " + tgtHllType.toString()
        + ", Case2: Heap Union, Memory sketch; Case4: /Memory Union, Memory sketch";
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

  private static void checkArrays(byte[] sk1, byte[] sk2, String comb, boolean omitHipAccum) {
    int len = sk1.length;
    if (len != sk2.length) {
      println("Sketch images not the same length: " + comb);
      return;
    }
    print(comb + ": ");
    for (int i = 0; i < len; i++) {
      if (omitHipAccum && (i >= 8) && (i <= 15)) { continue; }
      if (sk1[i] == sk2[i]) { continue; }
      print(i + " ");
      fail();
    }
    println("");
  }

  //BUILDERS
  private Union buildHeapUnion(int lgMaxK, CurMode curMode) {
    Union u = new Union(lgMaxK);
    int n = (curMode == null) ? 0 : getN(lgMaxK, curMode);
    for (int i = 0; i < n; i++) { u.update(i + v); }
    v += n;
    return u;
  }

  private Union buildMemoryUnion(int lgMaxK, CurMode curMode) {
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgMaxK, TgtHllType.HLL_8);
    WritableMemory wmem = WritableMemory.allocate(bytes);
    Union u = new Union(lgMaxK, wmem);
    int n = (curMode == null) ? 0 : getN(lgMaxK, curMode);
    for (int i = 0; i < n; i++) { u.update(i + v); }
    v += n;
    return u;
  }

  private HllSketch buildHeapSketch(int lgK, TgtHllType tgtHllType, CurMode curMode) {
    HllSketch sk = new HllSketch(lgK, tgtHllType);
    int n = (curMode == null) ? 0 : getN(lgK, curMode);
    for (int i = 0; i < n; i++) { sk.update(i + v); }
    v += n;
    return sk;
  }

  private HllSketch buildMemorySketch(int lgK, TgtHllType tgtHllType, CurMode curMode) {
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK,tgtHllType);
    WritableMemory wmem = WritableMemory.allocate(bytes);
    HllSketch sk = new HllSketch(lgK, tgtHllType, wmem);
    int n = (curMode == null) ? 0 : getN(lgK, curMode);
    for (int i = 0; i < n; i++) { sk.update(i + v); }
    v += n;
    return sk;
  }

  private static Union buildHeapUnionHllMode(int lgMaxK, int startN) {
    Union u = new Union(lgMaxK);
    int n = getN(lgMaxK, HLL);
    for (int i = 0; i < n; i++) { u.update(i + startN); }
    return u;
  }

  private static Union buildMemoryUnionHllMode(int lgMaxK, int startN) {
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgMaxK, TgtHllType.HLL_8);
    WritableMemory wmem = WritableMemory.allocate(bytes);
    Union u = new Union(lgMaxK, wmem);
    int n = getN(lgMaxK, HLL);
    for (int i = 0; i < n; i++) { u.update(i + startN); }
    return u;
  }

  private static HllSketch buildHeapSketchHllMode(int lgK, TgtHllType tgtHllType, int startN) {
    HllSketch sk = new HllSketch(lgK, tgtHllType);
    int n = getN(lgK, HLL);
    for (int i = 0; i < n; i++) { sk.update(i + startN); }
    return sk;
  }

  private static HllSketch buildMemorySketchHllMode(int lgK, TgtHllType tgtHllType, int startN) {
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK,tgtHllType);
    WritableMemory wmem = WritableMemory.allocate(bytes);
    HllSketch sk = new HllSketch(lgK, tgtHllType, wmem);
    int n = getN(lgK, HLL);
    for (int i = 0; i < n; i++) { sk.update(i + startN); }
    return sk;
  }

  //if lgK >= 8, curMode != SET!
  private static int getN(int lgK, CurMode curMode) {
    if (curMode == LIST) { return 4; }
    if (curMode == SET) { return 1 << (lgK - 4); }
    return ((lgK < 8) && (curMode == HLL)) ? (1 << lgK) : 1 << (lgK - 3);
  }

  @Test
  public void checkCurMinConversion() {
    TgtHllType hll8 = HLL_8;
    TgtHllType hll4 = HLL_4;
    for (int lgK = 4; lgK <= 21; lgK++) {
      HllSketch sk8 = new HllSketch(lgK, hll8);
      //The Coupon Collector Problem predicts that all slots will be filled by k Log(k).
      int n = (1 << lgK) * lgK;
      for (int i = 0; i < n; i++) { sk8.update(i); }
      double est8 = sk8.getEstimate();
      AbstractHllArray aharr8 = (AbstractHllArray)sk8.hllSketchImpl;
      int curMin8 = aharr8.getCurMin();
      int numAtCurMin8 = aharr8.getNumAtCurMin();
      HllSketch sk4 = sk8.copyAs(hll4);
      AbstractHllArray aharr4 = (AbstractHllArray)sk4.hllSketchImpl;
      int curMin4 = ((AbstractHllArray)sk4.hllSketchImpl).getCurMin();
      int numAtCurMin4 =aharr4.getNumAtCurMin();
      double est4 = sk4.getEstimate();
      assertEquals(est4, est8, 0.0);
      assertEquals(curMin4, 1);
      //println("Est 8 = " + est8 + ", CurMin = " + curMin8 + ", #CurMin + " + numAtCurMin8);
      //println("Est 4 = " + est4 + ", CurMin = " + curMin4 + ", #CurMin + " + numAtCurMin4);
    }
  }

  private static double bytesToDouble(byte[] arr, int offset) {
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
