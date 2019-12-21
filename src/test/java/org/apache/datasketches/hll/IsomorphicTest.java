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

import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings({"javadoc", "unused"})
public class IsomorphicTest {
  long v = 0;
  final int maxLgK = 12;
  HllSketch source;
  Union union;

  @Test
  public void isomorphsUnionUpdatableHeap() {
    for (int lgK = 4; lgK <= 21; lgK++) {
      for (int cm = 0; cm <= 2; cm++) {
        if ((lgK < 8) && (cm == 1)) { continue; } //lgk < 8 list transistions directly to HLL
        CurMode curMode = CurMode.fromOrdinal(cm);
        for (int t = 0; t <= 2; t++) {
          TgtHllType tgtHllType = TgtHllType.fromOrdinal(t);
          HllSketch sk1 = buildHeapSketch(lgK, tgtHllType, curMode);
          byte[] sk1bytes = sk1.toUpdatableByteArray();
          Union union = buildHeapUnion(lgK, null);
          union.update(sk1);
          HllSketch sk2 = union.getResult(tgtHllType);
          byte[] sk2bytes = sk2.toUpdatableByteArray();
          String comb = "LgK=" + lgK + ", CurMode=" + curMode.toString() + ", Type:" + tgtHllType;
          checkArrays(sk1bytes, sk2bytes, comb);
        }
      }
    }
  }

  @Test
  public void isomorphsUnionCompactHeap() {
    for (int lgK = 4; lgK <= 21; lgK++) {
      for (int cm = 0; cm <= 2; cm++) {
        if ((lgK < 8) && (cm == 1)) { continue; } //lgk < 8 list transistions directly to HLL
        CurMode curMode = CurMode.fromOrdinal(cm);
        for (int t = 0; t <= 2; t++) {
          TgtHllType tgtHllType = TgtHllType.fromOrdinal(t);
          HllSketch sk1 = buildHeapSketch(lgK, tgtHllType, curMode);
          byte[] sk1bytes = sk1.toCompactByteArray();
          Union union = buildHeapUnion(lgK, null);
          union.update(sk1);
          HllSketch sk2 = union.getResult(tgtHllType);
          byte[] sk2bytes = sk2.toCompactByteArray();
          String comb = "LgK=" + lgK + ", CurMode=" + curMode.toString() + ", Type:" + tgtHllType;
          checkArrays(sk1bytes, sk2bytes, comb);
        }
      }
    }
  }


  @Test
  public void isomorphsCopyAsUpdatableHeap() {
    for (int lgK = 4; lgK <= 21; lgK++) {
      for (int cm = 0; cm <= 2; cm++) {
        if ((lgK < 8) && (cm == 1)) { continue; } //lgk < 8 list transistions directly to HLL
        CurMode curMode = CurMode.fromOrdinal(cm);
        for (int t1 = 0; t1 <= 2; t1++) {
          TgtHllType tgtHllType1 = TgtHllType.fromOrdinal(t1);
          HllSketch sk1 = buildHeapSketch(lgK, tgtHllType1, curMode);
          byte[] sk1bytes = sk1.toUpdatableByteArray();

          for (int t2 = 0; t2 <= 2; t2++) {
            if (t2 == t1) { continue; }
            TgtHllType tgtHllType2 = TgtHllType.fromOrdinal(t2);
            HllSketch sk2 = sk1.copyAs(tgtHllType2);
            HllSketch sk3 = sk2.copyAs(tgtHllType1);
            byte[] sk3bytes = sk3.toUpdatableByteArray();
            String comb = "LgK= " + lgK + ", CurMode= " + curMode.toString()
            + ", Type1: " + tgtHllType1 + ", Type2: " + tgtHllType2;
            checkArrays(sk1bytes, sk3bytes, comb);
          }
        }
      }
    }
  }

  @Test
  public void isomorphsCopyAsCompactHeap() {
    for (int lgK = 4; lgK <= 21; lgK++) {
      for (int cm = 0; cm <= 2; cm++) {
        if ((lgK < 8) && (cm == 1)) { continue; } //lgk < 8 list transistions directly to HLL
        CurMode curMode = CurMode.fromOrdinal(cm);
        for (int t1 = 0; t1 <= 2; t1++) {
          TgtHllType tgtHllType1 = TgtHllType.fromOrdinal(t1);
          HllSketch sk1 = buildHeapSketch(lgK, tgtHllType1, curMode);
          byte[] sk1bytes = sk1.toCompactByteArray();

          for (int t2 = 0; t2 <= 2; t2++) {
            if (t2 == t1) { continue; }
            TgtHllType tgtHllType2 = TgtHllType.fromOrdinal(t2);
            HllSketch sk2 = sk1.copyAs(tgtHllType2);
            HllSketch sk3 = sk2.copyAs(tgtHllType1);
            byte[] sk3bytes = sk3.toCompactByteArray();
            String comb = "LgK= " + lgK + ", CurMode= " + curMode.toString()
            + ", Type1: " + tgtHllType1 + ", Type2: " + tgtHllType2;
            checkArrays(sk1bytes, sk3bytes, comb);
          }
        }
      }
    }
  }


  private static void checkArrays(byte[] sk1, byte[] sk2, String comb) {
    int len = sk1.length;
    if (len != sk2.length) {
      println("Sketch images not the same length: " + comb);
      return;
    }

    boolean first = true;
    for (int i = 0; i < len; i++) {
      if (sk1[i] == sk2[i]) { continue; }
      if (first) {
        print(comb + ": ");
        first = false;
      }
      print(i + " ");
    }
    if (!first) { println(""); }
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

  //lgK >= 8
  private static int getN(int lgK, CurMode curMode) {
    if (curMode == LIST) { return 4; }
    if (curMode == SET) { return 1 << (lgK - 4); }
    return 1 << (lgK - 3); //HLL
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
