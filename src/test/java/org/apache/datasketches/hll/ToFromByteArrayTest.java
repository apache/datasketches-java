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

import static org.apache.datasketches.hll.TgtHllType.HLL_4;
import static org.apache.datasketches.hll.TgtHllType.HLL_6;
import static org.apache.datasketches.hll.TgtHllType.HLL_8;
import static org.testng.Assert.assertEquals;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class ToFromByteArrayTest {

  static final int[] nArr = {1, 3, 10, 30, 100, 300, 1000, 3000, 10000, 30000};

  @Test
  public void checkToFromSketch1() {
    for (int i = 0; i < 10; i++) {
      final int n = nArr[i];
      for (int lgK = 4; lgK <= 13; lgK++) {
        toFrom1(lgK, HLL_4, n);
        toFrom1(lgK, HLL_6, n);
        toFrom1(lgK, HLL_8, n);
      }
      println("=======");
    }
  }

  private static void toFrom1(final int lgConfigK, final TgtHllType tgtHllType, final int n) {
    final HllSketch src = new HllSketch(lgConfigK, tgtHllType);
    for (int i = 0; i < n; i++) {
      src.update(i);
    }
    //println("n: " + n + ", lgK: " + lgK + ", type: " + tgtHllType);
    //printSketch(src, "SRC");

    final byte[] byteArr1 = src.toCompactByteArray(); //compact
    final HllSketch dst = HllSketch.heapify(byteArr1);  //using byte[] interface
    //printSketch(dst, "DST");
    assertEquals(dst.getEstimate(), src.getEstimate(), 0.0);

    final byte[] byteArr2 = src.toUpdatableByteArray(); //updatable
    final MemorySegment seg2 = MemorySegment.ofArray(byteArr2);
    final HllSketch dst2 = HllSketch.heapify(seg2); //using MemorySegment interface
    //printSketch(dst, "DST");
    assertEquals(dst2.getEstimate(), src.getEstimate(), 0.0);

    final MemorySegment seg3 = MemorySegment.ofArray(byteArr2);
    final HllSketch dst3 = HllSketch.heapify(seg3); //using MemorySegment interface
    //printSketch(dst, "DST");
    assertEquals(dst3.getEstimate(), src.getEstimate(), 0.0);
  }

  @Test
  public void checkToFromSketch2() {
    for (int i = 0; i < 10; i++) {
      final int n = nArr[i];
      for (int lgK = 4; lgK <= 13; lgK++) {
        toFrom2(lgK, HLL_4, n);
        toFrom2(lgK, HLL_6, n);
        toFrom2(lgK, HLL_8, n);
      }
      println("=======");
    }
  }

  //Test direct
  private static void toFrom2(final int lgConfigK, final TgtHllType tgtHllType, final int n) {
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgConfigK, tgtHllType);
    final byte[] byteArray = new byte[bytes];
    final MemorySegment wseg = MemorySegment.ofArray(byteArray);
    final HllSketch src = new HllSketch(lgConfigK, tgtHllType, wseg);
    for (int i = 0; i < n; i++) {
      src.update(i);
    }
    //println("n: " + n + ", lgK: " + lgConfigK + ", type: " + tgtHllType);
    //printSketch(src, "Source");

    //Heapify compact
    final byte[] compactByteArr = src.toCompactByteArray(); //compact
    final HllSketch dst = HllSketch.heapify(compactByteArr);  //using byte[] interface
    //printSketch(dst, "Heapify From Compact");
    assertEquals(dst.getEstimate(), src.getEstimate(), 0.0);

    //Heapify updatable
    final byte[] updatableByteArr = src.toUpdatableByteArray();
    final MemorySegment wseg2 = MemorySegment.ofArray(updatableByteArr);
    final HllSketch dst2 = HllSketch.heapify(wseg2); //using MemorySegment interface
    //printSketch(dst2, "Heapify From Updatable");
    assertEquals(dst2.getEstimate(), src.getEstimate(), 0.0);

    //Wrap updatable
    final MemorySegment wseg3 = MemorySegment.ofArray(new byte[bytes]);
    MemorySegment.copy(wseg2, 0, wseg3, 0, wseg2.byteSize());
    final HllSketch dst3 = HllSketch.writableWrap(wseg3);
    //printSketch(dst3, "WritableWrap From Updatable");
    assertEquals(dst3.getEstimate(), src.getEstimate(), 0.0);

    //Wrap updatable as Read-Only
    final HllSketch dst4 = HllSketch.wrap(wseg3);
    assertEquals(dst4.getEstimate(), src.getEstimate(), 0.0);
  }

  //  static void printSketch(HllSketch sketch, String name) {
  //    println(name +":\n" + sketch.toString(true, true, true, false));
  //  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param o value to print
   */
  static void println(final Object o) {
    //System.out.println(o.toString()); //disable here
  }

}
