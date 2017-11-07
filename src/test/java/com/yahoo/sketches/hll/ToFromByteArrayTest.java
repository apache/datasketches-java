/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.TgtHllType.HLL_4;
import static com.yahoo.sketches.hll.TgtHllType.HLL_6;
import static com.yahoo.sketches.hll.TgtHllType.HLL_8;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;

/**
 * @author Lee Rhodes
 *
 */
public class ToFromByteArrayTest {

  static final int[] nArr = new int[] {1, 3, 10, 30, 100, 300, 1000, 3000, 10000, 30000};


  @Test
  public void checkToFromSketch1() {
    for (int i = 0; i < 10; i++) {
      int n = nArr[i];
      for (int lgK = 4; lgK <= 13; lgK++) {
        toFrom1(lgK, HLL_4, n);
        toFrom1(lgK, HLL_6, n);
        toFrom1(lgK, HLL_8, n);
      }
      println("=======");
    }
  }

  private static void toFrom1(int lgConfigK, TgtHllType tgtHllType, int n) {
    HllSketch src = new HllSketch(lgConfigK, tgtHllType);
    for (int i = 0; i < n; i++) {
      src.update(i);
    }
    //println("n: " + n + ", lgK: " + lgK + ", type: " + tgtHllType);
    //printSketch(src, "SRC");

    byte[] byteArr1 = src.toCompactByteArray(); //compact
    HllSketch dst = HllSketch.heapify(byteArr1);  //using byte[] interface
    //printSketch(dst, "DST");
    assertEquals(dst.getEstimate(), src.getEstimate(), 0.0);

    byte[] byteArr2 = src.toUpdatableByteArray(); //updatable
    Memory mem2 = Memory.wrap(byteArr2);
    HllSketch dst2 = HllSketch.heapify(mem2); //using Memory interface
    //printSketch(dst, "DST");
    assertEquals(dst2.getEstimate(), src.getEstimate(), 0.0);

    WritableMemory mem3 = WritableMemory.wrap(byteArr2);
    HllSketch dst3 = HllSketch.heapify(mem3); //using WritableMemory interface
    //printSketch(dst, "DST");
    assertEquals(dst3.getEstimate(), src.getEstimate(), 0.0);
  }

  @Test
  public void checkToFromSketch2() {
    for (int i = 0; i < 10; i++) {
      int n = nArr[i];
      for (int lgK = 4; lgK <= 13; lgK++) {
        toFrom2(lgK, HLL_4, n);
        toFrom2(lgK, HLL_6, n);
        toFrom2(lgK, HLL_8, n);
      }
      println("=======");
    }
  }

  //Test direct
  private static void toFrom2(int lgConfigK, TgtHllType tgtHllType, int n) {
    int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgConfigK, tgtHllType);
    byte[] byteArray = new byte[bytes];
    WritableMemory wmem = WritableMemory.wrap(byteArray);
    HllSketch src = new HllSketch(lgConfigK, tgtHllType, wmem);
    for (int i = 0; i < n; i++) {
      src.update(i);
    }
    //println("n: " + n + ", lgK: " + lgConfigK + ", type: " + tgtHllType);
    //printSketch(src, "Source");

    //Heapify compact
    byte[] compactByteArr = src.toCompactByteArray(); //compact
    HllSketch dst = HllSketch.heapify(compactByteArr);  //using byte[] interface
    //printSketch(dst, "Heapify From Compact");
    assertEquals(dst.getEstimate(), src.getEstimate(), 0.0);

    //Heapify updatable
    byte[] updatableByteArr = src.toUpdatableByteArray();
    WritableMemory wmem2 = WritableMemory.wrap(updatableByteArr);
    HllSketch dst2 = HllSketch.heapify(wmem2); //using Memory interface
    //printSketch(dst2, "Heapify From Updatable");
    assertEquals(dst2.getEstimate(), src.getEstimate(), 0.0);

    //Wrap updatable
    WritableMemory wmem3 = WritableMemory.allocate(bytes);
    wmem2.copyTo(0, wmem3, 0, wmem2.getCapacity());
    HllSketch dst3 = HllSketch.writableWrap(wmem3);
    //printSketch(dst3, "WritableWrap From Updatable");
    assertEquals(dst3.getEstimate(), src.getEstimate(), 0.0);

    //Wrap updatable as Read-Only
    HllSketch dst4 = HllSketch.wrap(wmem3);
    assertEquals(dst4.getEstimate(), src.getEstimate(), 0.0);
  }

  static void printSketch(HllSketch sketch, String name) {
    println(name +":\n" + sketch.toString(true, true, true, false));
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
