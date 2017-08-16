/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.yahoo.memory.WritableDirectHandle;
import com.yahoo.memory.WritableMemory;


/**
 * @author Lee Rhodes
 */
public class DirectAuxHashMapTest {

  @Test
  public void checkGrow() {
    int lgConfigK = 4;
    TgtHllType tgtHllType = TgtHllType.HLL_4;
    int n = 8; //put lgConfigK == 4 into HLL mode
    int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgConfigK, tgtHllType);
    HllSketch hllSketch;
    try (WritableDirectHandle handle = WritableMemory.allocateDirect(bytes)) {
      WritableMemory wmem = handle.get();
      hllSketch = HllSketch.writableWrap(lgConfigK, tgtHllType, wmem);
      for (int i = 0; i < n; i++) {
        hllSketch.update(i);
      }
      hllSketch.couponUpdate(HllUtil.pair(7, 15)); //mock extreme values
      hllSketch.couponUpdate(HllUtil.pair(8, 15));
      hllSketch.couponUpdate(HllUtil.pair(9, 15));
      //println(hllSketch.toString(true, true, true, true));
      DirectHllArray dha = (DirectHllArray) hllSketch.hllSketchImpl;
      assertEquals(dha.getAuxHashMap().getLgAuxArrInts(), 2);
      assertTrue(hllSketch.isMemory());
      assertTrue(hllSketch.isOffHeap());

      //Check heapify
      byte[] byteArray = hllSketch.toCompactByteArray();
      HllSketch hllSketch2 = HllSketch.heapify(byteArray);
      HllArray ha = (HllArray) hllSketch2.hllSketchImpl;
      assertEquals(ha.getAuxHashMap().getLgAuxArrInts(), 2);
      assertEquals(ha.getAuxHashMap().getAuxCount(), 3);

      //Check wrap
      byteArray = hllSketch.toUpdatableByteArray();
      WritableMemory wmem2 = WritableMemory.wrap(byteArray);
      hllSketch2 = HllSketch.writableWrap(wmem2);
      //println(hllSketch2.toString(true, true, true, true));
      dha = (DirectHllArray) hllSketch2.hllSketchImpl;
      assertEquals(dha.getAuxHashMap().getLgAuxArrInts(), 2);
      assertEquals(dha.getAuxHashMap().getAuxCount(), 3);

      //Check grow to on-heap
      hllSketch.couponUpdate(HllUtil.pair(10, 15));
      //println(hllSketch.toString(true, true, true, true));
      dha = (DirectHllArray) hllSketch.hllSketchImpl;
      assertEquals(dha.getAuxHashMap().getLgAuxArrInts(), 3);
      assertEquals(dha.getAuxHashMap().getAuxCount(), 4);
      assertTrue(hllSketch.isMemory());
      assertFalse(hllSketch.isOffHeap());
    }
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    System.out.println(s); //disable here
  }

}
