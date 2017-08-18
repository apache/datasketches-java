/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllUtil.LG_AUX_ARR_INTS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableDirectHandle;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesStateException;


/**
 * @author Lee Rhodes
 */
public class DirectAuxHashMapTest {

  @Test
  public void checkGrow() {
    int lgConfigK = 4;
    TgtHllType tgtHllType = TgtHllType.HLL_4;
    int n = 8; //put lgConfigK == 4 into HLL mode
    int bytes = BaseHllSketch.getMaxUpdatableSerializationBytes(lgConfigK, tgtHllType);
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

  @Test
  public void exerciseDirectAux() {
    int lgK = 15; //this combination should create an Aux with ~18 exceptions
    int lgU = 20;
    int bytes = BaseHllSketch.getMaxUpdatableSerializationBytes(lgK, TgtHllType.HLL_4);
    WritableMemory wmem = WritableMemory.allocate(bytes);
    HllSketch sk = new HllSketch(lgK, TgtHllType.HLL_4, wmem);
    for (int i = 0; i < (1 << lgU); i++) { sk.update(i); }
    AbstractHllArray absHll = (AbstractHllArray) sk.hllSketchImpl;
    int curMin = absHll.getCurMin();
    println("HLL_4, lgK: " + lgK + ", lgU: " + lgU);
    println("CurMin: " + curMin);
    PairIterator itr = absHll.getAuxIterator();
    if (itr != null) {
      println("Aux Array before SerDe.");
      println(itr.getHeader());
      while (itr.nextValid()) {
        println(itr.getString());
      }
    }
    byte[] byteArr = sk.toCompactByteArray();
    HllSketch sk2 = HllSketch.heapify(Memory.wrap(byteArr));
    assertEquals(sk.getEstimate(), sk2.getEstimate());

    assertEquals(sk.getUpdatableSerializationBytes(), 40 + (1 << (lgK - 1))
        + ((itr == null)? 0 : (4 << LG_AUX_ARR_INTS[lgK])));

    PairIterator h4itr = sk.getIterator();
    println("\nMain Array: where (value - curMin) > 14. key/vals should match above.");
    println(h4itr.getHeader());
    while (h4itr.nextValid()) {
      if ((h4itr.getValue() - curMin) > 14) {
        println(h4itr.getString());
      }
    }
    PairIterator auxItr = absHll.getAuxIterator();
    if (auxItr != null) {
      println("\nAux Array after SerDe: should match above.");
      println(auxItr.getHeader());
      while (auxItr.nextAll()) {
        if (auxItr.getValue() > 14) {
          println(auxItr.getString());
        }
      }
    }
    sk.toString(true, true, true, false);
  }

  @Test
  public void checkMustReplace() {
    int lgK = 7;
    int bytes = BaseHllSketch.getMaxUpdatableSerializationBytes(lgK, TgtHllType.HLL_4);
    WritableMemory wmem = WritableMemory.allocate(bytes);
    HllSketch sk = new HllSketch(lgK, TgtHllType.HLL_4, wmem);
    for (int i = 0; i < 25; i++) { sk.update(i); }
    DirectHllArray dHllArr = (DirectHllArray) sk.hllSketchImpl;
    AuxHashMap map = dHllArr.getNewAuxHashMap();
    map.mustAdd(100, 5);
    int val = map.mustFindValueFor(100);
    assertEquals(val, 5);

    map.mustReplace(100, 10);
    val = map.mustFindValueFor(100);
    assertEquals(val, 10);

    assertTrue(map.isMemory());
    assertFalse(map.isOffHeap());
    assertNull(map.copy());
    assertNull(map.getAuxIntArr());

    try {
      map.mustAdd(100, 12);
      fail();
    } catch (SketchesStateException e) {
      //expected
    }


    try {
      map.mustFindValueFor(101);
      fail();
    } catch (SketchesStateException e) {
      //expected
    }

    try {
      map.mustReplace(101, 5);
      fail();
    } catch (SketchesStateException e) {
      //expected
    }
  }


  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }

}
