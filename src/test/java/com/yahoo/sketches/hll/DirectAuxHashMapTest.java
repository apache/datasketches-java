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

import java.util.HashMap;

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
    int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgConfigK, tgtHllType);
    HllSketch hllSketch;
    try (WritableDirectHandle handle = WritableMemory.allocateDirect(bytes)) {
      WritableMemory wmem = handle.get();
      hllSketch = new HllSketch(lgConfigK, tgtHllType, wmem);
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
      assertTrue(hllSketch.isSameResource(wmem));

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
      DirectHllArray dha2 = (DirectHllArray) hllSketch2.hllSketchImpl;
      assertEquals(dha2.getAuxHashMap().getLgAuxArrInts(), 2);
      assertEquals(dha2.getAuxHashMap().getAuxCount(), 3);

      //Check grow to on-heap
      hllSketch.couponUpdate(HllUtil.pair(10, 15)); //puts it over the edge, must grow
      //println(hllSketch.toString(true, true, true, true));
      dha = (DirectHllArray) hllSketch.hllSketchImpl;
      assertEquals(dha.getAuxHashMap().getLgAuxArrInts(), 3);
      assertEquals(dha.getAuxHashMap().getAuxCount(), 4);
      assertTrue(hllSketch.isMemory());
      assertFalse(hllSketch.isOffHeap());
      assertFalse(hllSketch.isSameResource(wmem));
    }
  }

  @Test
  public void checkDiffToByteArr() {
    int lgK = 12; //this combination should create an Aux with ~18 exceptions
    int lgU = 19;
    TgtHllType type = TgtHllType.HLL_4;
    int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK, type);
    byte[] memByteArr = new byte[bytes];
    WritableMemory wmem = WritableMemory.wrap(memByteArr);
    HllSketch heapSk = new HllSketch(lgK, type);
    HllSketch dirSk = new HllSketch(lgK, type, wmem);
    for (int i = 0; i < (1 << lgU); i++) {
      heapSk.update(i);
      dirSk.update(i); //problem starts here.
    }
    AbstractHllArray heapHllArr = (AbstractHllArray) heapSk.hllSketchImpl;
    AbstractHllArray dirHllArr = (AbstractHllArray) dirSk.hllSketchImpl;
    assert dirHllArr instanceof DirectHllArray;
    AuxHashMap heapAux = heapHllArr.getAuxHashMap();
    assert heapAux instanceof HeapAuxHashMap;
    AuxHashMap dirAux = dirHllArr.getAuxHashMap();
    assert dirAux instanceof DirectAuxHashMap; //TOOD FAILS!
    println("HeapAuxCount: " + heapAux.getAuxCount());
    println("DirAuxCount: " + dirAux.getAuxCount());
    int heapCurMin = heapHllArr.getCurMin();
    int dirCurMin = dirHllArr.getCurMin();
    println("HeapCurMin: " + heapCurMin);
    println("DirCurMin: " + dirCurMin);


    PairIterator auxItr;
    auxItr = heapHllArr.getAuxIterator();
    println("\nHeap Pairs");
    //println(itr.getHeader());
    while (auxItr.nextValid()) {
      println("" + auxItr.getPair());
    }
    auxItr = dirHllArr.getAuxIterator();
    println("\nDirect Pairs");
    //println(itr.getHeader());
    while (auxItr.nextValid()) {
      println(""+ auxItr.getPair());
    }

    PairIterator hllItr;
    hllItr = heapSk.iterator();
    println("Heap HLL arr");
    println(hllItr.getHeader());
    while (hllItr.nextValid()) {
      if ((hllItr.getValue() - heapCurMin) > 14) {
        println(hllItr.getString() + ", " + hllItr.getPair());
      }
    }
    hllItr = dirSk.iterator();
    println("Direct HLL arr");
    println(hllItr.getHeader());
    while (hllItr.nextValid()) {
      if ((hllItr.getValue() - dirCurMin) > 14) {
        println(hllItr.getString() + ", " + hllItr.getPair());
      }
    }

    byte[] heapImg = heapSk.toUpdatableByteArray();
    Memory heapImgMem = Memory.wrap(heapImg);
    byte[] dirImg = dirSk.toUpdatableByteArray();
    Memory dirImgMem = Memory.wrap(dirImg);

    println("heapLen: " + heapImg.length + ", dirLen: " + dirImg.length
        + ", memObjLen: "+memByteArr.length);
    int auxStart = 40 + (1 << (lgK -1));
    println("AuxStart: " + auxStart);


    println(String.format("%14s%14s%14s", "dir wmem", "heap to b[]", "direct to b[]"));
    for (int i = auxStart; i < heapImg.length; i += 4) {
      println(String.format("%14d%14d%14d",
          wmem.getInt(i), heapImgMem.getInt(i), dirImgMem.getInt(i)));
      assert memByteArr[i] == heapImg[i];
      assert heapImg[i] == dirImg[i] : "i: " + i;
    }
    assertEquals(heapImg, dirImg);
  }

  @Test
  public void exerciseHeapAndDirectAux() {
    initSketchAndMap(true, true);  //direct, compact
    initSketchAndMap(false, true); //heap, compact
    initSketchAndMap(true, false); //direct, updatable
    initSketchAndMap(false, false); //heap, updatable
  }

  static void initSketchAndMap(boolean direct, boolean compact) {
    int lgK = 15; //this combination should create an Aux with ~18 exceptions
    int lgU = 20;
    println("HLL_4, lgK: " + lgK + ", lgU: " + lgU);
    HashMap<Integer, Integer> map = new HashMap<>();

    //create sketch
    HllSketch sketch;
    if (direct) {
      int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK, TgtHllType.HLL_4);
      WritableMemory wmem = WritableMemory.allocate(bytes);
      sketch = new HllSketch(lgK, TgtHllType.HLL_4, wmem);
    } else {
      sketch = new HllSketch(lgK, TgtHllType.HLL_4);
    }
    for (int i = 0; i < (1 << lgU); i++) { sketch.update(i); }

    //check Ser Bytes
    assertEquals(sketch.getUpdatableSerializationBytes(), 40 + (1 << (lgK - 1))
        + (4 << LG_AUX_ARR_INTS[lgK]) );

    //extract the auxHashMap entries into a HashMap for easy checking
    //extract direct aux iterator
    AbstractHllArray absDirectHllArr = (AbstractHllArray) sketch.hllSketchImpl;

    //the auxHashMap must exist for this test
    AuxHashMap auxMap = absDirectHllArr.getAuxHashMap();
    int auxCount = auxMap.getAuxCount();
    assertEquals(auxMap.getCompactSizeBytes(), auxCount << 2);
    int auxArrInts = 1 << auxMap.getLgAuxArrInts();
    assertEquals(auxMap.getUpdatableSizeBytes(), auxArrInts << 2);

    PairIterator itr = absDirectHllArr.getAuxIterator();

    println("Source Aux Array.");
    println(itr.getHeader());
    while (itr.nextValid()) {
      map.put(itr.getSlot(), itr.getValue());  //create the aux reference map
      println(itr.getString());
    }
    double est = sketch.getEstimate();
    println("\nHLL Array of original sketch: should match Source Aux Array.");
    checkHllArr(sketch, map); //check HLL arr consistencies

    //serialize the direct sk as compact
    byte[] byteArr = (compact) ? sketch.toCompactByteArray() : sketch.toUpdatableByteArray();

    //Heapify the byteArr image & check estimate
    HllSketch heapSk = HllSketch.heapify(Memory.wrap(byteArr));
    assertEquals(heapSk.getEstimate(), est, 0.0);
    println("\nAux Array of heapified serialized sketch.");
    checkAux(heapSk, map); //check Aux consistencies
    println("\nHLL Array of heapified serialized sketch.");
    checkHllArr(heapSk, map); //check HLL arr consistencies

    //Wrap the image as read-only & check estimate
    HllSketch wrapSk = HllSketch.wrap(Memory.wrap(byteArr));
    assertEquals(wrapSk.getEstimate(), est, 0.0);
    println("\nAux Array of wrapped RO serialized sketch.");
    checkAux(wrapSk, map);
    println("\nHLL Array of wrapped RO serialized sketch.");
    checkHllArr(wrapSk, map);

    println(wrapSk.toString(false, false, true, true));
  }

  //check HLL array consistencies with the map
  static void checkHllArr(HllSketch sk, HashMap<Integer,Integer> map) {
    //extract aux iterator, which must exist for this test
    AbstractHllArray absHllArr = (AbstractHllArray) sk.hllSketchImpl;
    int curMin = absHllArr.getCurMin();
    //println("CurMin: " + curMin);
    PairIterator hllArrItr = sk.iterator();
    println(hllArrItr.getHeader());
    while (hllArrItr.nextValid()) {
      final int hllArrVal = hllArrItr.getValue();
      if ((hllArrItr.getValue() - curMin) > 14) {
        final int mapVal = map.get(hllArrItr.getSlot());
        println(hllArrItr.getString());
        assertEquals(hllArrVal, mapVal);
      }
    }
  }

  //Check Aux consistencies to the map
  static void checkAux(HllSketch sk, HashMap<Integer,Integer> map) {
    AbstractHllArray absHllArr = (AbstractHllArray) sk.hllSketchImpl;
    //extract aux iterator, which must exist for this test
    PairIterator heapAuxItr = absHllArr.getAuxIterator();
    println(heapAuxItr.getHeader());
    while (heapAuxItr.nextValid()) {
      final int afterVal = heapAuxItr.getValue();
      if (afterVal > 14) {
        println(heapAuxItr.getString());
        int auxSlot = heapAuxItr.getSlot();
        assert map.containsKey(auxSlot);
        final int beforeVal = map.get(heapAuxItr.getSlot());
        assertEquals(afterVal, beforeVal);
      }
    }
  }

  @Test
  public void checkDirectReadOnlyCompactAux() {
    int lgK = 15; //this combination should create an Aux with ~18 exceptions
    int lgU = 20;
    HllSketch sk = new HllSketch(lgK, TgtHllType.HLL_4);
    for (int i = 0; i < (1 << lgU); i++) { sk.update(i); }

  }

  @Test
  public void checkMustReplace() {
    int lgK = 7;
    int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK, TgtHllType.HLL_4);
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
