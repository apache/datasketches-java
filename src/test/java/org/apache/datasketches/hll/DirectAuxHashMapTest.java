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

import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static org.apache.datasketches.hll.HllUtil.LG_AUX_ARR_INTS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.HashMap;

import org.apache.datasketches.common.SketchesStateException;
import org.apache.datasketches.hll.AbstractHllArray;
import org.apache.datasketches.hll.AuxHashMap;
import org.apache.datasketches.hll.DirectAuxHashMap;
import org.apache.datasketches.hll.DirectHllArray;
import org.apache.datasketches.hll.HeapAuxHashMap;
import org.apache.datasketches.hll.HllArray;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.HllUtil;
import org.apache.datasketches.hll.PairIterator;
import org.apache.datasketches.hll.TgtHllType;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class DirectAuxHashMapTest {

  @Test
  public void checkGrow() {
    final int lgConfigK = 4;
    final TgtHllType tgtHllType = TgtHllType.HLL_4;
    final int n = 8; //put lgConfigK == 4 into HLL mode
    final long bytes = HllSketch.getMaxUpdatableSerializationBytes(lgConfigK, tgtHllType);
    HllSketch hllSketch;
    final Arena arena = Arena.ofConfined();
    final MemorySegment wseg = arena.allocate(bytes);

    hllSketch = new HllSketch(lgConfigK, tgtHllType, wseg);
    for (int i = 0; i < n; i++) {
      hllSketch.update(i);
    }
    hllSketch.couponUpdate(HllUtil.pair(7, 15)); //mock extreme values
    hllSketch.couponUpdate(HllUtil.pair(8, 15));
    hllSketch.couponUpdate(HllUtil.pair(9, 15));
    println(hllSketch.toString(true, true, true, true));
    DirectHllArray dha = (DirectHllArray) hllSketch.hllSketchImpl;
    assertEquals(dha.getAuxHashMap().getLgAuxArrInts(), 2);
    assertTrue(hllSketch.hasMemorySegment());
    assertTrue(hllSketch.isOffHeap());
    assertTrue(hllSketch.isSameResource(wseg));

    //Check heapify
    byte[] byteArray = hllSketch.toCompactByteArray();
    HllSketch hllSketch2 = HllSketch.heapify(byteArray);
    final HllArray ha = (HllArray) hllSketch2.hllSketchImpl;
    assertEquals(ha.getAuxHashMap().getLgAuxArrInts(), 2);
    assertEquals(ha.getAuxHashMap().getAuxCount(), 3);

    //Check wrap
    byteArray = hllSketch.toUpdatableByteArray();
    final MemorySegment wseg2 = MemorySegment.ofArray(byteArray);
    hllSketch2 = HllSketch.writableWrap(wseg2);
    println(hllSketch2.toString(true, true, true, true));
    final DirectHllArray dha2 = (DirectHllArray) hllSketch2.hllSketchImpl;
    assertEquals(dha2.getAuxHashMap().getLgAuxArrInts(), 2);
    assertEquals(dha2.getAuxHashMap().getAuxCount(), 3);

    //Check grow to on-heap
    hllSketch.couponUpdate(HllUtil.pair(10, 15)); //puts it over the edge, must grow
    println(hllSketch.toString(true, true, true, true));
    dha = (DirectHllArray) hllSketch.hllSketchImpl;
    assertEquals(dha.getAuxHashMap().getLgAuxArrInts(), 3);
    assertEquals(dha.getAuxHashMap().getAuxCount(), 4);
    assertTrue(hllSketch.hasMemorySegment());
    assertFalse(hllSketch.isOffHeap());
    assertFalse(hllSketch.isSameResource(wseg));
    arena.close();
    assertFalse(wseg.scope().isAlive());
  }

  @Test
  public void checkDiffToByteArr() {
    final int lgK = 12; //this combination should create an Aux with ~18 exceptions
    final int lgU = 19;
    final TgtHllType type = TgtHllType.HLL_4;
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK, type);
    final byte[] segByteArr = new byte[bytes];
    final MemorySegment wseg = MemorySegment.ofArray(segByteArr);
    final HllSketch heapSk = new HllSketch(lgK, type);
    final HllSketch dirSk = new HllSketch(lgK, type, wseg);
    for (int i = 0; i < (1 << lgU); i++) {
      heapSk.update(i);
      dirSk.update(i); //problem starts here.
    }
    final AbstractHllArray heapHllArr = (AbstractHllArray) heapSk.hllSketchImpl;
    final AbstractHllArray dirHllArr = (AbstractHllArray) dirSk.hllSketchImpl;
    assert dirHllArr instanceof DirectHllArray;
    final AuxHashMap heapAux = heapHllArr.getAuxHashMap();
    assert heapAux instanceof HeapAuxHashMap;
    final AuxHashMap dirAux = dirHllArr.getAuxHashMap();
    assert dirAux instanceof DirectAuxHashMap;
    println("HeapAuxCount: " + heapAux.getAuxCount());
    println("DirAuxCount: " + dirAux.getAuxCount());
    final int heapCurMin = heapHllArr.getCurMin();
    final int dirCurMin = dirHllArr.getCurMin();
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

    final byte[] heapImg = heapSk.toUpdatableByteArray();
    final MemorySegment heapImgSeg = MemorySegment.ofArray(heapImg);
    final byte[] dirImg = dirSk.toUpdatableByteArray();
    final MemorySegment dirImgSeg = MemorySegment.ofArray(dirImg);

    println("heapLen: " + heapImg.length + ", dirLen: " + dirImg.length
        + ", segObjLen: "+segByteArr.length);
    final int auxStart = 40 + (1 << (lgK -1));
    println("AuxStart: " + auxStart);


    println(String.format("%14s%14s%14s", "dir wseg", "heap to b[]", "direct to b[]"));
    for (int i = auxStart; i < heapImg.length; i += 4) {
      println(String.format("%14d%14d%14d",
          wseg.get(JAVA_INT_UNALIGNED, i), heapImgSeg.get(JAVA_INT_UNALIGNED, i), dirImgSeg.get(JAVA_INT_UNALIGNED, i)));
      assert segByteArr[i] == heapImg[i];
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

  static void initSketchAndMap(final boolean direct, final boolean compact) {
    final int lgK = 15; //this combination should create an Aux with ~18 exceptions
    final int lgU = 20;
    println("HLL_4, lgK: " + lgK + ", lgU: " + lgU);
    final HashMap<Integer, Integer> map = new HashMap<>();

    //create sketch
    HllSketch sketch;
    if (direct) {
      final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK, TgtHllType.HLL_4);
      final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
      sketch = new HllSketch(lgK, TgtHllType.HLL_4, wseg);
    } else {
      sketch = new HllSketch(lgK, TgtHllType.HLL_4);
    }
    for (int i = 0; i < (1 << lgU); i++) { sketch.update(i); }

    //check Ser Bytes
    assertEquals(sketch.getUpdatableSerializationBytes(), 40 + (1 << (lgK - 1)) + (4 << LG_AUX_ARR_INTS[lgK]) );

    //extract the auxHashMap entries into a HashMap for easy checking
    //extract direct aux iterator
    final AbstractHllArray absDirectHllArr = (AbstractHllArray) sketch.hllSketchImpl;

    //the auxHashMap must exist for this test
    final AuxHashMap auxMap = absDirectHllArr.getAuxHashMap();
    final int auxCount = auxMap.getAuxCount();
    assertEquals(auxMap.getCompactSizeBytes(), auxCount << 2);
    final int auxArrInts = 1 << auxMap.getLgAuxArrInts();
    assertEquals(auxMap.getUpdatableSizeBytes(), auxArrInts << 2);

    final PairIterator itr = absDirectHllArr.getAuxIterator();

    println("Source Aux Array.");
    println(itr.getHeader());
    while (itr.nextValid()) {
      map.put(itr.getSlot(), itr.getValue());  //create the aux reference map
      println(itr.getString());
    }
    final double est = sketch.getEstimate();
    println("\nHLL Array of original sketch: should match Source Aux Array.");
    checkHllArr(sketch, map); //check HLL arr consistencies

    //serialize the direct sk as compact
    final byte[] byteArr = (compact) ? sketch.toCompactByteArray() : sketch.toUpdatableByteArray();

    //Heapify the byteArr image & check estimate
    final HllSketch heapSk = HllSketch.heapify(MemorySegment.ofArray(byteArr));
    assertEquals(heapSk.getEstimate(), est, 0.0);
    println("\nAux Array of heapified serialized sketch.");
    checkAux(heapSk, map); //check Aux consistencies
    println("\nHLL Array of heapified serialized sketch.");
    checkHllArr(heapSk, map); //check HLL arr consistencies

    //Wrap the image as read-only & check estimate
    final HllSketch wrapSk = HllSketch.wrap(MemorySegment.ofArray(byteArr));
    assertEquals(wrapSk.getEstimate(), est, 0.0);
    println("\nAux Array of wrapped RO serialized sketch.");
    checkAux(wrapSk, map);
    println("\nHLL Array of wrapped RO serialized sketch.");
    checkHllArr(wrapSk, map);

    println(wrapSk.toString(false, false, true, true));
  }

  //check HLL array consistencies with the map
  static void checkHllArr(final HllSketch sk, final HashMap<Integer,Integer> map) {
    //extract aux iterator, which must exist for this test
    final AbstractHllArray absHllArr = (AbstractHllArray) sk.hllSketchImpl;
    final int curMin = absHllArr.getCurMin();
    //println("CurMin: " + curMin);
    final PairIterator hllArrItr = sk.iterator();
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
  static void checkAux(final HllSketch sk, final HashMap<Integer,Integer> map) {
    final AbstractHllArray absHllArr = (AbstractHllArray) sk.hllSketchImpl;
    //extract aux iterator, which must exist for this test
    final PairIterator heapAuxItr = absHllArr.getAuxIterator();
    println(heapAuxItr.getHeader());
    while (heapAuxItr.nextValid()) {
      final int afterVal = heapAuxItr.getValue();
      if (afterVal > 14) {
        println(heapAuxItr.getString());
        final int auxSlot = heapAuxItr.getSlot();
        assert map.containsKey(auxSlot);
        final int beforeVal = map.get(heapAuxItr.getSlot());
        assertEquals(afterVal, beforeVal);
      }
    }
  }

  @Test
  public void checkDirectReadOnlyCompactAux() {
    final int lgK = 15; //this combination should create an Aux with ~18 exceptions
    final int lgU = 20;
    final HllSketch sk = new HllSketch(lgK, TgtHllType.HLL_4);
    for (int i = 0; i < (1 << lgU); i++) { sk.update(i); }

  }

  @Test
  public void checkMustReplace() {
    final int lgK = 7;
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK, TgtHllType.HLL_4);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    final HllSketch sk = new HllSketch(lgK, TgtHllType.HLL_4, wseg);
    for (int i = 0; i < 25; i++) { sk.update(i); }
    final DirectHllArray dHllArr = (DirectHllArray) sk.hllSketchImpl;
    final AuxHashMap map = dHllArr.getNewAuxHashMap();
    map.mustAdd(100, 5);
    int val = map.mustFindValueFor(100);
    assertEquals(val, 5);

    map.mustReplace(100, 10);
    val = map.mustFindValueFor(100);
    assertEquals(val, 10);

    assertTrue(map.hasMemorySegment());
    assertFalse(map.isOffHeap());
    assertNull(map.copy());
    assertNull(map.getAuxIntArr());

    try {
      map.mustAdd(100, 12);
      fail();
    } catch (final SketchesStateException e) {
      //expected
    }

    try {
      map.mustFindValueFor(101);
      fail();
    } catch (final SketchesStateException e) {
      //expected
    }

    try {
      map.mustReplace(101, 5);
      fail();
    } catch (final SketchesStateException e) {
      //expected
    }
  }


  /**
   * @param s value to print
   */
  static void println(final String s) {
    //System.out.println(s); //disable here
  }

}
