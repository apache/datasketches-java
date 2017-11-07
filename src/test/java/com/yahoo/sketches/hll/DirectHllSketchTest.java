/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.HashSet;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * @author Lee Rhodes
 */
public class DirectHllSketchTest {

  @Test
  public void checkNoWriteAccess() {
    noWriteAccess(TgtHllType.HLL_4, 7);
    noWriteAccess(TgtHllType.HLL_4, 24);
    noWriteAccess(TgtHllType.HLL_4, 25);
    noWriteAccess(TgtHllType.HLL_6, 25);
    noWriteAccess(TgtHllType.HLL_8, 25);
  }

  private static void noWriteAccess(TgtHllType tgtHllType, int n) {
    int lgConfigK = 8;
    int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgConfigK, tgtHllType);
    WritableMemory wmem = WritableMemory.allocate(bytes);
    HllSketch sk = new HllSketch(lgConfigK, tgtHllType, wmem);

    for (int i = 0; i < n; i++) { sk.update(i); }

    HllSketch sk2 = HllSketch.wrap(wmem);
    try {
      sk2.update(1);
      fail();
    } catch (SketchesArgumentException e) {
      //expected
    }
  }

  @Test
  public void checkCompactToUpdatable() {
    int lgConfigK = 15;
    int n = 1 << 20;
    TgtHllType type = TgtHllType.HLL_4;

    int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgConfigK, type);
    WritableMemory wmem = WritableMemory.allocate(bytes);
    //create first direct updatable sketch
    HllSketch sk = new HllSketch(lgConfigK, type, wmem);
    for (int i = 0; i < n; i++) { sk.update(i); }
    //Create compact byte arr
    byte[] cByteArr = sk.toCompactByteArray(); //16496 = (auxStart)16424 + 72
    Memory cmem = Memory.wrap(cByteArr);
    //Create updatable byte arr
    byte[] uByteArr = sk.toUpdatableByteArray(); //16936 = (auxStart)16424 + 512
    //get auxStart and auxArrInts for updatable
    AbstractHllArray absArr = (AbstractHllArray)sk.hllSketchImpl;
    int auxStart = absArr.auxStart;
    int auxArrInts = 1 << absArr.getAuxHashMap().getLgAuxArrInts();
    //hash set to check result
    HashSet<Integer> set = new HashSet<>();
    //create HashSet of values
    PairIterator itr = new IntMemoryPairIterator(uByteArr, auxStart, auxArrInts, lgConfigK);
    //println(itr.getHeader());
    int validCount = 0;
    while (itr.nextValid()) {
      set.add(itr.getPair());
      validCount++;
      //println(itr.getString());
    }

    //Wrap the compact image as read-only
    HllSketch sk2 = HllSketch.wrap(cmem); //cmem is 16496
    //serialize it to updatable image
    byte[] uByteArr2 = sk2.toUpdatableByteArray();
    PairIterator itr2 = new IntMemoryPairIterator(uByteArr2, auxStart, auxArrInts, lgConfigK);
    //println(itr2.getHeader());
    int validCount2 = 0;
    while (itr2.nextValid()) {
      boolean exists = set.contains(itr2.getPair());
      if (exists) { validCount2++; }
      //println(itr2.getString());
    }
    assertEquals(validCount, validCount2);
  }

  @Test
  public void checkPutKxQ1_Misc() {
    int bytes = HllSketch.getMaxUpdatableSerializationBytes(4, TgtHllType.HLL_4);
    WritableMemory wmem = WritableMemory.allocate(bytes);
    HllSketch sk = new HllSketch(4, TgtHllType.HLL_4, wmem);
    for (int i = 0; i < 8; i++) { sk.update(i); }
    assertTrue(sk.getCurMode() == CurMode.HLL);
    AbstractHllArray absArr = (AbstractHllArray)sk.hllSketchImpl;
    absArr.putKxQ1(1.0);
    assertEquals(absArr.getKxQ1(), 1.0);
    absArr.putKxQ1(0.0);

    Memory mem = wmem;
    HllSketch sk2 = HllSketch.wrap(mem);
    try {
      sk2.reset();
      fail();
    } catch (SketchesArgumentException e) {
      //expected
    }
  }

  @Test
  public void checkToCompactByteArr() {
    int bytes = HllSketch.getMaxUpdatableSerializationBytes(4, TgtHllType.HLL_4);
    WritableMemory wmem = WritableMemory.allocate(bytes);
    HllSketch sk = new HllSketch(4, TgtHllType.HLL_4, wmem);
    for (int i = 0; i < 8; i++) { sk.update(i); }
    byte[] compByteArr = sk.toCompactByteArray();
    Memory compMem = Memory.wrap(compByteArr);
    HllSketch sk2 = HllSketch.wrap(compMem);
    byte[] compByteArr2 = sk2.toCompactByteArray();
    assertEquals(compByteArr2, compByteArr);
  }

  @Test
  public void checkToUpdatableByteArr() {
    int bytes = HllSketch.getMaxUpdatableSerializationBytes(4, TgtHllType.HLL_4);
    WritableMemory wmem = WritableMemory.allocate(bytes);
    HllSketch sk = new HllSketch(4, TgtHllType.HLL_4, wmem);
    for (int i = 0; i < 8; i++) { sk.update(i); }
    byte[] udByteArr = sk.toUpdatableByteArray();
    byte[] compByteArr = sk.toCompactByteArray();
    Memory compMem = Memory.wrap(compByteArr);
    HllSketch sk2 = HllSketch.wrap(compMem);
    byte[] udByteArr2 = sk2.toUpdatableByteArray();
    assertEquals(udByteArr2, udByteArr);
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
