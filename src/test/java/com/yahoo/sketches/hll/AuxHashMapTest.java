/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesStateException;

/**
 * @author Lee Rhodes
 */
public class AuxHashMapTest {

  @Test
  public void exerciseAux() {
    int lgK = 15;
    int lgU = 20;
    HllSketch sk = new HllSketch(lgK, TgtHllType.HLL_4);
    for (int i = 0; i < (1 << lgU); i++) { sk.update(i); }
    int curMin = sk.hllSketchImpl.getCurMin();
    println("HLL_4, lgK: " + lgK + ", lgU: " + lgU);
    println("CurMin: " + curMin);
    PairIterator itr = sk.getAuxIterator();
    println("Aux Array before SerDe.");
    println(itr.getHeader());
    while (itr.nextValid()) {
      println(itr.getString());
    }
    byte[] byteArr = sk.toByteArray();
    HllSketch sk2 = HllSketch.heapify(Memory.wrap(byteArr));
    assertEquals(sk.getEstimate(), sk2.getEstimate());

    PairIterator h4itr = sk.getIterator();
    println("\nMain Array: where (value - curMin) > 14. key/vals should match above.");
    println(h4itr.getHeader());
    while (h4itr.nextValid()) {
      if ((h4itr.getValue() - curMin) > 14) {
        println(h4itr.getString());
      }
    }
    h4itr = sk.getAuxIterator();
    println("\nAux Array after SerDe: should match above.");
    println(h4itr.getHeader());
    while (h4itr.nextAll()) {
      if (h4itr.getValue() > 14) {
        println(h4itr.getString());
      }
    }
    sk.toString(true);
  }

  @Test
  public void checkMustReplace() {
    AuxHashMap map = new AuxHashMap(3, 7);
    map.mustAdd(100, 5);
    int val = map.mustFindValueFor(100);
    assertEquals(val, 5);

    map.mustReplace(100, 10);
    val = map.mustFindValueFor(100);
    assertEquals(val, 10);

    try {
      map.mustReplace(101, 5);
      fail();
    } catch (SketchesStateException e) {
      //expected
    }
  }

  @Test
  public void checkGrowSpace() {
    AuxHashMap map = new AuxHashMap(3, 7);
    assertEquals(map.lgAuxArrSize, 3);
    for (int i = 1; i <= 7; i++) {
      map.mustAdd(i, i);
    }
    assertEquals(map.lgAuxArrSize, 4);

    byte[] byteArr = map.toByteArray();
    WritableMemory wmem = WritableMemory.wrap(byteArr);
    AuxHashMap map2 = AuxHashMap.heapify(wmem, 0, 7, 7);
    assertEquals(map2.lgAuxArrSize, 4);
  }

  @Test(expectedExceptions = SketchesStateException.class)
  public void checkExceptions1() {
    AuxHashMap map = new AuxHashMap(3, 7);
    map.mustAdd(100, 5);
    map.mustFindValueFor(101);
  }

  @Test(expectedExceptions = SketchesStateException.class)
  public void checkExceptions2() {
    AuxHashMap map = new AuxHashMap(3, 7);
    map.mustAdd(100, 5);
    map.mustAdd(100, 6);
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }

}
