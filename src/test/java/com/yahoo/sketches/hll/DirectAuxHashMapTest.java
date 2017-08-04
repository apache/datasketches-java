/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.PreambleUtil.HLL_PREINTS;
import static com.yahoo.sketches.hll.PreambleUtil.insertCurMode;
import static com.yahoo.sketches.hll.PreambleUtil.insertFamilyId;
import static com.yahoo.sketches.hll.PreambleUtil.insertFlags;
import static com.yahoo.sketches.hll.PreambleUtil.insertLgArr;
import static com.yahoo.sketches.hll.PreambleUtil.insertLgK;
import static com.yahoo.sketches.hll.PreambleUtil.insertListCount;
import static com.yahoo.sketches.hll.PreambleUtil.insertPreInts;
import static com.yahoo.sketches.hll.PreambleUtil.insertSerVer;
import static com.yahoo.sketches.hll.PreambleUtil.insertTgtHllType;

import org.testng.annotations.Test;

import com.yahoo.memory.WritableDirectHandle;
import com.yahoo.memory.WritableMemory;


/**
 * @author Lee Rhodes
 */
public class DirectAuxHashMapTest {

  @Test
  public void checkGrow() {
    try (WritableDirectHandle handle = WritableMemory.allocateDirect(48 + 16)) {
      WritableMemory wmem = handle.get();
      Object memObj = wmem.getArray();
      long memAdd = wmem.getCumulativeOffset(0L);
      wmem.clear();
      insertPreInts(memObj, memAdd, HLL_PREINTS);
      insertSerVer(memObj, memAdd);
      insertFamilyId(memObj, memAdd);
      insertLgK(memObj, memAdd, 4);
      insertLgArr(memObj, memAdd, 2);
      insertFlags(memObj, memAdd, (byte) 0);
      insertListCount(memObj, memAdd, (byte) 0);
      insertCurMode(memObj, memAdd, CurMode.HLL);
      insertTgtHllType(memObj, memAdd, TgtHllType.HLL_4);
      DirectAuxHashMap map = new DirectAuxHashMap(wmem, false);
      map.mustAdd(2, 1);
      map.mustAdd(4, 2);
      map.mustAdd(6, 3);
      println("Direct: " + map.isDirect()); //Before grow
      PairIterator itr = map.getAuxIterator();
      println(itr.getHeader());
      while (itr.nextAll()) {
        println(itr.getString());
      }

      map.mustAdd(8, 4);
      println("Direct: " + map.isDirect()); //After grow
      itr = map.getAuxIterator();
      println(itr.getHeader());
      while (itr.nextAll()) {
        println(itr.getString());
      }
    }
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }

}
