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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;
import java.util.HashSet;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesReadOnlyException;
import org.apache.datasketches.hll.AbstractHllArray;
import org.apache.datasketches.hll.CurMode;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.IntMemorySegmentPairIterator;
import org.apache.datasketches.hll.PairIterator;
import org.apache.datasketches.hll.TgtHllType;
import org.testng.annotations.Test;

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

  private static void noWriteAccess(final TgtHllType tgtHllType, final int n) {
    final int lgConfigK = 8;
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgConfigK, tgtHllType);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    final HllSketch sk = new HllSketch(lgConfigK, tgtHllType, wseg);

    for (int i = 0; i < n; i++) { sk.update(i); }

    final HllSketch sk2 = HllSketch.wrap(wseg);
    try {
      sk2.update(1);
      fail();
    } catch (final SketchesReadOnlyException e) {
      //expected
    }
  }

  @Test
  public void checkCompactToUpdatable() {
    final int lgConfigK = 15;
    final int n = 1 << 20;
    final TgtHllType type = TgtHllType.HLL_4;

    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgConfigK, type);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    //create first direct updatable sketch
    final HllSketch sk = new HllSketch(lgConfigK, type, wseg);
    for (int i = 0; i < n; i++) { sk.update(i); }
    //Create compact byte arr
    final byte[] cByteArr = sk.toCompactByteArray(); //16496 = (auxStart)16424 + 72
    final MemorySegment cseg = MemorySegment.ofArray(cByteArr);
    //Create updatable byte arr
    final byte[] uByteArr = sk.toUpdatableByteArray(); //16936 = (auxStart)16424 + 512
    //get auxStart and auxArrInts for updatable
    final AbstractHllArray absArr = (AbstractHllArray)sk.hllSketchImpl;
    final int auxStart = absArr.auxStart;
    final int auxArrInts = 1 << absArr.getAuxHashMap().getLgAuxArrInts();
    //hash set to check result
    final HashSet<Integer> set = new HashSet<>();
    //create HashSet of values
    final PairIterator itr = new IntMemorySegmentPairIterator(uByteArr, auxStart, auxArrInts, lgConfigK);
    //println(itr.getHeader());
    int validCount = 0;
    while (itr.nextValid()) {
      set.add(itr.getPair());
      validCount++;
      //println(itr.getString());
    }

    //Wrap the compact image as read-only
    final HllSketch sk2 = HllSketch.wrap(cseg); //cseg is 16496
    //serialize it to updatable image
    final byte[] uByteArr2 = sk2.toUpdatableByteArray();
    final PairIterator itr2 = new IntMemorySegmentPairIterator(uByteArr2, auxStart, auxArrInts, lgConfigK);
    //println(itr2.getHeader());
    int validCount2 = 0;
    while (itr2.nextValid()) {
      final boolean exists = set.contains(itr2.getPair());
      if (exists) { validCount2++; }
      //println(itr2.getString());
    }
    assertEquals(validCount, validCount2);
  }

  @Test
  public void checkPutKxQ1_Misc() {
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(4, TgtHllType.HLL_4);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    final HllSketch sk = new HllSketch(4, TgtHllType.HLL_4, wseg);
    for (int i = 0; i < 8; i++) { sk.update(i); }
    assertTrue(sk.getCurMode() == CurMode.HLL);
    final AbstractHllArray absArr = (AbstractHllArray)sk.hllSketchImpl;
    absArr.putKxQ1(1.0);
    assertEquals(absArr.getKxQ1(), 1.0);
    absArr.putKxQ1(0.0);

    final MemorySegment seg = wseg;
    final HllSketch sk2 = HllSketch.wrap(seg);
    try {
      sk2.reset();
      fail();
    } catch (final SketchesArgumentException e) {
      //expected
    }
  }

  @Test
  public void checkToCompactByteArr() {
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(4, TgtHllType.HLL_4);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    final HllSketch sk = new HllSketch(4, TgtHllType.HLL_4, wseg);
    for (int i = 0; i < 8; i++) { sk.update(i); }
    final byte[] compByteArr = sk.toCompactByteArray();
    final MemorySegment compSeg = MemorySegment.ofArray(compByteArr);
    final HllSketch sk2 = HllSketch.wrap(compSeg);
    final byte[] compByteArr2 = sk2.toCompactByteArray();
    assertEquals(compByteArr2, compByteArr);
  }

  @Test
  public void checkToUpdatableByteArr() {
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(4, TgtHllType.HLL_4);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    final HllSketch sk = new HllSketch(4, TgtHllType.HLL_4, wseg);
    for (int i = 0; i < 8; i++) { sk.update(i); }
    final byte[] udByteArr = sk.toUpdatableByteArray();
    final byte[] compByteArr = sk.toCompactByteArray();
    final MemorySegment compSeg = MemorySegment.ofArray(compByteArr);
    final HllSketch sk2 = HllSketch.wrap(compSeg);
    final byte[] udByteArr2 = sk2.toUpdatableByteArray();
    assertEquals(udByteArr2, udByteArr);
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    //System.out.println(s); //disable here
  }

}
