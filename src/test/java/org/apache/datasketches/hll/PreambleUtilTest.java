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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.apache.datasketches.hll.PreambleUtil.EMPTY_FLAG_MASK;
import static org.apache.datasketches.hll.PreambleUtil.FAMILY_BYTE;
import static org.apache.datasketches.hll.PreambleUtil.SER_VER_BYTE;
import static org.apache.datasketches.hll.PreambleUtil.extractFlags;
import static org.apache.datasketches.hll.PreambleUtil.insertFamilyId;
import static org.apache.datasketches.hll.PreambleUtil.insertPreInts;
import static org.apache.datasketches.hll.PreambleUtil.insertSerVer;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.hll.CurMode;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.PreambleUtil;
import org.apache.datasketches.hll.TgtHllType;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class PreambleUtilTest {

  //@Test
  public void preambleToString() { // Check Visually
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(8, TgtHllType.HLL_4);
    final byte[] byteArr1 = new byte[bytes];
    final MemorySegment wseg1 = MemorySegment.ofArray(byteArr1);
    final HllSketch sk = new HllSketch(8, TgtHllType.HLL_4, wseg1);
    byte[] byteArr2 = sk.toCompactByteArray();
    MemorySegment wseg2 = MemorySegment.ofArray(byteArr2);

    assertEquals(sk.getCurMode(), CurMode.LIST);
    assertTrue(sk.isEmpty());
    String s = HllSketch.toString(byteArr2); //empty sketch output
    println(s);
    println("LgArr: " + PreambleUtil.extractLgArr(wseg2));
    println("Empty: " + PreambleUtil.extractEmptyFlag(wseg2));
    println("Serialization Bytes: " + wseg2.byteSize());

    for (int i = 0; i < 7; i++) { sk.update(i); }
    byteArr2 = sk.toCompactByteArray();
    wseg2 = MemorySegment.ofArray(byteArr2);
    assertEquals(sk.getCurMode(), CurMode.LIST);
    assertFalse(sk.isEmpty());
    s = HllSketch.toString(byteArr2);
    println(s);
    println("LgArr: " + PreambleUtil.extractLgArr(wseg2));
    println("Empty: " + PreambleUtil.extractEmptyFlag(wseg2));
    println("Serialization Bytes: " + wseg2.byteSize());

    for (int i = 7; i < 24; i++) { sk.update(i); }
    byteArr2 = sk.toCompactByteArray();
    wseg2 = MemorySegment.ofArray(byteArr2);
    assertEquals(sk.getCurMode(), CurMode.SET);
    s = HllSketch.toString(byteArr2);
    println(s);
    println("LgArr: " + PreambleUtil.extractLgArr(wseg2));
    println("Empty: " + PreambleUtil.extractEmptyFlag(wseg2));
    println("Serialization Bytes: " + wseg2.byteSize());

    sk.update(24);
    byteArr2 = sk.toCompactByteArray();
    wseg2 = MemorySegment.ofArray(byteArr2);
    assertEquals(sk.getCurMode(), CurMode.HLL);
    s = HllSketch.toString(MemorySegment.ofArray(byteArr2));
    println(s);
    println("LgArr: " + PreambleUtil.extractLgArr(wseg2));
    println("Empty: " + PreambleUtil.extractEmptyFlag(wseg2));
    println("Serialization Bytes: " + wseg2.byteSize());
  }

  @Test
  public void checkCompactFlag() {
    final HllSketch sk = new HllSketch(7);
    final byte[] segObj = sk.toCompactByteArray();
    final MemorySegment wseg = MemorySegment.ofArray(segObj);
    boolean compact = PreambleUtil.extractCompactFlag(wseg);
    assertTrue(compact);

    PreambleUtil.insertCompactFlag(wseg, false);
    compact = PreambleUtil.extractCompactFlag(wseg);
    assertFalse(compact);
  }

  @SuppressWarnings("unused")
  @Test
  public void checkCorruptMemorySegmentInput() {
    final HllSketch sk = new HllSketch(12);
    byte[] segObj = sk.toCompactByteArray();
    MemorySegment wseg = MemorySegment.ofArray(segObj);
    HllSketch bad;

    //checkFamily
    try {
      wseg.set(JAVA_BYTE, FAMILY_BYTE, (byte) 0); //corrupt, should be 7
      bad = HllSketch.heapify(wseg);
      fail();
    } catch (final SketchesArgumentException e) { /* OK */ }
    insertFamilyId(wseg); //corrected

    //check SerVer
    try {
      wseg.set(JAVA_BYTE, SER_VER_BYTE, (byte) 0); //corrupt, should be 1
      bad = HllSketch.heapify(wseg);
      fail();
    } catch (final SketchesArgumentException e) { /* OK */ }
    insertSerVer(wseg); //corrected

    //check bad PreInts
    try {
      insertPreInts(wseg, 0); //corrupt, should be 2
      bad = HllSketch.heapify(wseg);
      fail();
    } catch (final SketchesArgumentException e) { /* OK */ }
    insertPreInts(wseg, 2); //corrected

    //check wrong PreInts and LIST
    try {
      insertPreInts(wseg, 3); //corrupt, should be 2
      bad = HllSketch.heapify(wseg);
      fail();
    } catch (final SketchesArgumentException e) { /* OK */ }
    insertPreInts(wseg, 2); //corrected

    //move to Set mode
    for (int i = 1; i <= 15; i++) { sk.update(i); }
    segObj = sk.toCompactByteArray();
    wseg = MemorySegment.ofArray(segObj);

    //check wrong PreInts and SET
    try {
      insertPreInts(wseg, 2); //corrupt, should be 3
      bad = HllSketch.heapify(wseg);
      fail();
    } catch (final SketchesArgumentException e) { /* OK */ }
    insertPreInts(wseg, 3); //corrected

    //move to HLL mode
    for (int i = 15; i <= 1000; i++) { sk.update(i); }
    segObj = sk.toCompactByteArray();
    wseg = MemorySegment.ofArray(segObj);

    //check wrong PreInts and HLL
    try {
      insertPreInts(wseg, 2); //corrupt, should be 10
      bad = HllSketch.heapify(wseg);
      fail();
    } catch (final SketchesArgumentException e) { /* OK */ }
    insertPreInts(wseg, 10); //corrected
  }

  @SuppressWarnings("unused")
  @Test
  public void checkExtractFlags() {
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(4, TgtHllType.HLL_4);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    final Object segObj = wseg.toArray(JAVA_BYTE);
    final HllSketch sk = new HllSketch(4, TgtHllType.HLL_4, wseg);
    final int flags = extractFlags(wseg);
    assertEquals(flags, EMPTY_FLAG_MASK);
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
