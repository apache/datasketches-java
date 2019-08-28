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

import org.testng.annotations.Test;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.SketchesArgumentException;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class PreambleUtilTest {

  @Test
  public void preambleToString() { //TODO Check Visually
    int bytes = HllSketch.getMaxUpdatableSerializationBytes(8, TgtHllType.HLL_4);
    byte[] byteArr1 = new byte[bytes];
    WritableMemory wmem1 = WritableMemory.wrap(byteArr1);
    HllSketch sk = new HllSketch(8, TgtHllType.HLL_4, wmem1);
    byte[] byteArr2 = sk.toCompactByteArray();
    WritableMemory wmem2 = WritableMemory.wrap(byteArr2);

    assertEquals(sk.getCurMode(), CurMode.LIST);
    assertTrue(sk.isEmpty());
    String s = HllSketch.toString(byteArr2); //empty sketch output
    println(s);
    println("LgArr: " + PreambleUtil.extractLgArr(wmem2));
    println("Empty: " + PreambleUtil.extractEmptyFlag(wmem2));
    println("Serialization Bytes: " + wmem2.getCapacity());

    for (int i = 0; i < 7; i++) { sk.update(i); }
    byteArr2 = sk.toCompactByteArray();
    wmem2 = WritableMemory.wrap(byteArr2);
    assertEquals(sk.getCurMode(), CurMode.LIST);
    assertFalse(sk.isEmpty());
    s = HllSketch.toString(byteArr2);
    println(s);
    println("LgArr: " + PreambleUtil.extractLgArr(wmem2));
    println("Empty: " + PreambleUtil.extractEmptyFlag(wmem2));
    println("Serialization Bytes: " + wmem2.getCapacity());

    for (int i = 7; i < 24; i++) { sk.update(i); }
    byteArr2 = sk.toCompactByteArray();
    wmem2 = WritableMemory.wrap(byteArr2);
    assertEquals(sk.getCurMode(), CurMode.SET);
    s = HllSketch.toString(byteArr2);
    println(s);
    println("LgArr: " + PreambleUtil.extractLgArr(wmem2));
    println("Empty: " + PreambleUtil.extractEmptyFlag(wmem2));
    println("Serialization Bytes: " + wmem2.getCapacity());

    sk.update(24);
    byteArr2 = sk.toCompactByteArray();
    wmem2 = WritableMemory.wrap(byteArr2);
    assertEquals(sk.getCurMode(), CurMode.HLL);
    s = HllSketch.toString(Memory.wrap(byteArr2));
    println(s);
    println("LgArr: " + PreambleUtil.extractLgArr(wmem2));
    println("Empty: " + PreambleUtil.extractEmptyFlag(wmem2));
    println("Serialization Bytes: " + wmem2.getCapacity());
  }

  @Test
  public void checkCompactFlag() {
    HllSketch sk = new HllSketch(7);
    byte[] memObj = sk.toCompactByteArray();
    WritableMemory wmem = WritableMemory.wrap(memObj);
    boolean compact = PreambleUtil.extractCompactFlag(wmem);
    assertTrue(compact);

    PreambleUtil.insertCompactFlag(wmem, false);
    compact = PreambleUtil.extractCompactFlag(wmem);
    assertFalse(compact);
  }

  @SuppressWarnings("unused")
  @Test
  public void checkCorruptMemoryInput() {
    HllSketch sk = new HllSketch(12);
    byte[] memObj = sk.toCompactByteArray();
    WritableMemory wmem = WritableMemory.wrap(memObj);
    long memAdd = wmem.getCumulativeOffset(0);
    HllSketch bad;

    //checkFamily
    try {
      wmem.putByte(FAMILY_BYTE, (byte) 0); //corrupt, should be 7
      bad = HllSketch.heapify(wmem);
      fail();
    } catch (SketchesArgumentException e) { /* OK */ }
    insertFamilyId(wmem); //corrected

    //check SerVer
    try {
      wmem.putByte(SER_VER_BYTE, (byte) 0); //corrupt, should be 1
      bad = HllSketch.heapify(wmem);
      fail();
    } catch (SketchesArgumentException e) { /* OK */ }
    insertSerVer(wmem); //corrected

    //check bad PreInts
    try {
      insertPreInts(wmem, 0); //corrupt, should be 2
      bad = HllSketch.heapify(wmem);
      fail();
    } catch (SketchesArgumentException e) { /* OK */ }
    insertPreInts(wmem, 2); //corrected

    //check wrong PreInts and LIST
    try {
      insertPreInts(wmem, 3); //corrupt, should be 2
      bad = HllSketch.heapify(wmem);
      fail();
    } catch (SketchesArgumentException e) { /* OK */ }
    insertPreInts(wmem, 2); //corrected

    //move to Set mode
    for (int i = 1; i <= 15; i++) { sk.update(i); }
    memObj = sk.toCompactByteArray();
    wmem = WritableMemory.wrap(memObj);
    memAdd = wmem.getCumulativeOffset(0);

    //check wrong PreInts and SET
    try {
      insertPreInts(wmem, 2); //corrupt, should be 3
      bad = HllSketch.heapify(wmem);
      fail();
    } catch (SketchesArgumentException e) { /* OK */ }
    insertPreInts(wmem, 3); //corrected

    //move to HLL mode
    for (int i = 15; i <= 1000; i++) { sk.update(i); }
    memObj = sk.toCompactByteArray();
    wmem = WritableMemory.wrap(memObj);
    memAdd = wmem.getCumulativeOffset(0);

    //check wrong PreInts and HLL
    try {
      insertPreInts(wmem, 2); //corrupt, should be 10
      bad = HllSketch.heapify(wmem);
      fail();
    } catch (SketchesArgumentException e) { /* OK */ }
    insertPreInts(wmem, 10); //corrected
  }

  @SuppressWarnings("unused")
  @Test
  public void checkExtractFlags() {
    int bytes = HllSketch.getMaxUpdatableSerializationBytes(4, TgtHllType.HLL_4);
    WritableMemory wmem = WritableMemory.allocate(bytes);
    Object memObj = wmem.getArray();
    long memAdd = wmem.getCumulativeOffset(0L);
    HllSketch sk = new HllSketch(4, TgtHllType.HLL_4, wmem);
    int flags = extractFlags(wmem);
    assertEquals(flags, EMPTY_FLAG_MASK);
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
