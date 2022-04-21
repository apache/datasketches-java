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

package org.apache.datasketches.kll;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.DefaultMemoryRequestServer;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

public class KllSketchTest {
  private static final DefaultMemoryRequestServer memReqSvr = new DefaultMemoryRequestServer();


  @Test
  public void checkWrapCase1Doubles() {
    KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(20);
    for (int i = 1; i <= 21; i++) { sk.update(i); }
    Memory mem = Memory.wrap(sk.toByteArray());
    KllDoublesSketch sk2 = KllDoublesSketch.wrap(mem); //KllSketch case 1
    assertTrue(mem.isReadOnly());
    assertTrue(sk2.isReadOnly());
    assertFalse(sk2.isDirect());
  }

  @Test
  public void checkWrapFloats() {
    KllFloatsSketch sk = KllFloatsSketch.newHeapInstance(20);
    for (int i = 1; i <= 21; i++) { sk.update(i); }
    Memory mem = Memory.wrap(sk.toByteArray());
    KllFloatsSketch sk2 = KllFloatsSketch.wrap(mem);
    assertTrue(mem.isReadOnly());
    assertTrue(sk2.isReadOnly());
    assertFalse(sk2.isDirect());
  }

  @Test
  public void checkWritableWrapCase6And2Doubles() {
    KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(20);
    for (int i = 1; i <= 21; i++) { sk.update(i); }
    WritableMemory wmem = WritableMemory.writableWrap(KllHelper.toUpdatableByteArrayImpl(sk));
    KllDoublesSketch sk2 = KllDoublesSketch.writableWrap(wmem, memReqSvr); //KllSketch case6
    assertFalse(wmem.isReadOnly());
    assertFalse(sk2.isReadOnly());
    assertFalse(sk2.isDirect());
  }

  @Test
  public void checkWritableWrapFloats() {
    KllFloatsSketch sk = KllFloatsSketch.newHeapInstance(20);
    for (int i = 1; i <= 20; i++) { sk.update(i); }
    sk.update(21);
    WritableMemory wmem = WritableMemory.writableWrap(KllHelper.toUpdatableByteArrayImpl(sk));
    KllFloatsSketch sk2 = KllFloatsSketch.writableWrap(wmem, memReqSvr);
    assertFalse(wmem.isReadOnly());
    assertFalse(sk2.isReadOnly());
    assertFalse(sk2.isDirect());
  }

  @Test
  public void checkKllSketchCase5Doubles() {
    KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(20);
    for (int i = 1; i <= 21; i++) { sk.update(i); }
    WritableMemory wmem = WritableMemory.writableWrap(sk.toByteArray());
    KllDoublesSketch sk2 = KllDoublesSketch.writableWrap(wmem, memReqSvr);
    assertFalse(wmem.isReadOnly());
    assertTrue(sk2.isReadOnly());
    assertFalse(sk2.isDirect());
  }

  @Test
  public void checkKllSketchCase3Doubles() {
    KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(20);
    for (int i = 1; i <= 21; i++) { sk.update(i); }
    Memory mem = Memory.wrap(KllHelper.toUpdatableByteArrayImpl(sk));
    WritableMemory wmem = (WritableMemory) mem;
    KllDoublesSketch sk2 = KllDoublesSketch.writableWrap(wmem, null);
    assertTrue(wmem.isReadOnly());
    assertTrue(sk2.isReadOnly());
    assertFalse(sk2.isDirect());
  }

  @Test
  public void checkKllSketchCase7Doubles() {
    KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(20);
    for (int i = 1; i <= 21; i++) { sk.update(i); }
    Memory mem = Memory.wrap(KllHelper.toUpdatableByteArrayImpl(sk));
    WritableMemory wmem = (WritableMemory) mem;
    KllDoublesSketch sk2 = KllDoublesSketch.writableWrap(wmem, memReqSvr);
    assertTrue(wmem.isReadOnly());
    assertTrue(sk2.isReadOnly());
    assertFalse(sk2.isDirect());
  }

  @Test
  public void checkReadOnlyExceptions() {
    int[] intArr = new int[0];
    int intV = 2;
    int idx = 1;
    KllFloatsSketch sk1 = KllFloatsSketch.newHeapInstance(20);
    Memory mem = Memory.wrap(sk1.toByteArray());
    KllFloatsSketch sk2 = KllFloatsSketch.wrap(mem);
    try { sk2.setLevelsArray(intArr);              fail(); } catch (SketchesArgumentException e) { }
    try { sk2.setLevelsArrayAt(idx,intV);          fail(); } catch (SketchesArgumentException e) { }
  }

  @SuppressWarnings("unused")
  @Test
  public void checkIsSameResource() {
    int cap = 128;
    WritableMemory wmem = WritableMemory.allocate(cap);
    WritableMemory reg1 = wmem.writableRegion(0, 64);
    WritableMemory reg2 = wmem.writableRegion(64, 64);
    assertFalse(reg1 == reg2);
    assertFalse(reg1.isSameResource(reg2));

    WritableMemory reg3 = wmem.writableRegion(0, 64);
    assertFalse(reg1 == reg3);
    assertTrue(reg1.isSameResource(reg3));

    byte[] byteArr1 = KllFloatsSketch.newHeapInstance(20).toByteArray();
    reg1.putByteArray(0, byteArr1, 0, byteArr1.length);
    KllFloatsSketch sk1 = KllFloatsSketch.wrap(reg1);

    byte[] byteArr2 = KllFloatsSketch.newHeapInstance(20).toByteArray();
    reg2.putByteArray(0, byteArr2, 0, byteArr2.length);
    if (!sk1.isSameResource(reg2)) {
      KllFloatsSketch sk2 = KllFloatsSketch.wrap(reg2);
    }

    byte[] byteArr3 = KllFloatsSketch.newHeapInstance(20).toByteArray();
    reg3.putByteArray(0, byteArr3, 0, byteArr3.length);
    if (!sk1.isSameResource(reg3)) {
      KllFloatsSketch sk3 = KllFloatsSketch.wrap(reg3);
      fail();
    }
  }

}

