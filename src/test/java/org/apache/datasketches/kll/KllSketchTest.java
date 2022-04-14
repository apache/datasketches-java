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
    WritableMemory wmem = WritableMemory.writableWrap(sk.toUpdatableByteArray());
    KllDoublesSketch sk2 = KllDoublesSketch.writableWrap(wmem, memReqSvr); //KllSketch case6
    assertFalse(wmem.isReadOnly());
    assertFalse(sk2.isReadOnly());
    assertFalse(sk2.isDirect());
  }

  @Test
  public void checkWritableWrapFloats() {
    KllFloatsSketch sk = KllFloatsSketch.newHeapInstance(20);
    for (int i = 1; i <= 21; i++) { sk.update(i); }
    WritableMemory wmem = WritableMemory.writableWrap(sk.toUpdatableByteArray());
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
    KllDoublesSketch sk2 = KllDoublesSketch.writableWrap(wmem, memReqSvr); //KllSketch case5
    assertFalse(wmem.isReadOnly());
    assertTrue(sk2.isReadOnly());
    assertFalse(sk2.isDirect());
  }

  @Test
  public void checkKllSketchCase3Doubles() {
    KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(20);
    for (int i = 1; i <= 21; i++) { sk.update(i); }
    Memory mem = Memory.wrap(sk.toUpdatableByteArray());
    WritableMemory wmem = (WritableMemory) mem;
    KllDoublesSketch sk2 = KllDoublesSketch.writableWrap(wmem, null); //KllSketch case3
    assertTrue(wmem.isReadOnly());
    assertTrue(sk2.isReadOnly());
    assertFalse(sk2.isDirect());
  }

  @Test
  public void checkKllSketchCase7Doubles() {
    KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(20);
    for (int i = 1; i <= 21; i++) { sk.update(i); }
    Memory mem = Memory.wrap(sk.toUpdatableByteArray());
    WritableMemory wmem = (WritableMemory) mem;
    KllDoublesSketch sk2 = KllDoublesSketch.writableWrap(wmem, memReqSvr); //KllSketch case7
    assertTrue(wmem.isReadOnly());
    assertTrue(sk2.isReadOnly());
    assertFalse(sk2.isDirect());
  }

}

