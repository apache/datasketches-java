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

import static org.apache.datasketches.kll.KllPreambleUtil.EMPTY_BIT_MASK;
import static org.apache.datasketches.kll.KllPreambleUtil.PREAMBLE_INTS_EMPTY_SINGLE;
import static org.apache.datasketches.kll.KllPreambleUtil.PREAMBLE_INTS_FULL;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_EMPTY_FULL;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_SINGLE;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryFamilyID;
import static org.apache.datasketches.kll.KllPreambleUtil.*;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemoryPreInts;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemorySerVer;
import static org.apache.datasketches.kll.KllSketch.SketchType.DOUBLES_SKETCH;
import static org.apache.datasketches.kll.KllSketch.SketchType.FLOATS_SKETCH;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

@SuppressWarnings("unused")
public class KllMemoryValidateTest {

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidFamily() {
    KllFloatsSketch sk = KllFloatsSketch.newHeapInstance();
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    setMemoryFamilyID(wmem, Family.KLL.getID() - 1);
    KllMemoryValidate memVal = new KllMemoryValidate(wmem, FLOATS_SKETCH);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidSerVer() {
    KllFloatsSketch sk = KllFloatsSketch.newHeapInstance();
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    setMemorySerVer(wmem, SERIAL_VERSION_EMPTY_FULL - 1);
    KllMemoryValidate memVal = new KllMemoryValidate(wmem, FLOATS_SKETCH);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidEmptyAndSingleFormat() {
    KllFloatsSketch sk = KllFloatsSketch.newHeapInstance();
    sk.update(1);
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    setMemoryEmptyFlag(wmem, true);
    KllMemoryValidate memVal = new KllMemoryValidate(wmem, FLOATS_SKETCH);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidUpdatableAndSerVer() {
    KllFloatsSketch sk = KllFloatsSketch.newHeapInstance();
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    setMemorySerVer(wmem, SERIAL_VERSION_SINGLE);
    KllMemoryValidate memVal = new KllMemoryValidate(wmem, FLOATS_SKETCH);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidSingleAndPreInts() {
    KllFloatsSketch sk = KllFloatsSketch.newHeapInstance();
    sk.update(1);
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    setMemoryPreInts(wmem, PREAMBLE_INTS_FULL);
    KllMemoryValidate memVal = new KllMemoryValidate(wmem, FLOATS_SKETCH);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidSingleAndSerVer() {
    KllFloatsSketch sk = KllFloatsSketch.newHeapInstance();
    sk.update(1);
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    setMemorySerVer(wmem, SERIAL_VERSION_EMPTY_FULL);
    KllMemoryValidate memVal = new KllMemoryValidate(wmem, FLOATS_SKETCH);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidEmptyDoublesAndPreIntsFull() {
    KllDoublesSketch sk = KllDoublesSketch.newHeapInstance();
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    setMemoryPreInts(wmem, PREAMBLE_INTS_FULL);
    KllMemoryValidate memVal = new KllMemoryValidate(wmem, DOUBLES_SKETCH);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidSingleDoubleCompactAndSerVer() {
    KllDoublesSketch sk = KllDoublesSketch.newHeapInstance();
    sk.update(1);
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    setMemorySerVer(wmem, SERIAL_VERSION_EMPTY_FULL);
    KllMemoryValidate memVal = new KllMemoryValidate(wmem, DOUBLES_SKETCH);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidDoubleUpdatableAndPreInts() {
    KllDoublesSketch sk = KllDoublesSketch.newHeapInstance();
    byte[] byteArr = KllHelper.toUpdatableByteArrayImpl(sk);
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    setMemoryPreInts(wmem, PREAMBLE_INTS_EMPTY_SINGLE);
    KllMemoryValidate memVal = new KllMemoryValidate(wmem, DOUBLES_SKETCH);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidFloatFullAndPreInts() {
    KllFloatsSketch sk = KllFloatsSketch.newHeapInstance();
    sk.update(1); sk.update(2);
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    setMemoryPreInts(wmem, PREAMBLE_INTS_EMPTY_SINGLE);
    KllMemoryValidate memVal = new KllMemoryValidate(wmem, FLOATS_SKETCH);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidFloatUpdatableFullAndPreInts() {
    KllFloatsSketch sk = KllFloatsSketch.newHeapInstance();
    sk.update(1); sk.update(2);
    byte[] byteArr = KllHelper.toUpdatableByteArrayImpl(sk);
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    setMemoryPreInts(wmem, PREAMBLE_INTS_EMPTY_SINGLE);
    KllMemoryValidate memVal = new KllMemoryValidate(wmem, FLOATS_SKETCH);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidDoubleCompactSingleAndPreInts() {
    KllDoublesSketch sk = KllDoublesSketch.newHeapInstance();
    sk.update(1);
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    setMemoryPreInts(wmem, PREAMBLE_INTS_FULL);//should be 2, single
    KllMemoryValidate memVal = new KllMemoryValidate(wmem, DOUBLES_SKETCH);
  }

}

