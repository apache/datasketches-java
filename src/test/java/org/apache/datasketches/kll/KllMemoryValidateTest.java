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

import static org.apache.datasketches.kll.KllPreambleUtil.PREAMBLE_INTS_EMPTY_SINGLE;
import static org.apache.datasketches.kll.KllPreambleUtil.PREAMBLE_INTS_FULL;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_EMPTY_FULL;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_SINGLE;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemorySegmentEmptyFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemorySegmentFamilyID;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemorySegmentPreInts;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemorySegmentSerVer;
import static org.apache.datasketches.kll.KllSketch.SketchType.DOUBLES_SKETCH;
import static org.apache.datasketches.kll.KllSketch.SketchType.FLOATS_SKETCH;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.kll.KllHelper;
import org.apache.datasketches.kll.KllMemorySegmentValidate;
import org.testng.annotations.Test;

@SuppressWarnings("unused")
public class KllMemoryValidateTest {

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidFamily() {
    final KllFloatsSketch sk = KllFloatsSketch.newHeapInstance();
    final byte[] byteArr = sk.toByteArray();
    final MemorySegment wseg = MemorySegment.ofArray(byteArr);
    setMemorySegmentFamilyID(wseg, Family.KLL.getID() - 1);
    final KllMemorySegmentValidate segVal = new KllMemorySegmentValidate(wseg, FLOATS_SKETCH);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidSerVer() {
    final KllFloatsSketch sk = KllFloatsSketch.newHeapInstance();
    final byte[] byteArr = sk.toByteArray();
    final MemorySegment wseg = MemorySegment.ofArray(byteArr);
    setMemorySegmentSerVer(wseg, SERIAL_VERSION_EMPTY_FULL - 1);
    final KllMemorySegmentValidate segVal = new KllMemorySegmentValidate(wseg, FLOATS_SKETCH);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidEmptyAndSingleFormat() {
    final KllFloatsSketch sk = KllFloatsSketch.newHeapInstance();
    sk.update(1);
    final byte[] byteArr = sk.toByteArray();
    final MemorySegment wseg = MemorySegment.ofArray(byteArr);
    setMemorySegmentEmptyFlag(wseg, true);
    final KllMemorySegmentValidate segVal = new KllMemorySegmentValidate(wseg, FLOATS_SKETCH);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidUpdatableAndSerVer() {
    final KllFloatsSketch sk = KllFloatsSketch.newHeapInstance();
    final byte[] byteArr = sk.toByteArray();
    final MemorySegment wseg = MemorySegment.ofArray(byteArr);
    setMemorySegmentSerVer(wseg, SERIAL_VERSION_SINGLE);
    final KllMemorySegmentValidate segVal = new KllMemorySegmentValidate(wseg, FLOATS_SKETCH);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidSingleAndPreInts() {
    final KllFloatsSketch sk = KllFloatsSketch.newHeapInstance();
    sk.update(1);
    final byte[] byteArr = sk.toByteArray();
    final MemorySegment wseg = MemorySegment.ofArray(byteArr);
    setMemorySegmentPreInts(wseg, PREAMBLE_INTS_FULL);
    final KllMemorySegmentValidate segVal = new KllMemorySegmentValidate(wseg, FLOATS_SKETCH);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidSingleAndSerVer() {
    final KllFloatsSketch sk = KllFloatsSketch.newHeapInstance();
    sk.update(1);
    final byte[] byteArr = sk.toByteArray();
    final MemorySegment wseg = MemorySegment.ofArray(byteArr);
    setMemorySegmentSerVer(wseg, SERIAL_VERSION_EMPTY_FULL);
    final KllMemorySegmentValidate segVal = new KllMemorySegmentValidate(wseg, FLOATS_SKETCH);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidEmptyDoublesAndPreIntsFull() {
    final KllDoublesSketch sk = KllDoublesSketch.newHeapInstance();
    final byte[] byteArr = sk.toByteArray();
    final MemorySegment wseg = MemorySegment.ofArray(byteArr);
    setMemorySegmentPreInts(wseg, PREAMBLE_INTS_FULL);
    final KllMemorySegmentValidate segVal = new KllMemorySegmentValidate(wseg, DOUBLES_SKETCH);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidSingleDoubleCompactAndSerVer() {
    final KllDoublesSketch sk = KllDoublesSketch.newHeapInstance();
    sk.update(1);
    final byte[] byteArr = sk.toByteArray();
    final MemorySegment wseg = MemorySegment.ofArray(byteArr);
    setMemorySegmentSerVer(wseg, SERIAL_VERSION_EMPTY_FULL);
    final KllMemorySegmentValidate segVal = new KllMemorySegmentValidate(wseg, DOUBLES_SKETCH);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidDoubleUpdatableAndPreInts() {
    final KllDoublesSketch sk = KllDoublesSketch.newHeapInstance();
    final byte[] byteArr = KllHelper.toByteArray(sk, true);
    final MemorySegment wseg = MemorySegment.ofArray(byteArr);
    setMemorySegmentPreInts(wseg, PREAMBLE_INTS_EMPTY_SINGLE);
    final KllMemorySegmentValidate segVal = new KllMemorySegmentValidate(wseg, DOUBLES_SKETCH);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidFloatFullAndPreInts() {
    final KllFloatsSketch sk = KllFloatsSketch.newHeapInstance();
    sk.update(1); sk.update(2);
    final byte[] byteArr = sk.toByteArray();
    final MemorySegment wseg = MemorySegment.ofArray(byteArr);
    setMemorySegmentPreInts(wseg, PREAMBLE_INTS_EMPTY_SINGLE);
    final KllMemorySegmentValidate segVal = new KllMemorySegmentValidate(wseg, FLOATS_SKETCH);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidFloatUpdatableFullAndPreInts() {
    final KllFloatsSketch sk = KllFloatsSketch.newHeapInstance();
    sk.update(1); sk.update(2);
    final byte[] byteArr = KllHelper.toByteArray(sk, true);
    final MemorySegment wseg = MemorySegment.ofArray(byteArr);
    setMemorySegmentPreInts(wseg, PREAMBLE_INTS_EMPTY_SINGLE);
    final KllMemorySegmentValidate segVal = new KllMemorySegmentValidate(wseg, FLOATS_SKETCH);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidDoubleCompactSingleAndPreInts() {
    final KllDoublesSketch sk = KllDoublesSketch.newHeapInstance();
    sk.update(1);
    final byte[] byteArr = sk.toByteArray();
    final MemorySegment wseg = MemorySegment.ofArray(byteArr);
    setMemorySegmentPreInts(wseg, PREAMBLE_INTS_FULL);//should be 2, single
    final KllMemorySegmentValidate segVal = new KllMemorySegmentValidate(wseg, DOUBLES_SKETCH);
  }

}

