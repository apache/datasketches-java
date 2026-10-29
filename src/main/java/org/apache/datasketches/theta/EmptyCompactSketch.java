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

package org.apache.datasketches.theta;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_SHORT_UNALIGNED;
import static org.apache.datasketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static org.apache.datasketches.theta.PreambleUtil.SEED_HASH_SHORT;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;

/**
 * Singleton Empty CompactThetaSketch.
 *
 * @author Lee Rhodes
 */
final class EmptyCompactSketch extends CompactThetaSketch {

  // The MASK selects which bits of the first 8 bytes are examined; it must accept historical images.
  // The TEST gives the required values of the examined bits; it must lie within the MASK.
  // The ARR is what we write going forward; (ARR & MASK) must equal TEST.
  //
  // A candidate EmptyCompactSketch must have
  //  bytes: PreLongs=1, SerVer=3, COMPACT-Family=3, bytes 3,4 = 0 (not used by compact);
  //  flags: read-only, empty, compact set; single-item and reserved bits 0, 6, 7 clear.
  // The ordered flag is ignored: Java before 1.0.0 and C++ before 3.3.0 wrote empty images without it.
  // The reserved flag bits must be zero for SerVer 3. Any future use of them requires a new SerVer.
  // The seedHash bytes are ignored: they may be 0 or the seed hash, depending on the writer.
  //
  // Legacy form: versions before 1.0.0 wrote empty sketches with p < 1 in the full 24-byte form,
  //  with PreLongs=3 and theta = p. These must pass EMPTY_SKETCH_TEST_LEGACY, have zero retained
  //  entries and carry the expected seed hash. Empty implies theta = 1.0, so theta is ignored.
  static final long EMPTY_SKETCH_MASK = 0X00_00_EF_FF_FF_FF_FF_FFL;
  static final long EMPTY_SKETCH_TEST = 0X00_00_0E_00_00_03_03_01L;
  static final long EMPTY_SKETCH_TEST_LEGACY = 0X00_00_0E_00_00_03_03_03L;
  //When returning a byte array the following flags are set: ordered, compact, empty, read-only
  static final byte[] EMPTY_COMPACT_SKETCH_ARR = { 1, 3, 3, 0, 0, 0x1E, 0, 0 };
  private static final EmptyCompactSketch EMPTY_COMPACT_SKETCH = new EmptyCompactSketch();

  private EmptyCompactSketch() {}

  static EmptyCompactSketch getInstance() {
    return EMPTY_COMPACT_SKETCH;
  }

  //This is effectively a heapify
  static EmptyCompactSketch getHeapInstance(final MemorySegment srcSeg, final short expectedSeedHash) {
    final long pre0 = srcSeg.get(JAVA_LONG_UNALIGNED, 0);
    if (testCandidatePre0(pre0)) {
      return EMPTY_COMPACT_SKETCH;
    }
    if ((pre0 & EMPTY_SKETCH_MASK) == EMPTY_SKETCH_TEST_LEGACY) {
      if ((srcSeg.byteSize() < 24) || (srcSeg.get(JAVA_INT_UNALIGNED, RETAINED_ENTRIES_INT) != 0)) {
        throw new SketchesArgumentException(
            "Input MemorySegment is a legacy empty image, but is too short or has retained entries.");
      }
      Util.checkSeedHashes(expectedSeedHash, srcSeg.get(JAVA_SHORT_UNALIGNED, SEED_HASH_SHORT));
      return EMPTY_COMPACT_SKETCH;
    }
    final long maskedPre0 = pre0 & EMPTY_SKETCH_MASK;
    throw new SketchesArgumentException("Input MemorySegment does not match required Preamble. "
        + "MemorySegment Pre0: " + Long.toHexString(maskedPre0)
        + ", required Pre0: " + Long.toHexString(EMPTY_SKETCH_TEST));
  }

  @Override
  // This returns with ordered flag = true independent of dstOrdered.
  // This is required for fast detection.
  // The hashSeed is ignored and set == 0.
  public CompactThetaSketch compact(final boolean dstOrdered, final MemorySegment dstWSeg) {
    if (dstWSeg == null) { return EmptyCompactSketch.getInstance(); }
    //dstWSeg.putByteArray(0, EMPTY_COMPACT_SKETCH_ARR, 0, 8);
    MemorySegment.copy(EMPTY_COMPACT_SKETCH_ARR, 0, dstWSeg, JAVA_BYTE, 0, 8);
    return new DirectCompactSketch(dstWSeg);
  }

  //static

  static boolean testCandidatePre0(final long candidate) {
    return (candidate & EMPTY_SKETCH_MASK) == EMPTY_SKETCH_TEST;
  }

  @Override
  public int getCurrentBytes() {
    return 8;
  }

  @Override
  public double getEstimate() { return 0; }

  @Override
  public int getRetainedEntries(final boolean valid) { //valid is only relevant for the AlphaSketch
    return 0;
  }

  @Override
  public long getThetaLong() {
    return Long.MAX_VALUE;
  }

  @Override
  public boolean isEmpty() {
    return true;
  }

  @Override
  public boolean isOrdered() {
    return true;
  }

  @Override
  public HashIterator iterator() {
    return new HeapCompactHashIterator(new long[0]);
  }

  /**
   * Returns 8 bytes representing a CompactThetaSketch that the following flags set:
   * ordered, compact, empty, readOnly. The SerVer is 3, the Family is COMPACT(3),
   * and the PreLongs = 1. The seedHash is zero.
   */
  @Override
  public byte[] toByteArray() {
    return EMPTY_COMPACT_SKETCH_ARR;
  }

  @Override
  long[] getCache() {
    return new long[0];
  }

  @Override
  int getCompactPreambleLongs() {
    return 1;
  }

  @Override
  int getCurrentPreambleLongs() {
    return 1;
  }

  @Override
  public short getSeedHash() {
    return 0;
  }

}
