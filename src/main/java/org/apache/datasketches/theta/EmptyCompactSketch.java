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

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * Singleton empty CompactSketch.
 *
 * @author Lee Rhodes
 */
final class EmptyCompactSketch extends CompactSketch {

  //For backward compatibility, a candidate long must have Flags= compact, read-only,
  //  COMPACT-Family=3, SerVer=3, PreLongs=1, and be exactly 8 bytes long. The seedHash is ignored.
  // NOTE: The empty and ordered flags may or may not be set
  private static final long EMPTY_SKETCH_MASK = 0X00_00_EB_00_00_FF_FF_FFL;
  private static final long EMPTY_SKETCH_TEST = 0X00_00_0A_00_00_03_03_01L;
  //When returning a byte array the empty and ordered bits are also set
  static final byte[] EMPTY_COMPACT_SKETCH_ARR = { 1, 3, 3, 0, 0, 0x1E, 0, 0 };
  private static final EmptyCompactSketch EMPTY_COMPACT_SKETCH = new EmptyCompactSketch();

  private EmptyCompactSketch() {}

  static EmptyCompactSketch getInstance() {
    return EMPTY_COMPACT_SKETCH;
  }

  //This should be a heapify
  static EmptyCompactSketch getHeapInstance(final Memory srcMem) {
    final long pre0 = srcMem.getLong(0);
    if (testCandidatePre0(pre0)) {
      return EMPTY_COMPACT_SKETCH;
    }
    final long maskedPre0 = pre0 & EMPTY_SKETCH_MASK;
    throw new SketchesArgumentException("Input Memory does not match required Preamble. "
        + "Memory Pre0: " + Long.toHexString(maskedPre0)
        + ", required Pre0: " + Long.toHexString(EMPTY_SKETCH_TEST));
  }

  @Override
  // This returns with ordered flag = true independent of dstOrdered.
  // This is required for fast detection.
  // The hashSeed is ignored and set == 0.
  public CompactSketch compact(final boolean dstOrdered, final WritableMemory wmem) {
    if (wmem == null) { return EmptyCompactSketch.getInstance(); }
    wmem.putByteArray(0, EMPTY_COMPACT_SKETCH_ARR, 0, 8);
    return new DirectCompactSketch(wmem);
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
  public int getRetainedEntries(final boolean valid) {
    return 0;
  }

  @Override
  public long getThetaLong() {
    return Long.MAX_VALUE;
  }

  @Override
  public boolean hasMemory() {
    return false;
  }

  @Override
  public boolean isDirect() {
    return false;
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
    return new HeapHashIterator(new long[0], 0, Long.MAX_VALUE);
  }

  /**
   * Returns 8 bytes representing a CompactSketch that the following flags set:
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
  Memory getMemory() {
    return null;
  }

  @Override
  short getSeedHash() {
    return 0;
  }

}
