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

package org.apache.datasketches.theta2;

import static org.apache.datasketches.common.ByteArrayUtil.getIntLE;
import static org.apache.datasketches.common.ByteArrayUtil.getLongLE;
import static org.apache.datasketches.common.ByteArrayUtil.getShortLE;
import static org.apache.datasketches.theta2.CompactOperations.memoryToCompact;
import static org.apache.datasketches.theta2.PreambleUtil.EMPTY_FLAG_MASK;
import static org.apache.datasketches.theta2.PreambleUtil.FLAGS_BYTE;
import static org.apache.datasketches.theta2.PreambleUtil.ORDERED_FLAG_MASK;
import static org.apache.datasketches.theta2.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static org.apache.datasketches.theta2.PreambleUtil.RETAINED_ENTRIES_INT;
import static org.apache.datasketches.theta2.PreambleUtil.SEED_HASH_SHORT;
import static org.apache.datasketches.theta2.PreambleUtil.THETA_LONG;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;

import org.apache.datasketches.thetacommon.ThetaUtil;

/**
 * Wrapper around a serialized compact read-only sketch. It is not empty, not a single item.
 *
 * <p>This sketch can only be associated with a Serialization Version 3 format binary image.</p>
 */
class WrappedCompactSketch extends CompactSketch {
  final byte[] bytes_;

  /**
   * Construct this sketch with the given bytes.
   * @param bytes containing serialized compact sketch.
   */
  WrappedCompactSketch(final byte[] bytes) {
    bytes_ = bytes;
  }

  /**
   * Wraps the given Memory, which must be a SerVer 3 CompactSketch image.
   * @param bytes representation of serialized compressed compact sketch.
   * @param seedHash The update seedHash.
   * <a href="{@docRoot}/resources/dictionary.html#seedHash">See Seed Hash</a>.
   * @return this sketch
   */
  static WrappedCompactSketch wrapInstance(final byte[] bytes, final short seedHash) {
    ThetaUtil.checkSeedHashes(getShortLE(bytes, SEED_HASH_SHORT), seedHash);
    return new WrappedCompactSketch(bytes);
  }

  //Sketch Overrides

  @Override
  public CompactSketch compact(final boolean dstOrdered, final MemorySegment dstSeg) {
    return memoryToCompact(MemorySegment.ofArray(bytes_), dstOrdered, dstSeg);
  }

  @Override
  public int getCurrentBytes() {
    final int preLongs = bytes_[PreambleUtil.PREAMBLE_LONGS_BYTE];
    final int numEntries = (preLongs == 1) ? 0 : getIntLE(bytes_, RETAINED_ENTRIES_INT);
    return (preLongs + numEntries) << 3;
  }

  @Override
  public int getRetainedEntries(final boolean valid) { //compact is always valid
    final int preLongs = bytes_[PREAMBLE_LONGS_BYTE];
    return (preLongs == 1) ? 0 : getIntLE(bytes_, RETAINED_ENTRIES_INT);
  }

  @Override
  public long getThetaLong() {
    final int preLongs = bytes_[PREAMBLE_LONGS_BYTE];
    return (preLongs > 2) ? getLongLE(bytes_, THETA_LONG) : Long.MAX_VALUE;
  }

  @Override
  public boolean hasMemorySegment() {
    return false;
  }

  @Override
  public boolean isDirect() {
    return false;
  }

  @Override
  public boolean isEmpty() {
    return (bytes_[FLAGS_BYTE] & EMPTY_FLAG_MASK) > 0;
  }

  @Override
  public boolean isOrdered() {
    return (bytes_[FLAGS_BYTE] & ORDERED_FLAG_MASK) > 0;
  }

  @Override
  public HashIterator iterator() {
    return new BytesCompactHashIterator(
      bytes_,
      bytes_[PREAMBLE_LONGS_BYTE] << 3,
      getRetainedEntries()
    );
  }

  @Override
  public byte[] toByteArray() {
    return Arrays.copyOf(bytes_, getCurrentBytes());
  }

  //restricted methods

  @Override
  long[] getCache() {
    final long[] cache = new long[getRetainedEntries()];
    int i = 0;
    final HashIterator it = iterator();
    while (it.next()) {
      cache[i++] = it.get();
    }
    return cache;
  }

  @Override
  int getCompactPreambleLongs() {
    return bytes_[PREAMBLE_LONGS_BYTE];
  }

  @Override
  int getCurrentPreambleLongs() {
    return bytes_[PREAMBLE_LONGS_BYTE];
  }

  @Override
  MemorySegment getMemorySegment() {
    return null;
  }

  @Override
  short getSeedHash() {
    return getShortLE(bytes_, SEED_HASH_SHORT);
  }
}
