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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.apache.datasketches.theta2.PreambleUtil.extractEntryBitsV4;
import static org.apache.datasketches.theta2.PreambleUtil.extractNumEntriesBytesV4;
import static org.apache.datasketches.theta2.PreambleUtil.extractPreLongs;
import static org.apache.datasketches.theta2.PreambleUtil.extractSeedHash;
import static org.apache.datasketches.theta2.PreambleUtil.extractThetaLongV4;
import static org.apache.datasketches.theta2.PreambleUtil.wholeBytesToHoldBits;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.Util;

/**
 * An off-heap (Direct), compact, compressed, read-only sketch. It is not empty, not a single item and ordered.
 *
 * <p>This sketch can only be associated with a Serialization Version 4 format binary image.</p>
 *
 * <p>This implementation uses data in a given MemorySegment that is owned and managed by the caller.
 * This MemorySegment can be off-heap, which if managed properly will greatly reduce the need for
 * the JVM to perform garbage collection.</p>
 */
final class DirectCompactCompressedSketch extends DirectCompactSketch {
  /**
   * Construct this sketch with the given MemorySegment.
   * @param seg Read-only MemorySegment object.
   */
  DirectCompactCompressedSketch(final MemorySegment seg) {
    super(seg);
  }

  /**
   * Wraps the given MemorySegment, which must be a SerVer 4 compressed CompactSketch image.
   * Must check the validity of the MemorySegment before calling.
   * @param srcSeg The source MemorySegment
   * @param seedHash The update seedHash.
   * <a href="{@docRoot}/resources/dictionary.html#seedHash">See Seed Hash</a>.
   * @return this sketch
   */
  static DirectCompactCompressedSketch wrapInstance(final MemorySegment srcSeg, final short seedHash) {
    Util.checkSeedHashes((short) extractSeedHash(srcSeg), seedHash);
    return new DirectCompactCompressedSketch(srcSeg);
  }

  //Sketch Overrides

  @Override
  public CompactSketch compact(final boolean dstOrdered, final MemorySegment dstSeg) {
    if (dstSeg != null) {
      MemorySegment.copy(seg_, 0, dstSeg, 0, getCurrentBytes());
      return new DirectCompactSketch(dstSeg);
    }
    return CompactSketch.heapify(seg_);
  }

  @Override
  public int getCurrentBytes() {
    final int preLongs = extractPreLongs(seg_);
    final int entryBits = extractEntryBitsV4(seg_);
    final int numEntriesBytes = extractNumEntriesBytesV4(seg_);
    return preLongs * Long.BYTES + numEntriesBytes + wholeBytesToHoldBits(getRetainedEntries() * entryBits);
  }

  private static final int START_PACKED_DATA_EXACT_MODE = 8;
  private static final int START_PACKED_DATA_ESTIMATION_MODE = 16;

  @Override
  public int getRetainedEntries(final boolean valid) { //compact is always valid
    // number of entries is stored using variable length encoding
    // most significant bytes with all zeros are not stored
    // one byte in the preamble has the number of non-zero bytes used
    final int preLongs = extractPreLongs(seg_); // if > 1 then the second long has theta
    final int numEntriesBytes = extractNumEntriesBytesV4(seg_);
    int offsetBytes = preLongs > 1 ? START_PACKED_DATA_ESTIMATION_MODE : START_PACKED_DATA_EXACT_MODE;
    int numEntries = 0;
    for (int i = 0; i < numEntriesBytes; i++) {
      numEntries |= Byte.toUnsignedInt(seg_.get(JAVA_BYTE, offsetBytes++)) << (i << 3);
    }
    return numEntries;
  }

  @Override
  public long getThetaLong() {
    final int preLongs = extractPreLongs(seg_);
    return (preLongs > 1) ? extractThetaLongV4(seg_) : Long.MAX_VALUE;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public boolean isOrdered() {
    return true;
  }

  @Override
  public HashIterator iterator() {
    return new MemorySegmentCompactCompressedHashIterator(
      seg_,
      (extractPreLongs(seg_) > 1 ? START_PACKED_DATA_ESTIMATION_MODE : START_PACKED_DATA_EXACT_MODE)
        + extractNumEntriesBytesV4(seg_),
      extractEntryBitsV4(seg_),
      getRetainedEntries()
    );
  }

  //restricted methods

  @Override
  long[] getCache() {
    final int numEntries = getRetainedEntries();
    final long[] cache = new long[numEntries];
    int i = 0;
    final HashIterator it = iterator();
    while (it.next()) {
      cache[i++] = it.get();
    }
    return cache;
  }
}
