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

import static org.apache.datasketches.theta.PreambleUtil.extractEntryBitsV4;
import static org.apache.datasketches.theta.PreambleUtil.extractNumEntriesBytesV4;
import static org.apache.datasketches.theta.PreambleUtil.extractPreLongs;
import static org.apache.datasketches.theta.PreambleUtil.extractSeedHash;
import static org.apache.datasketches.theta.PreambleUtil.extractThetaLongV4;
import static org.apache.datasketches.theta.PreambleUtil.wholeBytesToHoldBits;

import org.apache.datasketches.common.Util;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * An off-heap (Direct), compact, compressed, read-only sketch. It is not empty, not a single item and ordered.
 *
 * <p>This sketch can only be associated with a Serialization Version 4 format binary image.</p>
 *
 * <p>This implementation uses data in a given Memory that is owned and managed by the caller.
 * This Memory can be off-heap, which if managed properly will greatly reduce the need for
 * the JVM to perform garbage collection.</p>
 */
class DirectCompactCompressedSketch extends DirectCompactSketch {
  /**
   * Construct this sketch with the given memory.
   * @param mem Read-only Memory object.
   */
  DirectCompactCompressedSketch(final Memory mem) {
    super(mem);
  }

  /**
   * Wraps the given Memory, which must be a SerVer 4 compressed CompactSketch image.
   * Must check the validity of the Memory before calling.
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seedHash The update seedHash.
   * <a href="{@docRoot}/resources/dictionary.html#seedHash">See Seed Hash</a>.
   * @return this sketch
   */
  static DirectCompactCompressedSketch wrapInstance(final Memory srcMem, final short seedHash) {
    Util.checkSeedHashes((short) extractSeedHash(srcMem), seedHash);
    return new DirectCompactCompressedSketch(srcMem);
  }

  //Sketch Overrides

  @Override
  public CompactSketch compact(final boolean dstOrdered, final WritableMemory dstMem) {
    if (dstMem != null) {
      mem_.copyTo(0, dstMem, 0, getCurrentBytes());
      return new DirectCompactSketch(dstMem);
    }
    return CompactSketch.heapify(mem_);
  }

  @Override
  public int getCurrentBytes() {
    final int preLongs = extractPreLongs(mem_);
    final int entryBits = extractEntryBitsV4(mem_);
    final int numEntriesBytes = extractNumEntriesBytesV4(mem_);
    return preLongs * Long.BYTES + numEntriesBytes + wholeBytesToHoldBits(getRetainedEntries() * entryBits);
  }

  private static final int START_PACKED_DATA_EXACT_MODE = 8;
  private static final int START_PACKED_DATA_ESTIMATION_MODE = 16;

  @Override
  public int getRetainedEntries(final boolean valid) { //compact is always valid
    // number of entries is stored using variable length encoding
    // most significant bytes with all zeros are not stored
    // one byte in the preamble has the number of non-zero bytes used
    final int preLongs = extractPreLongs(mem_); // if > 1 then the second long has theta
    final int numEntriesBytes = extractNumEntriesBytesV4(mem_);
    int offsetBytes = preLongs > 1 ? START_PACKED_DATA_ESTIMATION_MODE : START_PACKED_DATA_EXACT_MODE;
    int numEntries = 0;
    for (int i = 0; i < numEntriesBytes; i++) {
      numEntries |= Byte.toUnsignedInt(mem_.getByte(offsetBytes++)) << (i << 3);
    }
    return numEntries;
  }

  @Override
  public long getThetaLong() {
    final int preLongs = extractPreLongs(mem_);
    return (preLongs > 1) ? extractThetaLongV4(mem_) : Long.MAX_VALUE;
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
    return new MemoryCompactCompressedHashIterator(
      mem_,
      (extractPreLongs(mem_) > 1 ? START_PACKED_DATA_ESTIMATION_MODE : START_PACKED_DATA_EXACT_MODE)
        + extractNumEntriesBytesV4(mem_),
      extractEntryBitsV4(mem_),
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
