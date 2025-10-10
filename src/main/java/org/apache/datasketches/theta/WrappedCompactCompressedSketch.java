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

import static org.apache.datasketches.theta.PreambleUtil.ENTRY_BITS_BYTE_V4;
import static org.apache.datasketches.theta.PreambleUtil.NUM_ENTRIES_BYTES_BYTE_V4;
import static org.apache.datasketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.wholeBytesToHoldBits;

import org.apache.datasketches.common.ByteArrayUtil;
import org.apache.datasketches.common.Util;

/**
 * A wrapper around a serialized compact compressed read-only sketch in the form of a byte array.
 * It is not an empty nor a single item sketch.
 *
 * <p>This sketch can only be associated with a Serialization Version 4 format binary image.</p>
 */
final class WrappedCompactCompressedSketch extends WrappedCompactSketch {

  /**
   * Construct this sketch with the given bytes.
   * @param bytes containing serialized compact compressed sketch.
   */
  WrappedCompactCompressedSketch(final byte[] bytes) {
    super(bytes);
  }

  /**
   * Wraps the given bytes, which must be a SerVer 4 compressed CompactThetaSketch image.
   * @param bytes representation of serialized compressed CompactThetaSketch.
   * @param seedHash The update seedHash.
   * <a href="{@docRoot}/resources/dictionary.html#seedHash">See Seed Hash</a>.
   * @return this sketch
   */
  static WrappedCompactCompressedSketch wrapInstance(final byte[] bytes, final short seedHash) {
    Util.checkSeedHashes(ByteArrayUtil.getShortLE(bytes, PreambleUtil.SEED_HASH_SHORT), seedHash);
    return new WrappedCompactCompressedSketch(bytes);
  }

  //ThetaSketch Overrides

  @Override
  public int getCurrentBytes() {
    final int preLongs = bytes_[PREAMBLE_LONGS_BYTE];
    final int entryBits = bytes_[ENTRY_BITS_BYTE_V4];
    final int numEntriesBytes = bytes_[NUM_ENTRIES_BYTES_BYTE_V4];
    return preLongs * Long.BYTES + numEntriesBytes + wholeBytesToHoldBits(getRetainedEntries() * entryBits);
  }

  private static final int START_PACKED_DATA_EXACT_MODE = 8;
  private static final int START_PACKED_DATA_ESTIMATION_MODE = 16;

  @Override
  public int getRetainedEntries(final boolean valid) { //valid is only relevant for the AlphaSketch
    // number of entries is stored using variable length encoding
    // most significant bytes with all zeros are not stored
    // one byte in the preamble has the number of non-zero bytes used
    final int preLongs = bytes_[PREAMBLE_LONGS_BYTE]; // if > 1 then the second long has theta
    final int numEntriesBytes = bytes_[NUM_ENTRIES_BYTES_BYTE_V4];
    int offsetBytes = preLongs > 1 ? START_PACKED_DATA_ESTIMATION_MODE : START_PACKED_DATA_EXACT_MODE;
    int numEntries = 0;
    for (int i = 0; i < numEntriesBytes; i++) {
      numEntries |= Byte.toUnsignedInt(bytes_[offsetBytes++]) << (i << 3);
    }
    return numEntries;
  }

  @Override
  public long getThetaLong() {
    final int preLongs = bytes_[PREAMBLE_LONGS_BYTE];
    return (preLongs > 1) ? ByteArrayUtil.getLongLE(bytes_, 8) : Long.MAX_VALUE;
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
    return new BytesCompactCompressedHashIterator(
      bytes_,
      (bytes_[PREAMBLE_LONGS_BYTE] > 1 ? START_PACKED_DATA_ESTIMATION_MODE : START_PACKED_DATA_EXACT_MODE)
        + bytes_[NUM_ENTRIES_BYTES_BYTE_V4],
      bytes_[ENTRY_BITS_BYTE_V4],
      getRetainedEntries()
    );
  }
}
