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

package org.apache.datasketches.tuple;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_SHORT_UNALIGNED;
import static org.apache.datasketches.common.Util.checkSeedHashes;
import static org.apache.datasketches.common.Util.computeSeedHash;
import static org.apache.datasketches.tuple.PreambleUtil.COMPACT_FLAGS_BYTE;
import static org.apache.datasketches.tuple.PreambleUtil.COMPACT_SER_VER;
import static org.apache.datasketches.tuple.PreambleUtil.COMPACT_SER_VER_LEGACY;
import static org.apache.datasketches.tuple.PreambleUtil.COMPACT_SER_VER_WITH_CLASS_NAME;
import static org.apache.datasketches.tuple.PreambleUtil.FAMILY_BYTE;
import static org.apache.datasketches.tuple.PreambleUtil.LEGACY_FLAGS_BYTE;
import static org.apache.datasketches.tuple.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static org.apache.datasketches.tuple.PreambleUtil.RETAINED_ENTRIES_INT;
import static org.apache.datasketches.tuple.PreambleUtil.SEED_HASH_SHORT;
import static org.apache.datasketches.tuple.PreambleUtil.SER_VER_BYTE;
import static org.apache.datasketches.tuple.PreambleUtil.SKETCH_TYPE_BYTE;
import static org.apache.datasketches.tuple.PreambleUtil.THETA_LONG;
import static org.apache.datasketches.thetacommon.HashOperations.count;

import java.lang.foreign.MemorySegment;
import java.lang.reflect.Array;

import org.apache.datasketches.common.ByteArrayUtil;
import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;

/**
 * CompactTupleSketches are never created directly. They are created as a result of
 * the compact() method of an UpdatableTupleSketch or as a result of the getResult()
 * method of a set operation like TupleUnion, TupleIntersection or TupleAnotB.
 * CompactTupleSketch consists of a compact list (i.e. no intervening spaces) of hash values,
 * corresponding list of Summaries, and a value for theta. The lists may or may
 * not be ordered. CompactTupleSketch is read-only.
 *
 * @param <S> type of Summary
 */
public final class CompactTupleSketch<S extends Summary> extends TupleSketch<S> {
  private final long[] hashArr_;
  private S[] summaryArr_;

  private enum FlagsLegacy { IS_BIG_ENDIAN, IS_EMPTY, HAS_ENTRIES, IS_THETA_INCLUDED }

  private enum Flags { IS_RESERVED, IS_READ_ONLY, IS_EMPTY, IS_COMPACT, IS_ORDERED }

  /**
   * Create a CompactTupleSketch from correct components
   * @param hashArr compacted hash array
   * @param summaryArr compacted summary array
   * @param thetaLong long value of theta
   * @param empty empty flag
   * @param seedHash the seed hash
   */
  CompactTupleSketch(final long[] hashArr, final S[] summaryArr, final long thetaLong, final boolean empty,
      final short seedHash) {
    super(thetaLong, empty, null, seedHash);
    super.thetaLong_ = thetaLong;
    super.empty_ = empty;
    hashArr_ = hashArr;
    summaryArr_ = summaryArr;
  }

  /**
   * This is to create an instance of a CompactTupleSketch given a serialized form
   *
   * @param seg MemorySegment object with serialized CompactTupleSketch
   * @param deserializer the SummaryDeserializer
   * @param seed the update seed that the serialized sketch must have been created with
   */
  CompactTupleSketch(final MemorySegment seg, final SummaryDeserializer<S> deserializer, final long seed) {
    super(Long.MAX_VALUE, true, null, computeSeedHash(seed));
    final byte preambleLongs = seg.get(JAVA_BYTE, PREAMBLE_LONGS_BYTE);
    final byte version = seg.get(JAVA_BYTE, SER_VER_BYTE);
    final byte familyId = seg.get(JAVA_BYTE, FAMILY_BYTE);
    SerializerDeserializer.validateFamily(familyId, preambleLongs);
    if (version > COMPACT_SER_VER) {
      throw new SketchesArgumentException(
          "Unsupported serial version. Expected: " + COMPACT_SER_VER + " or lower, actual: " + version);
    }
    SerializerDeserializer
      .validateType(seg.get(JAVA_BYTE, SKETCH_TYPE_BYTE), SerializerDeserializer.SketchType.CompactTupleSketch);
    if (version <= COMPACT_SER_VER_LEGACY) { // legacy serial format, always written with the default seed
      final byte flags = seg.get(JAVA_BYTE, LEGACY_FLAGS_BYTE);
      int offset = LEGACY_FLAGS_BYTE + 1; // fields after the flags are packed
      empty_ = (flags & (1 << FlagsLegacy.IS_EMPTY.ordinal())) > 0;
      if (!empty_) { checkSeedHashes(seedHash_, Util.DEFAULT_SEED_HASH); }
      final boolean isThetaIncluded = (flags & (1 << FlagsLegacy.IS_THETA_INCLUDED.ordinal())) > 0;
      if (isThetaIncluded) {
        thetaLong_ = seg.get(JAVA_LONG_UNALIGNED, offset);
        offset += Long.BYTES;
      } else {
        thetaLong_ = Long.MAX_VALUE;
      }
      final boolean hasEntries = (flags & (1 << FlagsLegacy.HAS_ENTRIES.ordinal())) > 0;
      if (hasEntries) {
        int classNameLength = 0;
        if (version == COMPACT_SER_VER_WITH_CLASS_NAME) {
          classNameLength = seg.get(JAVA_BYTE, offset++);
        }
        final int count = seg.get(JAVA_INT_UNALIGNED, offset);
        offset += Integer.BYTES;
        if (version == COMPACT_SER_VER_WITH_CLASS_NAME) {
          offset += classNameLength;
        }
        hashArr_ = new long[count];

        for (int i = 0; i < count; i++) {
          hashArr_[i] = seg.get(JAVA_LONG_UNALIGNED, offset);
          offset += Long.BYTES;
        }
        for (int i = 0; i < count; i++) {
          offset += readSummary(seg, offset, i, count, deserializer);
        }
      } else {
        hashArr_ = new long[0];
        summaryArr_ = null;
      }
    } else { // current serial format
      final byte flags = seg.get(JAVA_BYTE, COMPACT_FLAGS_BYTE);
      final short seedHash = seg.get(JAVA_SHORT_UNALIGNED, SEED_HASH_SHORT);
      empty_ = (flags & (1 << Flags.IS_EMPTY.ordinal())) > 0;
      if (!empty_) { checkSeedHashes(seedHash_, seedHash); } // an empty image carries zero, as in theta
      thetaLong_ = Long.MAX_VALUE;
      int count = 0;
      if (!empty_) {
        if (preambleLongs == 1) {
          count = 1;
        } else {
          count = seg.get(JAVA_INT_UNALIGNED, RETAINED_ENTRIES_INT);
          if (preambleLongs > 2) {
            thetaLong_ = seg.get(JAVA_LONG_UNALIGNED, THETA_LONG);
          }
        }
      }
      int offset = preambleLongs * Long.BYTES; // the entries start after the preamble
      hashArr_ = new long[count];

      for (int i = 0; i < count; i++) {
        hashArr_[i] = seg.get(JAVA_LONG_UNALIGNED, offset);
        offset += Long.BYTES;
        offset += readSummary(seg, offset, i, count, deserializer);
      }
    }
  }

  @SuppressWarnings({"unchecked"})
  private int readSummary(final MemorySegment seg, final int offset, final int i, final int count,
      final SummaryDeserializer<S> deserializer) {
    final MemorySegment segRegion = seg.asSlice(offset, seg.byteSize() - offset);
    final DeserializeResult<S> result = deserializer.heapifySummary(segRegion);
    final S summary = result.getObject();
    final Class<S> summaryType = (Class<S>) result.getObject().getClass();
    if (summaryArr_ == null) {
      summaryArr_ = (S[]) Array.newInstance(summaryType, count);
    }
    summaryArr_[i] = summary;
    return result.getSize();
  }

  @Override
  public CompactTupleSketch<S> compact() {
    return this;
  }

  long[] getHashArr() {
    return hashArr_;
  }

  S[] getSummaryArr() {
    return summaryArr_;
  }

  @Override
  public int getRetainedEntries() {
    return hashArr_ == null ? 0 : hashArr_.length;
  }

  @Override
  public int getCountLessThanThetaLong(final long thetaLong) {
    return count(hashArr_, thetaLong);
  }

  // Layout of first 8 bytes:
  // Long || Start Byte Adr:
  // Adr:
  //      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0              |
  //  0   ||    seed hash    |  Flags | unused | SkType | FamID  | SerVer |  Preamble_Longs    |
  @Override
  public byte[] toByteArray() {
  final int count = getRetainedEntries();
    final boolean isSingleItem = (count == 1) && !isEstimationMode();
    final int preambleLongs = isEmpty() || isSingleItem ? 1 : isEstimationMode() ? 3 : 2;

    int summariesSizeBytes = 0;
    final byte[][] summariesBytes = new byte[count][];
    if (count > 0) {
      for (int i = 0; i < count; i++) {
        summariesBytes[i] = summaryArr_[i].toByteArray();
        summariesSizeBytes += summariesBytes[i].length;
      }
    }

    final int sizeBytes = (Long.BYTES * preambleLongs) + (Long.BYTES * count) + summariesSizeBytes;
    final byte[] bytes = new byte[sizeBytes];
    bytes[PREAMBLE_LONGS_BYTE] = (byte) preambleLongs;
    bytes[SER_VER_BYTE] = COMPACT_SER_VER;
    bytes[FAMILY_BYTE] = (byte) Family.TUPLE.getID();
    bytes[SKETCH_TYPE_BYTE] = (byte) SerializerDeserializer.SketchType.CompactTupleSketch.ordinal();
    bytes[COMPACT_FLAGS_BYTE] = (byte) (
        (1 << Flags.IS_COMPACT.ordinal())
      | (1 << Flags.IS_READ_ONLY.ordinal())
      | (isEmpty() ? 1 << Flags.IS_EMPTY.ordinal() : 0)
    );
    // an empty image carries zero, as in theta
    ByteArrayUtil.putShortLE(bytes, SEED_HASH_SHORT, isEmpty() ? 0 : seedHash_);
    if (!isEmpty()) {
      if (!isSingleItem) {
        ByteArrayUtil.putIntLE(bytes, RETAINED_ENTRIES_INT, count);
        if (isEstimationMode()) {
          ByteArrayUtil.putLongLE(bytes, THETA_LONG, thetaLong_);
        }
      }
    }
    int offset = preambleLongs * Long.BYTES; // the entries start after the preamble
    for (int i = 0; i < count; i++) {
      ByteArrayUtil.putLongLE(bytes, offset, hashArr_[i]);
      offset += Long.BYTES;
      System.arraycopy(summariesBytes[i], 0, bytes, offset, summariesBytes[i].length);
      offset += summariesBytes[i].length;
    }
    return bytes;
  }

  @Override
  public TupleSketchIterator<S> iterator() {
    return new TupleSketchIterator<>(hashArr_, summaryArr_);
  }

}
