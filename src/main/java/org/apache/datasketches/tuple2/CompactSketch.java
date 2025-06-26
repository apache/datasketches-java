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

package org.apache.datasketches.tuple2;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static org.apache.datasketches.thetacommon2.HashOperations.count;

import java.lang.foreign.MemorySegment;
import java.lang.reflect.Array;
import java.nio.ByteOrder;

import org.apache.datasketches.common.ByteArrayUtil;
import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;

/**
 * CompactSketches are never created directly. They are created as a result of
 * the compact() method of an UpdatableSketch or as a result of the getResult()
 * method of a set operation like Union, Intersection or AnotB. CompactSketch
 * consists of a compact list (i.e. no intervening spaces) of hash values,
 * corresponding list of Summaries, and a value for theta. The lists may or may
 * not be ordered. CompactSketch is read-only.
 *
 * @param <S> type of Summary
 */
public final class CompactSketch<S extends Summary> extends Sketch<S> {
  private static final byte serialVersionWithSummaryClassNameUID = 1;
  private static final byte serialVersionUIDLegacy = 2;
  private static final byte serialVersionUID = 3;
  private static final short defaultSeedHash = (short) 37836; // for compatibility with C++
  private final long[] hashArr_;
  private S[] summaryArr_;

  private enum FlagsLegacy { IS_BIG_ENDIAN, IS_EMPTY, HAS_ENTRIES, IS_THETA_INCLUDED }

  private enum Flags { IS_BIG_ENDIAN, IS_READ_ONLY, IS_EMPTY, IS_COMPACT, IS_ORDERED }

  /**
   * Create a CompactSketch from correct components
   * @param hashArr compacted hash array
   * @param summaryArr compacted summary array
   * @param thetaLong long value of theta
   * @param empty empty flag
   */
  CompactSketch(final long[] hashArr, final S[] summaryArr, final long thetaLong, final boolean empty) {
    super(thetaLong, empty, null);
    super.thetaLong_ = thetaLong;
    super.empty_ = empty;
    hashArr_ = hashArr;
    summaryArr_ = summaryArr;
  }

  /**
   * This is to create an instance of a CompactSketch given a serialized form
   *
   * @param seg MemorySegment object with serialized CompactSketch
   * @param deserializer the SummaryDeserializer
   */
  CompactSketch(final MemorySegment seg, final SummaryDeserializer<S> deserializer) {
    super(Long.MAX_VALUE, true, null);
    int offset = 0;
    final byte preambleLongs = seg.get(JAVA_BYTE, offset++);
    final byte version = seg.get(JAVA_BYTE, offset++);
    final byte familyId = seg.get(JAVA_BYTE, offset++);
    SerializerDeserializer.validateFamily(familyId, preambleLongs);
    if (version > serialVersionUID) {
      throw new SketchesArgumentException(
          "Unsupported serial version. Expected: " + serialVersionUID + " or lower, actual: " + version);
    }
    SerializerDeserializer
      .validateType(seg.get(JAVA_BYTE, offset++), SerializerDeserializer.SketchType.CompactSketch);
    if (version <= serialVersionUIDLegacy) { // legacy serial format
      final byte flags = seg.get(JAVA_BYTE, offset++);
      final boolean isBigEndian = (flags & 1 << FlagsLegacy.IS_BIG_ENDIAN.ordinal()) > 0;
      if (isBigEndian ^ ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN)) {
        throw new SketchesArgumentException("Byte order mismatch");
      }
      empty_ = (flags & 1 << FlagsLegacy.IS_EMPTY.ordinal()) > 0;
      final boolean isThetaIncluded = (flags & 1 << FlagsLegacy.IS_THETA_INCLUDED.ordinal()) > 0;
      if (isThetaIncluded) {
        thetaLong_ = seg.get(JAVA_LONG_UNALIGNED, offset);
        offset += Long.BYTES;
      } else {
        thetaLong_ = Long.MAX_VALUE;
      }
      final boolean hasEntries = (flags & 1 << FlagsLegacy.HAS_ENTRIES.ordinal()) > 0;
      if (hasEntries) {
        int classNameLength = 0;
        if (version == serialVersionWithSummaryClassNameUID) {
          classNameLength = seg.get(JAVA_BYTE, offset++);
        }
        final int count = seg.get(JAVA_INT_UNALIGNED, offset);
        offset += Integer.BYTES;
        if (version == serialVersionWithSummaryClassNameUID) {
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
      offset++; //skip unused byte
      final byte flags = seg.get(JAVA_BYTE, offset++);
      offset += 2; //skip 2 unused bytes
      empty_ = (flags & 1 << Flags.IS_EMPTY.ordinal()) > 0;
      thetaLong_ = Long.MAX_VALUE;
      int count = 0;
      if (!empty_) {
        if (preambleLongs == 1) {
          count = 1;
        } else {
          count = seg.get(JAVA_INT_UNALIGNED, offset);
          offset += Integer.BYTES;
          offset += 4; // unused
          if (preambleLongs > 2) {
            thetaLong_ = seg.get(JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;
          }
        }
      }
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
  public CompactSketch<S> compact() {
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
    final boolean isSingleItem = count == 1 && !isEstimationMode();
    final int preambleLongs = isEmpty() || isSingleItem ? 1 : isEstimationMode() ? 3 : 2;

    int summariesSizeBytes = 0;
    final byte[][] summariesBytes = new byte[count][];
    if (count > 0) {
      for (int i = 0; i < count; i++) {
        summariesBytes[i] = summaryArr_[i].toByteArray();
        summariesSizeBytes += summariesBytes[i].length;
      }
    }

    final int sizeBytes = Long.BYTES * preambleLongs + Long.BYTES * count + summariesSizeBytes;
    final byte[] bytes = new byte[sizeBytes];
    int offset = 0;
    bytes[offset++] = (byte) preambleLongs;
    bytes[offset++] = serialVersionUID;
    bytes[offset++] = (byte) Family.TUPLE.getID();
    bytes[offset++] = (byte) SerializerDeserializer.SketchType.CompactSketch.ordinal();
    offset++; // unused
    bytes[offset++] = (byte) (
        (1 << Flags.IS_COMPACT.ordinal())
      | (1 << Flags.IS_READ_ONLY.ordinal())
      | (isEmpty() ? 1 << Flags.IS_EMPTY.ordinal() : 0)
    );
    ByteArrayUtil.putShortLE(bytes, offset, defaultSeedHash);
    offset += Short.BYTES;
    if (!isEmpty()) {
      if (!isSingleItem) {
        ByteArrayUtil.putIntLE(bytes, offset, count);
        offset += Integer.BYTES;
        offset += 4; // unused
        if (isEstimationMode()) {
          ByteArrayUtil.putLongLE(bytes, offset, thetaLong_);
          offset += Long.BYTES;
        }
      }
    }
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
