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

import static org.apache.datasketches.HashOperations.count;

import java.lang.reflect.Array;
import java.nio.ByteOrder;

import org.apache.datasketches.ByteArrayUtil;
import org.apache.datasketches.Family;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;

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
public class CompactSketch<S extends Summary> extends Sketch<S> {
  private static final byte serialVersionWithSummaryClassNameUID = 1;
  private static final byte serialVersionUIDLegacy = 2;
  private static final byte serialVersionUID = 3;
  private static final short defaultSeedHash = (short) 37836; // for compatibility with C++
  private long[] hashArr_;
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
    hashArr_ = hashArr;
    summaryArr_ = summaryArr;
    thetaLong_ = thetaLong;
    empty_ = empty;
  }

  /**
   * This is to create an instance of a CompactSketch given a serialized form
   *
   * @param mem Memory object with serialized CompactSketch
   * @param deserializer the SummaryDeserializer
   */
  CompactSketch(final Memory mem, final SummaryDeserializer<S> deserializer) {
    int offset = 0;
    final byte preambleLongs = mem.getByte(offset++);
    final byte version = mem.getByte(offset++);
    final byte familyId = mem.getByte(offset++);
    SerializerDeserializer.validateFamily(familyId, preambleLongs);
    if (version > serialVersionUID) {
      throw new SketchesArgumentException(
          "Unsupported serial version. Expected: " + serialVersionUID + " or lower, actual: " + version);
    }
    SerializerDeserializer
      .validateType(mem.getByte(offset++), SerializerDeserializer.SketchType.CompactSketch);
    if (version <= serialVersionUIDLegacy) { // legacy serial format
      final byte flags = mem.getByte(offset++);
      final boolean isBigEndian = (flags & 1 << FlagsLegacy.IS_BIG_ENDIAN.ordinal()) > 0;
      if (isBigEndian ^ ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN)) {
        throw new SketchesArgumentException("Byte order mismatch");
      }
      empty_ = (flags & 1 << FlagsLegacy.IS_EMPTY.ordinal()) > 0;
      final boolean isThetaIncluded = (flags & 1 << FlagsLegacy.IS_THETA_INCLUDED.ordinal()) > 0;
      if (isThetaIncluded) {
        thetaLong_ = mem.getLong(offset);
        offset += Long.BYTES;
      } else {
        thetaLong_ = Long.MAX_VALUE;
      }
      final boolean hasEntries = (flags & 1 << FlagsLegacy.HAS_ENTRIES.ordinal()) > 0;
      if (hasEntries) {
        int classNameLength = 0;
        if (version == serialVersionWithSummaryClassNameUID) {
          classNameLength = mem.getByte(offset++);
        }
        final int count = mem.getInt(offset);
        offset += Integer.BYTES;
        if (version == serialVersionWithSummaryClassNameUID) {
          offset += classNameLength;
        }
        hashArr_ = new long[count];
        for (int i = 0; i < count; i++) {
          hashArr_[i] = mem.getLong(offset);
          offset += Long.BYTES;
        }
        for (int i = 0; i < count; i++) {
          offset += readSummary(mem, offset, i, count, deserializer);
        }
      }
    } else { // current serial format
      offset++; //skip unused byte
      final byte flags = mem.getByte(offset++);
      offset += 2; //skip 2 unused bytes
      empty_ = (flags & 1 << Flags.IS_EMPTY.ordinal()) > 0;
      thetaLong_ = Long.MAX_VALUE;
      int count = 0;
      if (!empty_) {
        if (preambleLongs == 1) {
          count = 1;
        } else {
          count = mem.getInt(offset);
          offset += Integer.BYTES;
          offset += 4; // unused
          if (preambleLongs > 2) {
            thetaLong_ = mem.getLong(offset);
            offset += Long.BYTES;
          }
        }
      }
      hashArr_ = new long[count];
      for (int i = 0; i < count; i++) {
        hashArr_[i] = mem.getLong(offset);
        offset += Long.BYTES;
        offset += readSummary(mem, offset, i, count, deserializer);
      }
    }
  }

  @SuppressWarnings({"unchecked"})
  private int readSummary(final Memory mem, final int offset, final int i, final int count,
      final SummaryDeserializer<S> deserializer) {
    final Memory memRegion = mem.region(offset, mem.getCapacity() - offset);
    final DeserializeResult<S> result = deserializer.heapifySummary(memRegion);
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
  public SketchIterator<S> iterator() {
    return new SketchIterator<>(hashArr_, summaryArr_);
  }

}
