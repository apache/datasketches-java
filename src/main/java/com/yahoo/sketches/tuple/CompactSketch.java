/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

import java.lang.reflect.Array;
import java.nio.ByteOrder;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.ByteArrayUtil;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;

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
  private static final byte serialVersionUID = 2;

  private enum Flags { IS_BIG_ENDIAN, IS_EMPTY, HAS_ENTRIES, IS_THETA_INCLUDED }

  CompactSketch(final long[] keys, final S[] summaries, final long theta, final boolean isEmpty) {
    keys_ = keys;
    summaries_ = summaries;
    theta_ = theta;
    isEmpty_ = isEmpty;
  }

  /**
   * This is to create an instance of a CompactSketch given a serialized form
   *
   * @param mem Memory object with serialized CompactSketch
   * @param deserializer the SummaryDeserializer
   */
  @SuppressWarnings({"unchecked"})
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
    final byte flags = mem.getByte(offset++);
    final boolean isBigEndian = (flags & (1 << Flags.IS_BIG_ENDIAN.ordinal())) > 0;
    if (isBigEndian ^ ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN)) {
      throw new SketchesArgumentException("Byte order mismatch");
    }
    isEmpty_ = (flags & (1 << Flags.IS_EMPTY.ordinal())) > 0;
    final boolean isThetaIncluded = (flags & (1 << Flags.IS_THETA_INCLUDED.ordinal())) > 0;
    if (isThetaIncluded) {
      theta_ = mem.getLong(offset);
      offset += Long.BYTES;
    } else {
      theta_ = Long.MAX_VALUE;
    }
    final boolean hasEntries = (flags & (1 << Flags.HAS_ENTRIES.ordinal())) > 0;
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
      keys_ = new long[count];
      for (int i = 0; i < count; i++) {
        keys_[i] = mem.getLong(offset);
        offset += Long.BYTES;
      }
      for (int i = 0; i < count; i++) {
        final Memory memRegion = mem.region(offset, mem.getCapacity() - offset);
        final DeserializeResult<S> result = deserializer.heapifySummary(memRegion);
        final S summary = result.getObject();
        offset += result.getSize();
        if (summaries_ == null) {
          summaries_ = (S[]) Array.newInstance(summary.getClass(), count);
        }
        summaries_[i] = summary;
      }
    }
  }

  @Override
  public int getRetainedEntries() {
    return keys_ == null ? 0 : keys_.length;
  }

  // Layout of first 8 bytes:
  // Long || Start Byte Adr:
  // Adr:
  //      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0              |
  //  0   ||                          |  Flags | SkType | FamID  | SerVer |  Preamble_Longs    |
  @SuppressWarnings("null")
  @Override
  public byte[] toByteArray() {
    int summariesBytesLength = 0;
    byte[][] summariesBytes = null;
    final int count = getRetainedEntries();
    if (count > 0) {
      summariesBytes = new byte[count][];
      for (int i = 0; i < count; i++) {
        summariesBytes[i] = summaries_[i].toByteArray();
        summariesBytesLength += summariesBytes[i].length;
      }
    }

    int sizeBytes =
        Byte.BYTES // preamble longs
      + Byte.BYTES // serial version
      + Byte.BYTES // family id
      + Byte.BYTES // sketch type
      + Byte.BYTES; // flags
    final boolean isThetaIncluded = theta_ < Long.MAX_VALUE;
    if (isThetaIncluded) {
      sizeBytes += Long.BYTES; // theta
    }
    if (count > 0) {
      sizeBytes +=
        + Integer.BYTES // count
        + (Long.BYTES * count) + summariesBytesLength;
    }
    final byte[] bytes = new byte[sizeBytes];
    int offset = 0;
    bytes[offset++] = PREAMBLE_LONGS;
    bytes[offset++] = serialVersionUID;
    bytes[offset++] = (byte) Family.TUPLE.getID();
    bytes[offset++] = (byte) SerializerDeserializer.SketchType.CompactSketch.ordinal();
    final boolean isBigEndian = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);
    bytes[offset++] = (byte) (
      (isBigEndian ? 1 << Flags.IS_BIG_ENDIAN.ordinal() : 0)
      | (isEmpty_ ? 1 << Flags.IS_EMPTY.ordinal() : 0)
      | (count > 0 ? 1 << Flags.HAS_ENTRIES.ordinal() : 0)
      | (isThetaIncluded ? 1 << Flags.IS_THETA_INCLUDED.ordinal() : 0)
    );
    if (isThetaIncluded) {
      ByteArrayUtil.putLongLE(bytes, offset, theta_);
      offset += Long.BYTES;
    }
    if (count > 0) {
      ByteArrayUtil.putIntLE(bytes, offset, getRetainedEntries());
      offset += Integer.BYTES;
      for (int i = 0; i < count; i++) {
        ByteArrayUtil.putLongLE(bytes, offset, keys_[i]);
        offset += Long.BYTES;
      }
      for (int i = 0; i < count; i++) {
        System.arraycopy(summariesBytes[i], 0, bytes, offset, summariesBytes[i].length);
        offset += summariesBytes[i].length;
      }
    }
    return bytes;
  }

}
