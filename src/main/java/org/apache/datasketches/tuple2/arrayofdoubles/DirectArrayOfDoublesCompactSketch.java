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

package org.apache.datasketches.tuple2.arrayofdoubles;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_DOUBLE_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_SHORT_UNALIGNED;
import static org.apache.datasketches.thetacommon2.ThetaUtil.checkSeedHashes;
import static org.apache.datasketches.thetacommon2.ThetaUtil.computeSeedHash;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.tuple2.SerializerDeserializer;

/**
 * Direct Compact Sketch of type ArrayOfDoubles.
 *
 * <p>This implementation uses data in a given MemorySegment that is owned and managed by the caller.
 * This MemorySegment can be off-heap, which if managed properly will greatly reduce the need for
 * the JVM to perform garbage collection.</p>
 */
final class DirectArrayOfDoublesCompactSketch extends ArrayOfDoublesCompactSketch {

  // this value exists only on heap, never serialized
  private MemorySegment seg_;

  /**
   * Converts the given UpdatableArrayOfDoublesSketch to this compact form.
   * @param sketch the given UpdatableArrayOfDoublesSketch
   * @param dstSeg the given destination MemorySegment.
   */
  DirectArrayOfDoublesCompactSketch(final ArrayOfDoublesUpdatableSketch sketch,
      final MemorySegment dstSeg) {
    this(sketch, sketch.getThetaLong(), dstSeg);
  }

  /**
   * Converts the given UpdatableArrayOfDoublesSketch to this compact form
   * trimming if necessary according to given theta
   * @param sketch the given UpdatableArrayOfDoublesSketch
   * @param thetaLong new value of thetaLong
   * @param dstSeg the given destination MemorySegment.
   */
  DirectArrayOfDoublesCompactSketch(final ArrayOfDoublesUpdatableSketch sketch,
      final long thetaLong, final MemorySegment dstSeg) {
    super(sketch.getNumValues());
    checkMemorySegmentSize(dstSeg, sketch.getRetainedEntries(), sketch.getNumValues());
    seg_ = dstSeg;
    dstSeg.set(JAVA_BYTE, PREAMBLE_LONGS_BYTE, (byte) 1);
    dstSeg.set(JAVA_BYTE, SERIAL_VERSION_BYTE, serialVersionUID);
    dstSeg.set(JAVA_BYTE, FAMILY_ID_BYTE, (byte) Family.TUPLE.getID());
    dstSeg.set(JAVA_BYTE, SKETCH_TYPE_BYTE, (byte)
        SerializerDeserializer.SketchType.ArrayOfDoublesCompactSketch.ordinal());
    final boolean isBigEndian = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);
    isEmpty_ = sketch.isEmpty();
    final int count = sketch.getRetainedEntries();
    dstSeg.set(JAVA_BYTE, FLAGS_BYTE, (byte) (
      (isBigEndian ? 1 << Flags.IS_BIG_ENDIAN.ordinal() : 0)
      | (isEmpty_ ? 1 << Flags.IS_EMPTY.ordinal() : 0)
      | (count > 0 ? 1 << Flags.HAS_ENTRIES.ordinal() : 0)
    ));
    dstSeg.set(JAVA_BYTE, NUM_VALUES_BYTE, (byte) numValues_);
    dstSeg.set(JAVA_SHORT_UNALIGNED, SEED_HASH_SHORT, computeSeedHash(sketch.getSeed()));
    thetaLong_ = Math.min(sketch.getThetaLong(), thetaLong);
    dstSeg.set(JAVA_LONG_UNALIGNED, THETA_LONG, thetaLong_);
    if (count > 0) {
      int keyOffset = ENTRIES_START;
      int valuesOffset = keyOffset + (SIZE_OF_KEY_BYTES * sketch.getRetainedEntries());
      final ArrayOfDoublesSketchIterator it = sketch.iterator();
      int actualCount = 0;
      while (it.next()) {
        if (it.getKey() < thetaLong_) {
          dstSeg.set(JAVA_LONG_UNALIGNED, keyOffset, it.getKey());
          MemorySegment.copy(it.getValues(), 0, dstSeg, JAVA_DOUBLE_UNALIGNED, valuesOffset, numValues_);
          keyOffset += SIZE_OF_KEY_BYTES;
          valuesOffset += SIZE_OF_VALUE_BYTES * numValues_;
          actualCount++;
        }
      }
      dstSeg.set(JAVA_INT_UNALIGNED, RETAINED_ENTRIES_INT, actualCount);
    }
  }

  /*
   * Creates an instance from components
   */
  DirectArrayOfDoublesCompactSketch(final long[] keys, final double[] values, final long thetaLong,
      final boolean isEmpty, final int numValues, final short seedHash, final MemorySegment dstSeg) {
    super(numValues);
    checkMemorySegmentSize(dstSeg, values.length, numValues);
    seg_ = dstSeg;
    dstSeg.set(JAVA_BYTE, PREAMBLE_LONGS_BYTE, (byte) 1);
    dstSeg.set(JAVA_BYTE, SERIAL_VERSION_BYTE, serialVersionUID);
    dstSeg.set(JAVA_BYTE, FAMILY_ID_BYTE, (byte) Family.TUPLE.getID());
    dstSeg.set(JAVA_BYTE, SKETCH_TYPE_BYTE, (byte)
        SerializerDeserializer.SketchType.ArrayOfDoublesCompactSketch.ordinal());
    final boolean isBigEndian = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);
    isEmpty_ = isEmpty;
    final int count = keys.length;
    dstSeg.set(JAVA_BYTE, FLAGS_BYTE, (byte) (
      (isBigEndian ? 1 << Flags.IS_BIG_ENDIAN.ordinal() : 0)
      | (isEmpty_ ? 1 << Flags.IS_EMPTY.ordinal() : 0)
      | (count > 0 ? 1 << Flags.HAS_ENTRIES.ordinal() : 0)
    ));
    dstSeg.set(JAVA_BYTE, NUM_VALUES_BYTE, (byte) numValues_);
    dstSeg.set(JAVA_SHORT_UNALIGNED, SEED_HASH_SHORT, seedHash);
    thetaLong_ = thetaLong;
    dstSeg.set(JAVA_LONG_UNALIGNED, THETA_LONG, thetaLong_);
    if (count > 0) {
      dstSeg.set(JAVA_INT_UNALIGNED, RETAINED_ENTRIES_INT, count);
      MemorySegment.copy(keys, 0, dstSeg, JAVA_LONG_UNALIGNED, ENTRIES_START, count);
      MemorySegment.copy(values, 0, dstSeg, JAVA_DOUBLE_UNALIGNED, ENTRIES_START + ((long) SIZE_OF_KEY_BYTES * count), values.length);
    }
  }

  /**
   * Wraps the given MemorySegment.
   * @param seg the given MemorySegment
   */
  DirectArrayOfDoublesCompactSketch(final MemorySegment seg) {
    super(seg.get(JAVA_BYTE, NUM_VALUES_BYTE));
    seg_ = seg;
    SerializerDeserializer.validateFamily(seg.get(JAVA_BYTE, FAMILY_ID_BYTE),
        seg.get(JAVA_BYTE, PREAMBLE_LONGS_BYTE));
    SerializerDeserializer.validateType(seg_.get(JAVA_BYTE, SKETCH_TYPE_BYTE),
        SerializerDeserializer.SketchType.ArrayOfDoublesCompactSketch);
    final byte version = seg_.get(JAVA_BYTE, SERIAL_VERSION_BYTE);
    if (version != serialVersionUID) {
      throw new SketchesArgumentException("Serial version mismatch. Expected: " + serialVersionUID
          + ", actual: " + version);
    }
    final boolean isBigEndian =
        (seg.get(JAVA_BYTE, FLAGS_BYTE) & (1 << Flags.IS_BIG_ENDIAN.ordinal())) != 0;
    if (isBigEndian ^ ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN)) {
      throw new SketchesArgumentException("Byte order mismatch");
    }

    isEmpty_ = (seg_.get(JAVA_BYTE, FLAGS_BYTE) & (1 << Flags.IS_EMPTY.ordinal())) != 0;
    thetaLong_ = seg_.get(JAVA_LONG_UNALIGNED, THETA_LONG);
  }

  /**
   * Wraps the given MemorySegment.
   * @param seg the given MemorySegment.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   */
  DirectArrayOfDoublesCompactSketch(final MemorySegment seg, final long seed) {
    super(seg.get(JAVA_BYTE, NUM_VALUES_BYTE));
    seg_ = seg;
    SerializerDeserializer.validateFamily(seg.get(JAVA_BYTE, FAMILY_ID_BYTE),
        seg.get(JAVA_BYTE, PREAMBLE_LONGS_BYTE));
    SerializerDeserializer.validateType(seg_.get(JAVA_BYTE, SKETCH_TYPE_BYTE),
        SerializerDeserializer.SketchType.ArrayOfDoublesCompactSketch);
    final byte version = seg_.get(JAVA_BYTE, SERIAL_VERSION_BYTE);
    if (version != serialVersionUID) {
      throw new SketchesArgumentException("Serial version mismatch. Expected: " + serialVersionUID
          + ", actual: " + version);
    }
    final boolean isBigEndian =
        (seg.get(JAVA_BYTE, FLAGS_BYTE) & (1 << Flags.IS_BIG_ENDIAN.ordinal())) != 0;
    if (isBigEndian ^ ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN)) {
      throw new SketchesArgumentException("Byte order mismatch");
    }
    checkSeedHashes(seg.get(JAVA_SHORT_UNALIGNED, SEED_HASH_SHORT), computeSeedHash(seed));
    isEmpty_ = (seg_.get(JAVA_BYTE, FLAGS_BYTE) & (1 << Flags.IS_EMPTY.ordinal())) != 0;
    thetaLong_ = seg.get(JAVA_LONG_UNALIGNED, THETA_LONG);
  }

  @Override
  public ArrayOfDoublesCompactSketch compact(final MemorySegment dstSeg) {
    if (dstSeg == null) {
      return new
          HeapArrayOfDoublesCompactSketch(getKeys(), getValuesAsOneDimension(), thetaLong_, isEmpty_, numValues_,
              getSeedHash());
    } else {
      MemorySegment.copy(seg_, 0, dstSeg, 0, seg_.byteSize());
      return new DirectArrayOfDoublesCompactSketch(dstSeg);
    }
  }

  @Override
  public int getRetainedEntries() {
    final boolean hasEntries =
        (seg_.get(JAVA_BYTE, FLAGS_BYTE) & (1 << Flags.HAS_ENTRIES.ordinal())) != 0;
    return (hasEntries ? seg_.get(JAVA_INT_UNALIGNED, RETAINED_ENTRIES_INT) : 0);
  }

  @Override
  //converts compact MemorySegment array of double[] to compact double[][]
  public double[][] getValues() {
    final int count = getRetainedEntries();
    final double[][] values = new double[count][];
    if (count > 0) {
      int valuesOffset = ENTRIES_START + (SIZE_OF_KEY_BYTES * count);
      for (int i = 0; i < count; i++) {
        final double[] array = new double[numValues_];
        MemorySegment.copy(seg_, JAVA_DOUBLE_UNALIGNED, valuesOffset, array, 0, numValues_);
        values[i] = array;
        valuesOffset += SIZE_OF_VALUE_BYTES * numValues_;
      }
    }
    return values;
  }

  @Override
  //converts compact MemorySegment array of double[] to compact double[]
  double[] getValuesAsOneDimension() {
    final int count = getRetainedEntries();
    final int numDoubles = count * numValues_;
    final double[] values = new double[numDoubles];
    if (count > 0) {
      final int valuesOffset = ENTRIES_START + (SIZE_OF_KEY_BYTES * count);
      MemorySegment.copy(seg_, JAVA_DOUBLE_UNALIGNED, valuesOffset, values, 0, numDoubles);
    }
    return values;
  }

  @Override
  //converts compact MemorySegment array of long[] to compact long[]
  long[] getKeys() {
    final int count = getRetainedEntries();
    final long[] keys = new long[count];
    if (count > 0) {
      for (int i = 0; i < count; i++) {
        MemorySegment.copy(seg_, JAVA_LONG_UNALIGNED, ENTRIES_START, keys, 0, count);
      }
    }
    return keys;
  }

  @Override
  public byte[] toByteArray() {
    final int sizeBytes = getCurrentBytes();
    final byte[] byteArray = new byte[sizeBytes];
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    MemorySegment.copy(seg_, 0, seg, 0, sizeBytes);
    return byteArray;
  }

  @Override
  public ArrayOfDoublesSketchIterator iterator() {
    return new DirectArrayOfDoublesSketchIterator(
        seg_, ENTRIES_START, getRetainedEntries(), numValues_);
  }

  @Override
  short getSeedHash() {
    return seg_.get(JAVA_SHORT_UNALIGNED, SEED_HASH_SHORT);
  }

  @Override
  public boolean hasMemorySegment() { return true; }

  @Override
  MemorySegment getMemorySegment() { return seg_; }

  private static void checkMemorySegmentSize(final MemorySegment seg, final int numEntries,
      final int numValues) {
    final int sizeNeeded =
        ENTRIES_START + ((SIZE_OF_KEY_BYTES + (SIZE_OF_VALUE_BYTES * numValues)) * numEntries);
    if (sizeNeeded > seg.byteSize()) {
      throw new SketchesArgumentException("Not enough space: need " + sizeNeeded
          + " bytes, got " + seg.byteSize() + " bytes");
    }
  }

}
