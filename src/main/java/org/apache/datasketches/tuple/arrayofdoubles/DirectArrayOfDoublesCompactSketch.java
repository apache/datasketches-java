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

package org.apache.datasketches.tuple.arrayofdoubles;

import java.nio.ByteOrder;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.tuple.SerializerDeserializer;
import org.apache.datasketches.tuple.Util;

/**
 * Direct Compact Sketch of type ArrayOfDoubles.
 *
 * <p>This implementation uses data in a given Memory that is owned and managed by the caller.
 * This Memory can be off-heap, which if managed properly will greatly reduce the need for
 * the JVM to perform garbage collection.</p>
 */
final class DirectArrayOfDoublesCompactSketch extends ArrayOfDoublesCompactSketch {

  // this value exists only on heap, never serialized
  private Memory mem_;

  /**
   * Converts the given UpdatableArrayOfDoublesSketch to this compact form.
   * @param sketch the given UpdatableArrayOfDoublesSketch
   * @param dstMem the given destination Memory.
   */
  DirectArrayOfDoublesCompactSketch(final ArrayOfDoublesUpdatableSketch sketch,
      final WritableMemory dstMem) {
    this(sketch, sketch.getThetaLong(), dstMem);
  }

  /**
   * Converts the given UpdatableArrayOfDoublesSketch to this compact form
   * trimming if necessary according to given theta
   * @param sketch the given UpdatableArrayOfDoublesSketch
   * @param thetaLong new value of thetaLong
   * @param dstMem the given destination Memory.
   */
  DirectArrayOfDoublesCompactSketch(final ArrayOfDoublesUpdatableSketch sketch,
      final long thetaLong, final WritableMemory dstMem) {
    super(sketch.getNumValues());
    checkIfEnoughMemory(dstMem, sketch.getRetainedEntries(), sketch.getNumValues());
    mem_ = dstMem;
    dstMem.putByte(PREAMBLE_LONGS_BYTE, (byte) 1);
    dstMem.putByte(SERIAL_VERSION_BYTE, serialVersionUID);
    dstMem.putByte(FAMILY_ID_BYTE, (byte) Family.TUPLE.getID());
    dstMem.putByte(SKETCH_TYPE_BYTE, (byte)
        SerializerDeserializer.SketchType.ArrayOfDoublesCompactSketch.ordinal());
    final boolean isBigEndian = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);
    isEmpty_ = sketch.isEmpty();
    final int count = sketch.getRetainedEntries();
    dstMem.putByte(FLAGS_BYTE, (byte) (
      (isBigEndian ? 1 << Flags.IS_BIG_ENDIAN.ordinal() : 0)
      | (isEmpty_ ? 1 << Flags.IS_EMPTY.ordinal() : 0)
      | (count > 0 ? 1 << Flags.HAS_ENTRIES.ordinal() : 0)
    ));
    dstMem.putByte(NUM_VALUES_BYTE, (byte) numValues_);
    dstMem.putShort(SEED_HASH_SHORT, Util.computeSeedHash(sketch.getSeed()));
    thetaLong_ = Math.min(sketch.getThetaLong(), thetaLong);
    dstMem.putLong(THETA_LONG, thetaLong_);
    if (count > 0) {
      int keyOffset = ENTRIES_START;
      int valuesOffset = keyOffset + (SIZE_OF_KEY_BYTES * sketch.getRetainedEntries());
      final ArrayOfDoublesSketchIterator it = sketch.iterator();
      int actualCount = 0;
      while (it.next()) {
        if (it.getKey() < thetaLong_) {
          dstMem.putLong(keyOffset, it.getKey());
          dstMem.putDoubleArray(valuesOffset, it.getValues(), 0, numValues_);
          keyOffset += SIZE_OF_KEY_BYTES;
          valuesOffset += SIZE_OF_VALUE_BYTES * numValues_;
          actualCount++;
        }
      }
      dstMem.putInt(RETAINED_ENTRIES_INT, actualCount);
    }
  }

  /*
   * Creates an instance from components
   */
  DirectArrayOfDoublesCompactSketch(final long[] keys, final double[] values, final long thetaLong,
      final boolean isEmpty, final int numValues, final short seedHash, final WritableMemory dstMem) {
    super(numValues);
    checkIfEnoughMemory(dstMem, values.length, numValues);
    mem_ = dstMem;
    dstMem.putByte(PREAMBLE_LONGS_BYTE, (byte) 1);
    dstMem.putByte(SERIAL_VERSION_BYTE, serialVersionUID);
    dstMem.putByte(FAMILY_ID_BYTE, (byte) Family.TUPLE.getID());
    dstMem.putByte(SKETCH_TYPE_BYTE, (byte)
        SerializerDeserializer.SketchType.ArrayOfDoublesCompactSketch.ordinal());
    final boolean isBigEndian = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);
    isEmpty_ = isEmpty;
    final int count = keys.length;
    dstMem.putByte(FLAGS_BYTE, (byte) (
      (isBigEndian ? 1 << Flags.IS_BIG_ENDIAN.ordinal() : 0)
      | (isEmpty_ ? 1 << Flags.IS_EMPTY.ordinal() : 0)
      | (count > 0 ? 1 << Flags.HAS_ENTRIES.ordinal() : 0)
    ));
    dstMem.putByte(NUM_VALUES_BYTE, (byte) numValues_);
    dstMem.putShort(SEED_HASH_SHORT, seedHash);
    thetaLong_ = thetaLong;
    dstMem.putLong(THETA_LONG, thetaLong_);
    if (count > 0) {
      dstMem.putInt(RETAINED_ENTRIES_INT, count);
      dstMem.putLongArray(ENTRIES_START, keys, 0, count);
      dstMem.putDoubleArray(
          ENTRIES_START + ((long) SIZE_OF_KEY_BYTES * count), values, 0, values.length);
    }
  }

  /**
   * Wraps the given Memory.
   * @param mem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   */
  DirectArrayOfDoublesCompactSketch(final Memory mem) {
    super(mem.getByte(NUM_VALUES_BYTE));
    mem_ = mem;
    SerializerDeserializer.validateFamily(mem.getByte(FAMILY_ID_BYTE),
        mem.getByte(PREAMBLE_LONGS_BYTE));
    SerializerDeserializer.validateType(mem_.getByte(SKETCH_TYPE_BYTE),
        SerializerDeserializer.SketchType.ArrayOfDoublesCompactSketch);
    final byte version = mem_.getByte(SERIAL_VERSION_BYTE);
    if (version != serialVersionUID) {
      throw new SketchesArgumentException("Serial version mismatch. Expected: " + serialVersionUID
          + ", actual: " + version);
    }
    final boolean isBigEndian =
        (mem.getByte(FLAGS_BYTE) & (1 << Flags.IS_BIG_ENDIAN.ordinal())) != 0;
    if (isBigEndian ^ ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN)) {
      throw new SketchesArgumentException("Byte order mismatch");
    }

    isEmpty_ = (mem_.getByte(FLAGS_BYTE) & (1 << Flags.IS_EMPTY.ordinal())) != 0;
    thetaLong_ = mem_.getLong(THETA_LONG);
  }

  /**
   * Wraps the given Memory.
   * @param mem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   */
  DirectArrayOfDoublesCompactSketch(final Memory mem, final long seed) {
    super(mem.getByte(NUM_VALUES_BYTE));
    mem_ = mem;
    SerializerDeserializer.validateFamily(mem.getByte(FAMILY_ID_BYTE),
        mem.getByte(PREAMBLE_LONGS_BYTE));
    SerializerDeserializer.validateType(mem_.getByte(SKETCH_TYPE_BYTE),
        SerializerDeserializer.SketchType.ArrayOfDoublesCompactSketch);
    final byte version = mem_.getByte(SERIAL_VERSION_BYTE);
    if (version != serialVersionUID) {
      throw new SketchesArgumentException("Serial version mismatch. Expected: " + serialVersionUID
          + ", actual: " + version);
    }
    final boolean isBigEndian =
        (mem.getByte(FLAGS_BYTE) & (1 << Flags.IS_BIG_ENDIAN.ordinal())) != 0;
    if (isBigEndian ^ ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN)) {
      throw new SketchesArgumentException("Byte order mismatch");
    }
    Util.checkSeedHashes(mem.getShort(SEED_HASH_SHORT), Util.computeSeedHash(seed));
    isEmpty_ = (mem_.getByte(FLAGS_BYTE) & (1 << Flags.IS_EMPTY.ordinal())) != 0;
    thetaLong_ = mem_.getLong(THETA_LONG);
  }

  @Override
  public ArrayOfDoublesCompactSketch compact(final WritableMemory dstMem) {
    if (dstMem == null) {
      return new
          HeapArrayOfDoublesCompactSketch(getKeys(), getValuesAsOneDimension(), thetaLong_, isEmpty_, numValues_,
              getSeedHash());
    } else {
      mem_.copyTo(0, dstMem, 0, mem_.getCapacity());
      return new DirectArrayOfDoublesCompactSketch(dstMem);
    }
  }

  @Override
  public int getRetainedEntries() {
    final boolean hasEntries =
        (mem_.getByte(FLAGS_BYTE) & (1 << Flags.HAS_ENTRIES.ordinal())) != 0;
    return (hasEntries ? mem_.getInt(RETAINED_ENTRIES_INT) : 0);
  }

  @Override
  //converts compact Memory array of double[] to compact double[][]
  public double[][] getValues() {
    final int count = getRetainedEntries();
    final double[][] values = new double[count][];
    if (count > 0) {
      int valuesOffset = ENTRIES_START + (SIZE_OF_KEY_BYTES * count);
      for (int i = 0; i < count; i++) {
        final double[] array = new double[numValues_];
        mem_.getDoubleArray(valuesOffset, array, 0, numValues_);
        values[i] = array;
        valuesOffset += SIZE_OF_VALUE_BYTES * numValues_;
      }
    }
    return values;
  }

  @Override
  //converts compact Memory array of double[] to compact double[]
  double[] getValuesAsOneDimension() {
    final int count = getRetainedEntries();
    final int numDoubles = count * numValues_;
    final double[] values = new double[numDoubles];
    if (count > 0) {
      final int valuesOffset = ENTRIES_START + (SIZE_OF_KEY_BYTES * count);
      mem_.getDoubleArray(valuesOffset, values, 0, numDoubles);
    }
    return values;
  }

  @Override
  //converts compact Memory array of long[] to compact long[]
  long[] getKeys() {
    final int count = getRetainedEntries();
    final long[] keys = new long[count];
    if (count > 0) {
      for (int i = 0; i < count; i++) {
        mem_.getLongArray(ENTRIES_START, keys, 0, count);
      }
    }
    return keys;
  }

  @Override
  public byte[] toByteArray() {
    final int sizeBytes = getCurrentBytes();
    final byte[] byteArray = new byte[sizeBytes];
    final WritableMemory mem = WritableMemory.writableWrap(byteArray);
    mem_.copyTo(0, mem, 0, sizeBytes);
    return byteArray;
  }

  @Override
  public ArrayOfDoublesSketchIterator iterator() {
    return new DirectArrayOfDoublesSketchIterator(
        mem_, ENTRIES_START, getRetainedEntries(), numValues_);
  }

  @Override
  short getSeedHash() {
    return mem_.getShort(SEED_HASH_SHORT);
  }

  @Override
  public boolean hasMemory() { return true; }

  @Override
  Memory getMemory() { return mem_; }

  private static void checkIfEnoughMemory(final Memory mem, final int numEntries,
      final int numValues) {
    final int sizeNeeded =
        ENTRIES_START + ((SIZE_OF_KEY_BYTES + (SIZE_OF_VALUE_BYTES * numValues)) * numEntries);
    if (sizeNeeded > mem.getCapacity()) {
      throw new SketchesArgumentException("Not enough memory: need " + sizeNeeded
          + " bytes, got " + mem.getCapacity() + " bytes");
    }
  }

}
