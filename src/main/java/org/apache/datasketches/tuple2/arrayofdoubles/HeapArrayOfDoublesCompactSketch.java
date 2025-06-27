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

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;
import java.util.Arrays;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.tuple2.SerializerDeserializer;

/**
 * The on-heap implementation of tuple Compact Sketch of type ArrayOfDoubles.
 */
final class HeapArrayOfDoublesCompactSketch extends ArrayOfDoublesCompactSketch {

  private final short seedHash_;
  private long[] keys_;
  private double[] values_;

  /**
   * Converts the given UpdatableArrayOfDoublesSketch to this compact form.
   * @param sketch the given UpdatableArrayOfDoublesSketch
   */
  HeapArrayOfDoublesCompactSketch(final ArrayOfDoublesUpdatableSketch sketch) {
    this(sketch, sketch.getThetaLong());
  }

  /**
   * Converts the given UpdatableArrayOfDoublesSketch to this compact form
   * trimming if necessary according to given thetaLong
   * @param sketch the given UpdatableArrayOfDoublesSketch
   * @param thetaLong new value of thetaLong
   */
  HeapArrayOfDoublesCompactSketch(final ArrayOfDoublesUpdatableSketch sketch, final long thetaLong) {
    super(sketch.getNumValues());
    isEmpty_ = sketch.isEmpty();
    thetaLong_ = Math.min(sketch.getThetaLong(), thetaLong);
    seedHash_ = Util.computeSeedHash(sketch.getSeed());
    final int count = sketch.getRetainedEntries();
    if (count > 0) {
      keys_ = new long[count];
      values_ = new double[count * numValues_];
      final ArrayOfDoublesSketchIterator it = sketch.iterator();
      int i = 0;
      while (it.next()) {
        final long key = it.getKey();
        if (key < thetaLong_) {
          keys_[i] = key;
          System.arraycopy(it.getValues(), 0, values_, i * numValues_, numValues_);
          i++;
        }
      }
      // trim if necessary
      if (i < count) {
        if (i == 0) {
          keys_ = null;
          values_ = null;
        } else {
          keys_ = Arrays.copyOf(keys_, i);
          values_ = Arrays.copyOf(values_, i * numValues_);
        }
      }
    }
  }

  /*
   * Creates an instance from components
   */
  HeapArrayOfDoublesCompactSketch(final long[] keys, final double[] values, final long thetaLong,
      final boolean isEmpty, final int numValues, final short seedHash) {
    super(numValues);
    keys_ = keys;
    values_ = values;
    thetaLong_ = thetaLong;
    isEmpty_ = isEmpty;
    seedHash_ = seedHash;
  }

  /**
   * This is to create an instance given a serialized form
   * @param seg the destination segment
   */
  HeapArrayOfDoublesCompactSketch(final MemorySegment seg) {
    this(seg, Util.DEFAULT_UPDATE_SEED);
  }

  /**
   * This is to create an instance given a serialized form
   * @param seg the source MemorySegment
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   */
  HeapArrayOfDoublesCompactSketch(final MemorySegment seg, final long seed) {
    super(seg.get(JAVA_BYTE, NUM_VALUES_BYTE));
    seedHash_ = seg.get(JAVA_SHORT_UNALIGNED, SEED_HASH_SHORT);
    SerializerDeserializer.validateFamily(seg.get(JAVA_BYTE, FAMILY_ID_BYTE),
        seg.get(JAVA_BYTE, PREAMBLE_LONGS_BYTE));
    SerializerDeserializer.validateType(seg.get(JAVA_BYTE, SKETCH_TYPE_BYTE),
        SerializerDeserializer.SketchType.ArrayOfDoublesCompactSketch);
    final byte version = seg.get(JAVA_BYTE, SERIAL_VERSION_BYTE);
    if (version != serialVersionUID) {
      throw new SketchesArgumentException(
          "Serial version mismatch. Expected: " + serialVersionUID + ", actual: " + version);
    }
    final boolean isBigEndian =
        (seg.get(JAVA_BYTE, FLAGS_BYTE) & (1 << Flags.IS_BIG_ENDIAN.ordinal())) != 0;
    if (isBigEndian ^ ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN)) {
      throw new SketchesArgumentException("Byte order mismatch");
    }
    Util.checkSeedHashes(seedHash_, Util.computeSeedHash(seed));
    isEmpty_ = (seg.get(JAVA_BYTE, FLAGS_BYTE) & (1 << Flags.IS_EMPTY.ordinal())) != 0;
    thetaLong_ = seg.get(JAVA_LONG_UNALIGNED, THETA_LONG);
    final boolean hasEntries =
        (seg.get(JAVA_BYTE, FLAGS_BYTE) & (1 << Flags.HAS_ENTRIES.ordinal())) != 0;
    if (hasEntries) {
      final int count = seg.get(JAVA_INT_UNALIGNED, RETAINED_ENTRIES_INT);
      keys_ = new long[count];
      values_ = new double[count * numValues_];
      MemorySegment.copy(seg, JAVA_LONG_UNALIGNED, ENTRIES_START, keys_, 0, count);
      MemorySegment.copy(seg, JAVA_DOUBLE_UNALIGNED, ENTRIES_START + ((long) SIZE_OF_KEY_BYTES * count), values_, 0, values_.length);
    }
  }

  @Override
  public ArrayOfDoublesCompactSketch compact(final MemorySegment dstSeg) {
   if (dstSeg == null) {
      return new
          HeapArrayOfDoublesCompactSketch(keys_.clone(), values_.clone(), thetaLong_, isEmpty_, numValues_, seedHash_);
    } else {
      final byte[] byteArr = this.toByteArray();
      MemorySegment.copy(byteArr, 0, dstSeg, JAVA_BYTE, 0, byteArr.length);
      return new DirectArrayOfDoublesCompactSketch(dstSeg);
    }
  }

  @Override
  public int getRetainedEntries() {
    return keys_ == null ? 0 : keys_.length;
  }

  @Override
  public byte[] toByteArray() {
    final int count = getRetainedEntries();
    final int sizeBytes = getCurrentBytes();
    final byte[] bytes = new byte[sizeBytes];
    final MemorySegment seg = MemorySegment.ofArray(bytes);
    seg.set(JAVA_BYTE, PREAMBLE_LONGS_BYTE, (byte) 1);
    seg.set(JAVA_BYTE, SERIAL_VERSION_BYTE, serialVersionUID);
    seg.set(JAVA_BYTE, FAMILY_ID_BYTE, (byte) Family.TUPLE.getID());
    seg.set(JAVA_BYTE, SKETCH_TYPE_BYTE, (byte) SerializerDeserializer.SketchType.ArrayOfDoublesCompactSketch.ordinal());
    final boolean isBigEndian = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);
    seg.set(JAVA_BYTE, FLAGS_BYTE, (byte) (
      ((isBigEndian ? 1 : 0) << Flags.IS_BIG_ENDIAN.ordinal())
      | ((isEmpty() ? 1 : 0) << Flags.IS_EMPTY.ordinal())
      | ((count > 0 ? 1 : 0) << Flags.HAS_ENTRIES.ordinal())
    ));
    seg.set(JAVA_BYTE, NUM_VALUES_BYTE, (byte) numValues_);
    seg.set(JAVA_SHORT_UNALIGNED, SEED_HASH_SHORT, seedHash_);
    seg.set(JAVA_LONG_UNALIGNED, THETA_LONG, thetaLong_);
    if (count > 0) {
      seg.set(JAVA_INT_UNALIGNED, RETAINED_ENTRIES_INT, count);
      MemorySegment.copy(keys_, 0, seg, JAVA_LONG_UNALIGNED, ENTRIES_START, count);
      MemorySegment.copy(values_, 0, seg, JAVA_DOUBLE_UNALIGNED, ENTRIES_START + ((long) SIZE_OF_KEY_BYTES * count), values_.length);
    }
    return bytes;
  }

  @Override
  //converts compact heap array of double[] to compact double[][]
  public double[][] getValues() {
    final int count = getRetainedEntries();
    final double[][] values = new double[count][];
    if (count > 0) {
      int i = 0;
      for (int j = 0; j < count; j++) {
        values[i++] = Arrays.copyOfRange(values_, j * numValues_, (j + 1) * numValues_);
      }
    }
    return values;
  }

  @Override
  double[] getValuesAsOneDimension() {
    return values_.clone();
  }

  @Override
  long[] getKeys() {
    return keys_.clone();
  }

  @Override
  public ArrayOfDoublesSketchIterator iterator() {
    return new HeapArrayOfDoublesSketchIterator(keys_, values_, numValues_);
  }

  @Override
  short getSeedHash() {
    return seedHash_;
  }

  @Override
  public boolean hasMemorySegment() { return false; }

  @Override
  MemorySegment getMemorySegment() { return null; }
}
