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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static org.apache.datasketches.common.Util.ceilingPowerOf2;
import static org.apache.datasketches.common.Util.checkBounds;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.QuickSelect;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.thetacommon.ThetaUtil;

/**
 * Top level class for hash table based implementations of tuple sketch of type
 * ArrayOfDoubles that uses the QuickSelect algorithm.
 */
abstract class ArrayOfDoublesQuickSelectSketch extends ArrayOfDoublesUpdatableSketch {

  static final byte serialVersionUID = 1;

  // Layout of next 16 bytes:
  // Long || Start Byte Adr:
  // Adr:
  //      ||   23   |   22   |   21   |   20   |   19   |   18   |   17   |    16     |
  //  3   ||-----------P (float)---------------|--------|--lgRF--|--lgArr-|---lgNom---|
  //      ||   31   |   30   |   29   |   28   |   27   |   26   |   25   |    24     |
  //  4   ||-----------------------------------|----------Retained Entries------------|

  static final int LG_NOM_ENTRIES_BYTE = 16;
  static final int LG_CUR_CAPACITY_BYTE = 17;
  static final int LG_RESIZE_FACTOR_BYTE = 18;
  // 1 byte of padding for alignment
  static final int SAMPLING_P_FLOAT = 20;
  static final int RETAINED_ENTRIES_INT = 24;
  // 4 bytes of padding for alignment
  static final int ENTRIES_START = 32;

  static final int DEFAULT_LG_RESIZE_FACTOR = 3;

  /**
   * Checks that the given image holds the whole hash table it declares, and that its counts are valid,
   * before anything is allocated or read from it. The serialized form always includes the whole table.
   * @param seg the serialized QuickSelect sketch
   * @throws SketchesArgumentException if the image is too small or its counts are invalid
   */
  static void checkQuickSelectImage(final MemorySegment seg) {
    checkBounds(0, ENTRIES_START, seg.byteSize());
    final int numValues = seg.get(JAVA_BYTE, NUM_VALUES_BYTE);
    final int lgCurCapacity = seg.get(JAVA_BYTE, LG_CUR_CAPACITY_BYTE);
    final int lgNomEntries = seg.get(JAVA_BYTE, LG_NOM_ENTRIES_BYTE);
    //the table grows to at most twice the nominal entries, but is never smaller than the minimum table size
    if ((numValues < 1) || (lgNomEntries < 0) || (lgNomEntries > ThetaUtil.MAX_LG_NOM_LONGS) || (lgCurCapacity < 0)
        || (lgCurCapacity > Math.max(lgNomEntries + 1, ThetaUtil.MIN_LG_ARR_LONGS))) {
      throw new SketchesArgumentException("Possible corruption: numValues: " + numValues
          + ", lgNomEntries: " + lgNomEntries + ", lgCurrentCapacity: " + lgCurCapacity);
    }
    final long capacity = 1L << lgCurCapacity;
    final int count = seg.get(JAVA_INT_UNALIGNED, RETAINED_ENTRIES_INT);
    if ((count < 0) || (count > capacity)) {
      throw new SketchesArgumentException("Possible corruption: invalid retained entries: " + count);
    }
    checkBounds(ENTRIES_START, capacity * (SIZE_OF_KEY_BYTES + ((long) SIZE_OF_VALUE_BYTES * numValues)),
        seg.byteSize());
  }

  // these can be derived from other things, but are kept here for performance
  int rebuildThreshold_; //absolute value relative to current capacity
  int lgCurrentCapacity_;

  ArrayOfDoublesQuickSelectSketch(final int numValues, final long seed) {
    super(numValues, seed);
  }

  abstract void updateValues(int index, double[] values);

  abstract void setNotEmpty();

  abstract boolean isInSamplingMode();

  abstract void rebuild(int newCapacity);

  abstract long getKey(int index);

  abstract void setValues(int index, double[] values);

  abstract void incrementCount();

  abstract void setThetaLong(long thetaLong);

  abstract int insertKey(long key);

  abstract int findOrInsertKey(long key);

  abstract double[] find(long key);

  abstract int getSerializedSizeBytes();

  abstract void serializeInto(MemorySegment seg);

  @Override
  public void trim() {
    if (getRetainedEntries() > getNominalEntries()) {
      setThetaLong(getNewThetaLong());
      rebuild();
    }
  }

  @Override
  public int getMaxBytes() {
    final int nomEntries = getNominalEntries();
    final int numValues = getNumValues();
    return getMaxBytes(nomEntries, numValues);
  }

  @Override
  public int getCurrentBytes() {
    return getSerializedSizeBytes();
  }

  /**
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than or equal to
   * given value.
   * @param numValues Number of double values to keep for each key
   * @return maximum required storage bytes given nomEntries and numValues
   */
  static int getMaxBytes(final int nomEntries, final int numValues) {
    return ENTRIES_START
        + (SIZE_OF_KEY_BYTES + SIZE_OF_VALUE_BYTES * numValues) * ceilingPowerOf2(nomEntries) * 2;
  }

  // non-public methods below

  // this is a special back door insert for merging
  // not sufficient by itself without keeping track of theta of another sketch
  void merge(final long key, final double[] values) {
    setNotEmpty();
    if (key < thetaLong_) {
      final int index = findOrInsertKey(key);
      if (index < 0) {
        incrementCount();
        setValues(~index, values);
      } else {
        updateValues(index, values);
      }
      rebuildIfNeeded();
    }
  }

  void rebuildIfNeeded() {
    if (getRetainedEntries() <= rebuildThreshold_) { return; }
    if (getCurrentCapacity() > getNominalEntries()) {
      setThetaLong(getNewThetaLong());
      rebuild();
    } else {
      rebuild(getCurrentCapacity() * getResizeFactor().getValue());
    }
  }

  void rebuild() {
    rebuild(getCurrentCapacity());
  }

  void insert(final long key, final double[] values) {
    final int index = insertKey(key);
    setValues(index, values);
    incrementCount();
  }

  final void setRebuildThreshold() {
    if (getCurrentCapacity() > getNominalEntries()) {
      rebuildThreshold_ = (int) (getCurrentCapacity() * ThetaUtil.REBUILD_THRESHOLD);
    } else {
      rebuildThreshold_ = (int) (getCurrentCapacity() * ThetaUtil.RESIZE_THRESHOLD);
    }
  }

  @Override
  void insertOrIgnore(final long key, final double[] values) {
    if (values.length != getNumValues()) {
      throw new SketchesArgumentException("input array of values must have " + getNumValues()
        + " elements, but has " + values.length);
    }
    setNotEmpty();
    if ((key == 0) || (key >= thetaLong_)) { return; }
    final int index = findOrInsertKey(key);
    if (index < 0) {
      incrementCount();
      setValues(~index, values);
    } else {
      updateValues(index, values);
    }
    rebuildIfNeeded();
  }

  long getNewThetaLong() {
    final long[] keys = new long[getRetainedEntries()];
    int i = 0;
    for (int j = 0; j < getCurrentCapacity(); j++) {
      final long key = getKey(j);
      if (key != 0) { keys[i++] = key; }
    }
    return QuickSelect.select(keys, 0, getRetainedEntries() - 1, getNominalEntries());
  }

}
