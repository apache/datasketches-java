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

import static org.apache.datasketches.Util.REBUILD_THRESHOLD;
import static org.apache.datasketches.Util.RESIZE_THRESHOLD;
import static org.apache.datasketches.Util.ceilingPowerOf2;

import org.apache.datasketches.QuickSelect;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.WritableMemory;

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

  // these can be derived from other things, but are kept here for performance
  int rebuildThreshold_;
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

  abstract void setThetaLong(long theta);

  abstract int insertKey(long key);

  abstract int findOrInsertKey(long key);

  abstract double[] find(long key);

  abstract int getSerializedSizeBytes();

  abstract void serializeInto(WritableMemory mem);

  @Override
  public void trim() {
    if (getRetainedEntries() > getNominalEntries()) {
      setThetaLong(getNewTheta());
      rebuild();
    }
  }

  /**
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than
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
    if (key < theta_) {
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
      setThetaLong(getNewTheta());
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

  void setRebuildThreshold() {
    if (getCurrentCapacity() > getNominalEntries()) {
      rebuildThreshold_ = (int) (getCurrentCapacity() * REBUILD_THRESHOLD);
    } else {
      rebuildThreshold_ = (int) (getCurrentCapacity() * RESIZE_THRESHOLD);
    }
  }

  @Override
  void insertOrIgnore(final long key, final double[] values) {
    if (values.length != getNumValues()) {
      throw new SketchesArgumentException("input array of values must have " + getNumValues()
        + " elements, but has " + values.length);
    }
    setNotEmpty();
    if ((key == 0) || (key >= theta_)) { return; }
    final int index = findOrInsertKey(key);
    if (index < 0) {
      incrementCount();
      setValues(~index, values);
    } else {
      updateValues(index, values);
    }
    rebuildIfNeeded();
  }

  long getNewTheta() {
    final long[] keys = new long[getRetainedEntries()];
    int i = 0;
    for (int j = 0; j < getCurrentCapacity(); j++) {
      final long key = getKey(j);
      if (key != 0) { keys[i++] = key; }
    }
    return QuickSelect.select(keys, 0, getRetainedEntries() - 1, getNominalEntries());
  }

}
