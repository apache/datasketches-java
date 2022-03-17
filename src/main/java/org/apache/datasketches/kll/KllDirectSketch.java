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

package org.apache.datasketches.kll;

import static org.apache.datasketches.kll.KllPreambleUtil.extractDyMinK;
import static org.apache.datasketches.kll.KllPreambleUtil.extractK;
import static org.apache.datasketches.kll.KllPreambleUtil.extractLevelZeroSortedFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.extractN;
import static org.apache.datasketches.kll.KllPreambleUtil.extractNumLevels;
import static org.apache.datasketches.kll.KllPreambleUtil.insertDyMinK;
import static org.apache.datasketches.kll.KllPreambleUtil.insertLevelZeroSortedFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.insertN;
import static org.apache.datasketches.kll.KllPreambleUtil.insertNumLevels;
//import static org.apache.datasketches.kll.KllPreambleUtil.SketchType.DOUBLE_SKETCH;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.kll.KllPreambleUtil.Layout;
import org.apache.datasketches.kll.KllPreambleUtil.SketchType;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.memory.DefaultMemoryRequestServer;

abstract class KllDirectSketch extends KllSketch {
  //All these members are constant for the life of this object. If the WritableMemory changes, it will require
  //rebuilding this class
  final WritableMemory wmem;
  final Layout layout;
  final boolean updatable;
  final int numLevels_;
  final int memItemsCap;
  final int sketchBytes;
  final WritableMemory levelsWmem;
  final WritableMemory minMaxWmem;
  final WritableMemory itemsWmem;
  DefaultMemoryRequestServer defaultMemReqSvr = null;


  /**
   * For the direct sketches it is important that the methods implemented here are designed to work dynamically
   * as the sketch grows off-heap.
   * @param wmem the current WritableMemory
   * @param sketchType either DOUBLE_SKETCH or FLOAT_SKETCH
   */
  KllDirectSketch(final WritableMemory wmem, final SketchType sketchType) {
    super(sketchType);
    final MemoryValidate memVal = new MemoryValidate(wmem);
    this.wmem = wmem;
    layout = memVal.layout;
    updatable = memVal.updatable;
    numLevels_ = memVal.numLevels;
    memItemsCap = memVal.memItemsCap;
    sketchBytes = memVal.sketchBytes;
    levelsWmem = memVal.levelsWmem;
    minMaxWmem = memVal.minMaxWmem;
    itemsWmem = memVal.itemsWmem;
    defaultMemReqSvr = updatable ? new DefaultMemoryRequestServer() : null;
  }

  private static void kllDirectSketchThrow(final int errNo) {
    String msg = "";
    switch (errNo) {
      case 30: msg = "Sketch Memory is immutable, cannot write."; break;
    }
    throw new SketchesArgumentException(msg);
  }

  @Override
  public int getK() {
    return extractK(wmem);
  }

  @Override
  public long getN() {
    return extractN(wmem);
  }

  @Override
  public boolean isUpdatable() {
    return updatable;
  }

  @Override
  public abstract byte[] toByteArray();

  @Override
  public abstract String toString(final boolean withLevels, final boolean withData);

  @Override
  public abstract byte[] toUpdatableByteArray();

  @Override
  int getDyMinK() {
    return extractDyMinK(wmem);
  }

  int getItemsArrLengthItems() {
    if (updatable) { return getLevelsArray()[getNumLevels()]; }
    return getNumRetained();
  }


  @Override
  String getLayout() { return layout.toString(); }

  @Override
  int[] getLevelsArray() {
    final int cap = getNumLevels() + 1;
    final int[] myLevelsArr = new int[cap];
    levelsWmem.getIntArray(0, myLevelsArr, 0, cap);
    return myLevelsArr;
  }

  /**
   * For determining the actual length of the array as stored in Memory
   * @return the actual length of the array as stored in Memory
   */
  int getLevelsArrLengthInts() {
    final int memLengthInts;

    switch (layout) {
      case FLOAT_EMPTY_COMPACT:
      case DOUBLE_EMPTY_COMPACT:
      case FLOAT_SINGLE_COMPACT:
      case DOUBLE_SINGLE_COMPACT: { memLengthInts = 0; break; }
      case FLOAT_FULL_COMPACT:
      case DOUBLE_FULL_COMPACT: { memLengthInts = getNumLevels(); break; }
      case FLOAT_UPDATABLE:
      case DOUBLE_UPDATABLE: { memLengthInts = getNumLevels() + 1; break; }
      default: return 0; //can't get here
    }
    return memLengthInts;
  }

  @Override
  int getNumLevels() {
    return extractNumLevels(wmem);
  }

  @Override
  void incN() {
    if (!updatable) { kllDirectSketchThrow(30); }
    long n = extractN(wmem);
    insertN(wmem, ++n);
  }

  @Override
  void incNumLevels() {
    if (!updatable) { kllDirectSketchThrow(30); }
    int numLevels = extractNumLevels(wmem);
    insertNumLevels(wmem, ++numLevels);
  }

  @Override
  boolean isLevelZeroSorted() {
    return extractLevelZeroSortedFlag(wmem);
  }

  @Override
  void setDyMinK(final int dyMinK) {
    if (!updatable) { kllDirectSketchThrow(30); }
    insertDyMinK(wmem, dyMinK);
  }

  @Override
  void updateLevelsArray(final int[] levels) {
    if (!updatable) { kllDirectSketchThrow(30); }
    levelsWmem.putIntArray(0, levels, 0, levels.length);
  }

  @Override
  void setLevelZeroSorted(final boolean sorted) {
    if (!updatable) { kllDirectSketchThrow(30); }
    insertLevelZeroSortedFlag(wmem, sorted);
  }

  @Override
  void setN(final long n) {
    if (!updatable) { kllDirectSketchThrow(30); }
    insertN(wmem, n);
  }


  @Override
  void setNumLevels(final int numLevels) {
    if (!updatable) { kllDirectSketchThrow(30); }
    insertNumLevels(wmem, numLevels);
  }
}
