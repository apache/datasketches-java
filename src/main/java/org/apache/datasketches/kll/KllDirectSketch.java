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

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.kll.KllPreambleUtil.Layout;
import org.apache.datasketches.kll.KllPreambleUtil.MemoryCheck;
import org.apache.datasketches.kll.KllPreambleUtil.SketchType;
import org.apache.datasketches.memory.WritableMemory;

abstract class KllDirectSketch extends KllSketch {
  final WritableMemory wmem;
  final Layout layout;
  final boolean compact;
  final int dataStartBytes;

  KllDirectSketch(final WritableMemory wmem, final SketchType sketchType) {
    super(sketchType);
    final MemoryCheck memChk = new MemoryCheck(wmem);
    this.wmem = wmem;
    this.layout = memChk.layout;
    this.compact = !memChk.updatable;
    this.dataStartBytes = memChk.dataStart;
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
  public int getNumRetained() {
    if (compact) {
      final int itemCapacity = KllHelper.computeTotalItemCapacity(getK(), M, getNumLevels());
      return itemCapacity - getLevelsArrayAt(0);
    }
    return getLevelsArrayAt(getLevelsArrayAt(getNumLevels()) - getLevelsArrayAt(0) );
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

  @Override
  int[] getLevelsArray() {
    final int lengthInts = getLevelsArrLengthInts();
    final int[] levelsArr = new int[lengthInts];
    wmem.getIntArray(dataStartBytes, levelsArr, 0, lengthInts);
    return levelsArr;
  }

  @Override
  int getLevelsArrayAt(final int index) {
    return wmem.getInt(dataStartBytes + index * Integer.BYTES);
  }

  @Override
  int getNumLevels() {
    return extractNumLevels(wmem);
  }

  @Override
  void incN() {
    if (compact) { kllDirectSketchThrow(30); }
    long n = extractN(wmem);
    insertN(wmem, ++n);
  }

  @Override
  void incNumLevels() {
    if (compact) { kllDirectSketchThrow(30); }
    int numLevels = extractNumLevels(wmem);
    insertNumLevels(wmem, ++numLevels);
  }

  @Override
  boolean isLevelZeroSorted() {
    return extractLevelZeroSortedFlag(wmem);
  }

  @Override
  void setDyMinK(final int dyMinK) {
    if (compact) { kllDirectSketchThrow(30); }
    insertDyMinK(wmem, dyMinK);
  }

  @Override
  void setLevelsArray(final int[] levels) {
    if (compact) { kllDirectSketchThrow(30); }
    final int lengthInts = getLevelsArrLengthInts();
    wmem.putIntArray(dataStartBytes, levels, 0, lengthInts);
  }

  @Override
  void setLevelsArrayAt(final int index, final int value) {
    if (compact) { kllDirectSketchThrow(30); }
    wmem.putInt(dataStartBytes + index * Integer.BYTES, value);
  }

  @Override
  void setLevelsArrayAtMinusEq(final int index, final int minusEq) {
    if (compact) { kllDirectSketchThrow(30); }
    final int old = wmem.getInt(dataStartBytes + index * Integer.BYTES);
    wmem.putInt(dataStartBytes + index * Integer.BYTES, old - minusEq);
  }

  @Override
  void setLevelsArrayAtPlusEq(final int index, final int plusEq) {
    if (compact) { kllDirectSketchThrow(30); }
    final int old = wmem.getInt(dataStartBytes + index * Integer.BYTES);
    wmem.putInt(dataStartBytes + index * Integer.BYTES, old + plusEq);
  }

  @Override
  void setLevelZeroSorted(final boolean sorted) {
    if (compact) { kllDirectSketchThrow(30); }
    insertLevelZeroSortedFlag(wmem, sorted);
  }

  @Override
  void setN(final long n) {
    if (compact) { kllDirectSketchThrow(30); }
    insertN(wmem, n);
  }

  @Override
  void setNumLevels(final int numLevels) {
    if (compact) { kllDirectSketchThrow(30); }
    insertNumLevels(wmem, numLevels);
  }

  int getItemsDataStartBytes() {
    return dataStartBytes + getLevelsArrLengthInts() * Integer.BYTES;
  }

  int getItemsArrLengthItems() {
    if (compact) { return getNumRetained(); }
    return getLevelsArrayAt(getNumLevels());
  }

  int getLevelsArrLengthInts() {
    final int lengthInts;

    switch (layout) {
      case FLOAT_EMPTY_COMPACT:
      case FLOAT_SINGLE_COMPACT:
      case DOUBLE_EMPTY_COMPACT:
      case DOUBLE_SINGLE_COMPACT: { return 0; }

      case FLOAT_FULL_COMPACT: { lengthInts = getNumLevels(); break; }
      case DOUBLE_FULL_COMPACT: { lengthInts = getNumLevels(); break; }
      case FLOAT_UPDATABLE: { lengthInts = getNumLevels() + 1; break; }
      case DOUBLE_UPDATABLE: { lengthInts = getNumLevels() + 1; break; }
      default: return 0;
    }
    return lengthInts;
  }


  private static void kllDirectSketchThrow(final int errNo) {
    String msg = "";
    switch (errNo) {
      case 30: msg = "Sketch Memory is immutable, cannot write."; break;
    }
    throw new SketchesArgumentException(msg);
  }
}
