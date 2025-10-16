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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_DOUBLE_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static org.apache.datasketches.common.ByteArrayUtil.copyBytes;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_SINGLE_ITEM;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemorySegmentK;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemorySegmentLevelZeroSortedFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemorySegmentM;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemorySegmentMinK;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemorySegmentN;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemorySegmentNumLevels;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemorySegmentFamilyID;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemorySegmentK;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemorySegmentLevelZeroSortedFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemorySegmentM;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemorySegmentMinK;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemorySegmentN;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemorySegmentNumLevels;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemorySegmentPreInts;
import static org.apache.datasketches.kll.KllPreambleUtil.setMemorySegmentSerVer;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_EMPTY;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_FULL;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_SINGLE;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.UPDATABLE;
import static org.apache.datasketches.kll.KllSketch.SketchType.KLL_DOUBLES_SKETCH;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.ByteArrayUtil;
import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.MemorySegmentRequest;
import org.apache.datasketches.common.MemorySegmentStatus;
import org.apache.datasketches.common.SketchesArgumentException;

/**
 * This class implements an off-heap, updatable KllDoublesSketch using MemorySegment.
 *
 * <p>Please refer to the documentation in the package-info:<br>
 * {@link org.apache.datasketches.kll}</p>
 *
 * @author Lee Rhodes, Kevin Lang
 */
class KllDirectDoublesSketch extends KllDoublesSketch {
  private MemorySegment wseg;
  private final MemorySegmentRequest mSegReq;

  /**
   * Constructs from MemorySegment or MemorySegment already initialized with a sketch image and validated.
   * @param wseg the current MemorySegment
   * @param segVal the MemorySegment Validate object
   * @param mSegmentRequest the MemorySegmentRequest object.
   */
  KllDirectDoublesSketch(
      final MemorySegment wseg,
      final KllMemorySegmentValidate segVal,
      final MemorySegmentRequest mSegmentRequest) {
    super(segVal);
    this.wseg = wseg;
    mSegReq = mSegmentRequest;
  }

  /**
   * Create a new updatable, direct instance of this sketch.
   * @param k parameter that controls size of the sketch and accuracy of estimates
   * @param m parameter that controls the minimum level width in items.
   * @param dstSeg the given destination MemorySegment object for use by the sketch
   * @param mSegReq the callback for the sketch to request a larger MemorySegment.
   * @return a new instance of this sketch
   */
  static KllDirectDoublesSketch newDirectUpdatableInstance(
      final int k,
      final int m,
      final MemorySegment dstSeg,
      final MemorySegmentRequest mSegmentRequest) {
    setMemorySegmentPreInts(dstSeg, UPDATABLE.getPreInts());
    setMemorySegmentSerVer(dstSeg, UPDATABLE.getSerVer());
    setMemorySegmentFamilyID(dstSeg, Family.KLL.getID());
    setMemorySegmentK(dstSeg, k);
    setMemorySegmentM(dstSeg, m);
    setMemorySegmentN(dstSeg, 0);
    setMemorySegmentMinK(dstSeg, k);
    setMemorySegmentNumLevels(dstSeg, 1);
    int offset = DATA_START_ADR;
    //new Levels array
    MemorySegment.copy(new int[] {k, k}, 0, dstSeg, JAVA_INT_UNALIGNED, offset, 2);
    offset += 2 * Integer.BYTES;
    //new min/max array
    MemorySegment.copy(new double[] {Double.NaN, Double.NaN}, 0, dstSeg, JAVA_DOUBLE_UNALIGNED, offset, 2);
    offset += 2 * ITEM_BYTES;
    //new empty items array
    MemorySegment.copy(new double[k], 0, dstSeg, JAVA_DOUBLE_UNALIGNED, offset, k);
    final KllMemorySegmentValidate segVal = new KllMemorySegmentValidate(dstSeg, KLL_DOUBLES_SKETCH, null);
    final MemorySegment wSeg = dstSeg;
    return new KllDirectDoublesSketch(wSeg, segVal, mSegmentRequest);
  }

  //End of constructors

  @Override
  String getItemAsString(final int index) {
    if (isEmpty()) { return "NaN"; }
    return Double.toString(getDoubleItemsArray()[index]);
  }

  @Override
  public int getK() {
    return getMemorySegmentK(wseg);
  }

  //MinMax Methods

  @Override
  public double getMaxItem() {
    if ((sketchStructure == COMPACT_EMPTY) || isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    if (sketchStructure == COMPACT_SINGLE) { return getDoubleSingleItem(); }
    //either compact-full or updatable
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + ITEM_BYTES;
    return wseg.get(JAVA_DOUBLE_UNALIGNED, offset);
  }

  @Override
  double getMaxItemInternal() {
    if ((sketchStructure == COMPACT_EMPTY) || isEmpty()) { return Double.NaN; }
    if (sketchStructure == COMPACT_SINGLE) { return getDoubleSingleItem(); }
    //either compact-full or updatable
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + ITEM_BYTES;
    return wseg.get(JAVA_DOUBLE_UNALIGNED, offset);
  }

  @Override
  String getMaxItemAsString() {
    final double maxItem = getMaxItemInternal();
    return Double.toString(maxItem);
  }

  @Override
  public double getMinItem() {
    if ((sketchStructure == COMPACT_EMPTY) || isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    if (sketchStructure == COMPACT_SINGLE) { return getDoubleSingleItem(); }
    //either compact-full or updatable
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure);
    return wseg.get(JAVA_DOUBLE_UNALIGNED, offset);
  }

  @Override
  double getMinItemInternal() {
    if ((sketchStructure == COMPACT_EMPTY) || isEmpty()) { return Double.NaN; }
    if (sketchStructure == COMPACT_SINGLE) { return getDoubleSingleItem(); }
    //either compact-full or updatable
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure);
    return wseg.get(JAVA_DOUBLE_UNALIGNED, offset);
  }

  @Override
  String getMinItemAsString() {
    final double minItem = getMinItemInternal();
    return Double.toString(minItem);
  }

  @Override
  void setMaxItem(final double item) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + ITEM_BYTES;
    wseg.set(JAVA_DOUBLE_UNALIGNED, offset, item);
  }

  @Override
  void setMinItem(final double item) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure);
    wseg.set(JAVA_DOUBLE_UNALIGNED, offset, item);
  }

  //END MinMax Methods

  @Override
  public long getN() {
    if (sketchStructure == COMPACT_EMPTY) { return 0; }
    else if (sketchStructure == COMPACT_SINGLE) { return 1; }
    else { return getMemorySegmentN(wseg); }
  }

  //other restricted

  @Override //returns updatable, expanded array including free space at bottom
  double[] getDoubleItemsArray() {
    final int k = getK();
    if (sketchStructure == COMPACT_EMPTY) { return new double[k]; }
    if (sketchStructure == COMPACT_SINGLE) {
      final double[] itemsArr = new double[k];
      itemsArr[k - 1] = getDoubleSingleItem();
      return itemsArr;
    }
    final int capacityItems = KllHelper.computeTotalItemCapacity(k, getM(), getNumLevels());
    final double[] doubleItemsArr = new double[capacityItems];
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + (2 * ITEM_BYTES);
    final int shift = (sketchStructure == COMPACT_FULL) ? levelsArr[0] : 0;
    final int numItems = (sketchStructure == COMPACT_FULL) ? getNumRetained() : capacityItems;
    MemorySegment.copy(wseg, JAVA_DOUBLE_UNALIGNED, offset, doubleItemsArr, shift, numItems);
    return doubleItemsArr;
  }

  @Override //returns compact items array of retained items, no free space.
  double[] getDoubleRetainedItemsArray() {
    if (sketchStructure == COMPACT_EMPTY) { return new double[0]; }
    if (sketchStructure == COMPACT_SINGLE) { return new double[] { getDoubleSingleItem() }; }
    final int numRetained = getNumRetained();
    final double[] doubleItemsArr = new double[numRetained];
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + (2 * ITEM_BYTES)
        + (sketchStructure == COMPACT_FULL ? 0 : levelsArr[0] * ITEM_BYTES);
    MemorySegment.copy(wseg, JAVA_DOUBLE_UNALIGNED, offset, doubleItemsArr, 0, numRetained);
    return doubleItemsArr;
  }

  @Override
  double getDoubleSingleItem() {
    if (!isSingleItem()) { throw new SketchesArgumentException(NOT_SINGLE_ITEM_MSG); }
    if (sketchStructure == COMPACT_SINGLE) {
      return wseg.get(JAVA_DOUBLE_UNALIGNED, DATA_START_ADR_SINGLE_ITEM);
    }
    final int offset;
    if (sketchStructure == COMPACT_FULL) {
      offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + (2 * ITEM_BYTES);
    } else { //sketchStructure == UPDATABLE
      offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + (((2 + getK()) - 1) * ITEM_BYTES);
    }
    return wseg.get(JAVA_DOUBLE_UNALIGNED, offset);
  }

  @Override
  int getM() {
    return getMemorySegmentM(wseg);
  }

  @Override
  MemorySegmentRequest getMemorySegmentRequest() {
    return mSegReq;
  }

  @Override
  int getMinK() {
    if ((sketchStructure == COMPACT_FULL) || (sketchStructure == UPDATABLE)) { return getMemorySegmentMinK(wseg); }
    return getK();
  }

  @Override
  byte[] getMinMaxByteArr() {
    final byte[] bytesOut = new byte[2 * ITEM_BYTES];
    if (sketchStructure == COMPACT_EMPTY) {
      ByteArrayUtil.putDoubleLE(bytesOut, 0, Double.NaN);
      ByteArrayUtil.putDoubleLE(bytesOut, ITEM_BYTES, Double.NaN);
      return bytesOut;
    }
    final int offset;
    if (sketchStructure == COMPACT_SINGLE) {
      offset = DATA_START_ADR_SINGLE_ITEM;
      MemorySegment.copy(wseg, JAVA_BYTE, offset, bytesOut, 0, ITEM_BYTES);
      copyBytes(bytesOut, 0, bytesOut, ITEM_BYTES, ITEM_BYTES);
      return bytesOut;
    }
    //sketchStructure == UPDATABLE OR COMPACT_FULL
    offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure);
    MemorySegment.copy(wseg, JAVA_BYTE, offset, bytesOut, 0, ITEM_BYTES);
    MemorySegment.copy(wseg, JAVA_BYTE, offset + ITEM_BYTES, bytesOut, ITEM_BYTES, ITEM_BYTES);
    return bytesOut;
  }

  @Override
  byte[] getRetainedItemsByteArr() {
    if (sketchStructure == COMPACT_EMPTY) { return new byte[0]; }
    final double[] dblArr = getDoubleRetainedItemsArray();
    final byte[] dblByteArr = new byte[dblArr.length * ITEM_BYTES];
    final MemorySegment wseg2 = MemorySegment.ofArray(dblByteArr);
    MemorySegment.copy(dblArr, 0, wseg2, JAVA_DOUBLE_UNALIGNED, 0, dblArr.length);
    return dblByteArr;
  }

  @Override
  byte[] getTotalItemsByteArr() {
    final double[] dblArr = getDoubleItemsArray();
    final byte[] dblByteArr = new byte[dblArr.length * ITEM_BYTES];
    final MemorySegment wseg2 = MemorySegment.ofArray(dblByteArr);
    MemorySegment.copy(dblArr, 0, wseg2, JAVA_DOUBLE_UNALIGNED, 0, dblArr.length);
    return dblByteArr;
  }

  @Override
  MemorySegment getMemorySegment() {
    return wseg;
  }

  @Override
  void incN(final int increment) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    setMemorySegmentN(wseg, getMemorySegmentN(wseg) + increment);
  }

  @Override
  void incNumLevels() {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    int numLevels = getMemorySegmentNumLevels(wseg);
    setMemorySegmentNumLevels(wseg, ++numLevels);
  }

  @Override
  boolean isLevelZeroSorted() {
    return getMemorySegmentLevelZeroSortedFlag(wseg);
  }

  @Override
  void setDoubleItemsArray(final double[] doubleItems) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + (2 * ITEM_BYTES);
    MemorySegment.copy(doubleItems, 0, wseg, JAVA_DOUBLE_UNALIGNED, offset, doubleItems.length);
  }

  @Override
  void setDoubleItemsArrayAt(final int index, final double item) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    final int offset =
        DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + ((index + 2) * ITEM_BYTES);
    wseg.set(JAVA_DOUBLE_UNALIGNED, offset, item);
  }

  @Override
  void setDoubleItemsArrayAt(final int index, final double[] items, final int srcOffset, final int length) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    final int offset = DATA_START_ADR + getLevelsArrSizeBytes(sketchStructure) + ((index + 2) * ITEM_BYTES);
    MemorySegment.copy(items, srcOffset, wseg, JAVA_DOUBLE_UNALIGNED, offset, length);
  }

  @Override
  void setLevelZeroSorted(final boolean sorted) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    setMemorySegmentLevelZeroSortedFlag(wseg, sorted);
  }

  @Override
  void setMinK(final int minK) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    setMemorySegmentMinK(wseg, minK);
  }

  @Override
  void setN(final long n) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    setMemorySegmentN(wseg, n);
  }

  @Override
  void setNumLevels(final int numLevels) {
    if (readOnly) { throw new SketchesArgumentException(TGT_IS_READ_ONLY_MSG); }
    setMemorySegmentNumLevels(wseg, numLevels);
  }

  @Override
  public boolean hasMemorySegment() {
    return (wseg != null) && wseg.scope().isAlive();
  }

  @Override
  public boolean isOffHeap() {
    return hasMemorySegment() && wseg.isNative();
  }

  @Override
  public boolean isSameResource(final MemorySegment that) {
    return MemorySegmentStatus.isSameResource(wseg, that);
  }

  @Override
  void setMemorySegment(final MemorySegment wseg) {
    this.wseg = wseg;
  }

}
