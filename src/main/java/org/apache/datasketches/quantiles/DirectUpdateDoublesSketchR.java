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

package org.apache.datasketches.quantiles;

import static java.lang.foreign.ValueLayout.JAVA_DOUBLE_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static org.apache.datasketches.quantiles.PreambleUtil.COMBINED_BUFFER;
import static org.apache.datasketches.quantiles.PreambleUtil.COMPACT_FLAG_MASK;
import static org.apache.datasketches.quantiles.PreambleUtil.EMPTY_FLAG_MASK;
import static org.apache.datasketches.quantiles.PreambleUtil.MAX_DOUBLE;
import static org.apache.datasketches.quantiles.PreambleUtil.MIN_DOUBLE;
import static org.apache.datasketches.quantiles.PreambleUtil.N_LONG;
import static org.apache.datasketches.quantiles.PreambleUtil.ORDERED_FLAG_MASK;
import static org.apache.datasketches.quantiles.PreambleUtil.READ_ONLY_FLAG_MASK;
import static org.apache.datasketches.quantiles.PreambleUtil.extractFamilyID;
import static org.apache.datasketches.quantiles.PreambleUtil.extractFlags;
import static org.apache.datasketches.quantiles.PreambleUtil.extractK;
import static org.apache.datasketches.quantiles.PreambleUtil.extractN;
import static org.apache.datasketches.quantiles.PreambleUtil.extractPreLongs;
import static org.apache.datasketches.quantiles.PreambleUtil.extractSerVer;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.MemorySegmentStatus;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesReadOnlyException;
import org.apache.datasketches.quantilescommon.QuantilesAPI;

/**
 * Implements the DoublesSketch off-heap.
 *
 * @author Kevin Lang
 * @author Lee Rhodes
 *
 */
class DirectUpdateDoublesSketchR extends UpdateDoublesSketch {
  static final int MIN_DIRECT_DOUBLES_SER_VER = 3;
  MemorySegment seg_;

  //**CONSTRUCTORS**********************************************************
  DirectUpdateDoublesSketchR(final int k, final MemorySegment seg) {
    super(k); //Checks k
    seg_ = seg;
  }

  /**
   * Wrap this sketch around the given non-compact MemorySegment image of a DoublesSketch.
   *
   * @param srcSeg the given non-compact MemorySegment image of a DoublesSketch that may have data
   * @return a sketch that wraps the given srcSeg
   */
  static DirectUpdateDoublesSketchR wrapInstance(final MemorySegment srcSeg) {
    final long segCap = srcSeg.byteSize();

    final int preLongs = extractPreLongs(srcSeg);
    final int serVer = extractSerVer(srcSeg);
    final int familyID = extractFamilyID(srcSeg);
    final int flags = extractFlags(srcSeg);
    final int k = extractK(srcSeg);

    final boolean empty = (flags & EMPTY_FLAG_MASK) > 0; //Preamble flags empty state
    final long n = empty ? 0 : extractN(srcSeg);

    //VALIDITY CHECKS
    checkPreLongs(preLongs);
    ClassicUtil.checkFamilyID(familyID);
    DoublesUtil.checkDoublesSerVer(serVer, MIN_DIRECT_DOUBLES_SER_VER);
    checkDirectFlags(flags); //Cannot be compact
    ClassicUtil.checkK(k);
    checkCompact(serVer, flags);
    checkDirectSegCapacity(k, n, segCap);
    checkEmptyAndN(empty, n);

    return new DirectUpdateDoublesSketchR(k, srcSeg);
  }

  @Override
  public double getMaxItem() {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    return seg_.get(JAVA_DOUBLE_UNALIGNED, MAX_DOUBLE);
  }

  @Override
  public double getMinItem() {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    return seg_.get(JAVA_DOUBLE_UNALIGNED, MIN_DOUBLE);
  }

  @Override
  public long getN() {
    return (seg_.byteSize() < COMBINED_BUFFER) ? 0 : seg_.get(JAVA_LONG_UNALIGNED, N_LONG);
  }

  @Override
  public boolean hasMemorySegment() {
    return (seg_ != null);
  }

  @Override
  public boolean isOffHeap() {
    return (seg_ != null) ? seg_.isNative() : false;
  }

  @Override
  public boolean isReadOnly() {
    return true;
  }

  @Override
  public boolean isSameResource(final MemorySegment that) {
    return MemorySegmentStatus.isSameResource(seg_, that);
  }

  @Override
  public void reset() {
    throw new SketchesReadOnlyException("Call to reset() on read-only buffer");
  }

  @Override
  public void update(final double dataItem) {
    throw new SketchesReadOnlyException("Call to update() on read-only buffer");
  }

  //Restricted overrides
  //Gets

  @Override
  int getBaseBufferCount() {
    return ClassicUtil.computeBaseBufferItems(getK(), getN());
  }

  @Override
  int getCombinedBufferItemCapacity() {
    return Math.max(0, (int)seg_.byteSize() - COMBINED_BUFFER) / 8;
  }

  @Override
  double[] getCombinedBuffer() {
    final int k = getK();
    if (isEmpty()) { return new double[k << 1]; } //2K
    final long n = getN();
    final int itemCap = ClassicUtil.computeCombinedBufferItemCapacity(k, n);
    final double[] combinedBuffer = new double[itemCap];
    MemorySegment.copy(seg_, JAVA_DOUBLE_UNALIGNED, COMBINED_BUFFER, combinedBuffer, 0, itemCap);

    return combinedBuffer;
  }

  @Override
  long getBitPattern() {
    final int k = getK();
    final long n = getN();
    return ClassicUtil.computeBitPattern(k, n);
  }

  @Override
  MemorySegment getMemorySegment() {
    return seg_;
  }

  @Override
  UpdateDoublesSketch getSketchAndReset() {
    throw new SketchesReadOnlyException("Call to getResultAndReset() on read-only sketch");
  }

  //Puts

  @Override
  void putMinItem(final double minQuantile) {
    throw new SketchesReadOnlyException("Call to putMinQuantile() on read-only sketch");
  }

  @Override
  void putMaxItem(final double maxQuantile) {
    throw new SketchesReadOnlyException("Call to putMaxQuantile() on read-only sketch");
  }

  @Override
  void putN(final long n) {
    throw new SketchesReadOnlyException("Call to putN() on read-only sketch");
  }

  @Override
  void putCombinedBuffer(final double[] combinedBuffer) {
    throw new SketchesReadOnlyException("Call to putCombinedBuffer() on read-only sketch");
  }

  @Override
  void putBaseBufferCount(final int baseBufferCount) {
    throw new SketchesReadOnlyException("Call to putBaseBufferCount() on read-only sketch");
  }

  @Override
  void putBitPattern(final long bitPattern) {
    throw new SketchesReadOnlyException("Call to putBaseBufferCount() on read-only sketch");
  }

  @Override
  double[] growCombinedBuffer(final int curCombBufItemCap, final int itemSpaceNeeded) {
    throw new SketchesReadOnlyException("Call to growCombinedBuffer() on read-only sketch");
  }

  //Checks

  /**
   * Checks the validity of the direct MemorySegment capacity assuming n, k.
   * @param k the given k
   * @param n the given n
   * @param segCapBytes the current MemorySegment capacity in bytes
   */
  static void checkDirectSegCapacity(final int k, final long n, final long segCapBytes) {
    final int reqBufBytes = getUpdatableStorageBytes(k, n);

    if (segCapBytes < reqBufBytes) {
      throw new SketchesArgumentException("Possible corruption: MemorySegment capacity too small: "
          + segCapBytes + " < " + reqBufBytes);
    }
  }

  static void checkCompact(final int serVer, final int flags) {
    final boolean compact = (serVer == 2) || ((flags & COMPACT_FLAG_MASK) > 0);
    if (compact) {
      throw new SketchesArgumentException("Compact MemorySegment is not supported for Wrap Instance.");
    }
  }

  static void checkPreLongs(final int preLongs) {
    if ((preLongs < 1) || (preLongs > 2)) {
      throw new SketchesArgumentException(
          "Possible corruption: PreLongs must be 1 or 2: " + preLongs);
    }
  }

  static void checkDirectFlags(final int flags) {
    final int allowedFlags = //Cannot be compact!
        READ_ONLY_FLAG_MASK | EMPTY_FLAG_MASK | ORDERED_FLAG_MASK;
    final int flagsMask = ~allowedFlags;
    if ((flags & flagsMask) > 0) {
      throw new SketchesArgumentException(
         "Possible corruption: Invalid flags field: Cannot be compact! "
             + Integer.toBinaryString(flags));
    }
  }

  static void checkEmptyAndN(final boolean empty, final long n) {
    if (empty && (n > 0)) {
      throw new SketchesArgumentException(
          "Possible corruption: Empty Flag = true and N > 0: " + n);
    }
  }

}
