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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_DOUBLE_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static org.apache.datasketches.quantiles.ClassicUtil.DOUBLES_SER_VER;
import static org.apache.datasketches.quantiles.ClassicUtil.checkFamilyID;
import static org.apache.datasketches.quantiles.ClassicUtil.checkK;
import static org.apache.datasketches.quantiles.ClassicUtil.computeBitPattern;
import static org.apache.datasketches.quantiles.DoublesUtil.checkDoublesSerVer;
import static org.apache.datasketches.quantiles.PreambleUtil.COMBINED_BUFFER;
import static org.apache.datasketches.quantiles.PreambleUtil.COMPACT_FLAG_MASK;
import static org.apache.datasketches.quantiles.PreambleUtil.EMPTY_FLAG_MASK;
import static org.apache.datasketches.quantiles.PreambleUtil.FLAGS_BYTE;
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
import static org.apache.datasketches.quantiles.PreambleUtil.insertFamilyID;
import static org.apache.datasketches.quantiles.PreambleUtil.insertFlags;
import static org.apache.datasketches.quantiles.PreambleUtil.insertK;
import static org.apache.datasketches.quantiles.PreambleUtil.insertMaxDouble;
import static org.apache.datasketches.quantiles.PreambleUtil.insertMinDouble;
import static org.apache.datasketches.quantiles.PreambleUtil.insertN;
import static org.apache.datasketches.quantiles.PreambleUtil.insertPreLongs;
import static org.apache.datasketches.quantiles.PreambleUtil.insertSerVer;

import java.lang.foreign.MemorySegment;
import java.util.Objects;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.MemorySegmentRequest;
import org.apache.datasketches.common.MemorySegmentStatus;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesReadOnlyException;
import org.apache.datasketches.quantilescommon.QuantilesAPI;

/**
 * Implements the DoublesSketch in a MemorySegment.
 *
 * @author Kevin Lang
 * @author Lee Rhodes
 */
final class DirectUpdateDoublesSketch extends UpdateDoublesSketch {
  private static final int MIN_DIRECT_DOUBLES_SER_VER = 3;
  private MemorySegmentRequest mSegReq_ = null;
  private MemorySegment seg_;

  //**CONSTRUCTORS**
  private DirectUpdateDoublesSketch(final int k, final MemorySegment seg, final MemorySegmentRequest mSegReq) {
    super(k); //Checks k
    mSegReq_ = mSegReq;
    seg_ = seg;
  }

  /**
   * Creates a new instance of a DoublesSketch in a MemorySegment.
   *
   * @param k Parameter that controls space usage of sketch and accuracy of estimates.
   * Must be greater than 1 and less than 65536 and a power of 2.
   * @param dstSeg the non-null destination MemorySegment that will be initialized to hold the data for this sketch.
   * It must initially be at least (16 * MIN_K + 32) bytes, where MIN_K defaults to 2. As it grows
   * it will request more MemorySegment using the MemorySegmentRequest callback.
   * @param mSegReq the MemorySegmentRequest used if the incoming MemorySegment needs to expand.
   * Otherwise, it can be null and the default MemorySegmentRequest will be used.
   * @return a DirectUpdateDoublesSketch
   */
  static DirectUpdateDoublesSketch newInstance(final int k, final MemorySegment dstSeg, final MemorySegmentRequest mSegReq) {
    Objects.requireNonNull(dstSeg, "The MemorySegment dstSeg must not be null");
    final long segCap = dstSeg.byteSize();
    checkDirectSegCapacity(k, 0, segCap);

    //initialize dstSeg
    dstSeg.set(JAVA_LONG_UNALIGNED, 0, 0L); //clear pre0
    insertPreLongs(dstSeg, 2);
    insertSerVer(dstSeg, DOUBLES_SER_VER);
    insertFamilyID(dstSeg, Family.QUANTILES.getID());
    insertFlags(dstSeg, EMPTY_FLAG_MASK);
    insertK(dstSeg, k);

    if (segCap >= COMBINED_BUFFER) {
      insertN(dstSeg, 0L);
      insertMinDouble(dstSeg, Double.NaN);
      insertMaxDouble(dstSeg, Double.NaN);
    }

    return new DirectUpdateDoublesSketch(k, dstSeg, mSegReq);
  }

  /**
   * Wrap this sketch around the given updatable MemorySegment image of a DoublesSketch.
   *
   * @param srcSeg the given MemorySegment image of an UpdateDoublesSketch and must not be null.
   * @param mSegReq the MemorySegmentRequest used if the incoming MemorySegment needs to expand.
   * Otherwise, it can be null and the default MemorySegmentRequest will be used.
   * @return a sketch that wraps the given srcSeg
   */
  static DirectUpdateDoublesSketch wrapInstance(final MemorySegment srcSeg, final MemorySegmentRequest mSegReq) {
    Objects.requireNonNull(srcSeg, "The source MemorySegment must not be null");
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
    checkFamilyID(familyID);
    checkDoublesSerVer(serVer, MIN_DIRECT_DOUBLES_SER_VER);
    checkDirectFlags(flags); //Cannot be compact
    checkK(k);
    checkCompact(serVer, flags);
    checkDirectSegCapacity(k, n, segCap);
    checkEmptyAndN(empty, n);

    return new DirectUpdateDoublesSketch(k, srcSeg, mSegReq);
  }

  //**END CONSTRUCTORS**

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
    return seg_.isReadOnly();
  }

  @Override
  public boolean isSameResource(final MemorySegment that) {
    return MemorySegmentStatus.isSameResource(seg_, that);
  }

  @Override
  public void update(final double dataItem) {
    if (Double.isNaN(dataItem)) { return; }
    if (seg_.isReadOnly()) { throw new SketchesReadOnlyException("This sketch is read only."); }
    final int curBBCount = getBaseBufferCount();
    final int newBBCount = curBBCount + 1; //derived, not stored

    //must check MemorySegment capacity before we put anything in it
    final int combBufItemCap = getCombinedBufferItemCapacity();
    if (newBBCount > combBufItemCap) { //newBBCount can never exceed 2 * k.
      //unlike the heap sketch, this will grow the base buffer immediately to full size.
      seg_ = growCombinedSegBuffer(2 * getK());
    }

    final long curN = getN();
    final long newN = curN + 1;

    if (curN == 0) { //set min and max quantiles
      putMaxItem(dataItem);
      putMinItem(dataItem);
    } else {
      if (dataItem > getMaxItem()) { putMaxItem(dataItem); }
      if (dataItem < getMinItem()) { putMinItem(dataItem); }
    }

    seg_.set(JAVA_DOUBLE_UNALIGNED, COMBINED_BUFFER + ((long) curBBCount * Double.BYTES), dataItem); //put the item
    seg_.set(JAVA_BYTE, FLAGS_BYTE, (byte) 0); //not compact, not ordered, not empty

    if (newBBCount == (2 * k_)) { //Propagate
      // make sure there will be enough levels for the propagation
      final int curSegItemCap = getCombinedBufferItemCapacity();
      final int itemSpaceNeeded = DoublesUpdateImpl.getRequiredItemCapacity(k_, newN);

      //check seg has capacity to accommodate new level
      if (itemSpaceNeeded > curSegItemCap) {
        // copies base buffer plus old levels, adds space for new level
        seg_ = growCombinedSegBuffer(itemSpaceNeeded);
      }

      // sort base buffer via accessor which modifies the underlying base buffer,
      // then use as one of the inputs to propagate-carry
      final DoublesSketchAccessor bbAccessor = DoublesSketchAccessor.wrap(this, true);
      bbAccessor.sort();

      final long newBitPattern = DoublesUpdateImpl.inPlacePropagateCarry(
              0, // starting level
              null,
              bbAccessor,
              true,
              k_,
              DoublesSketchAccessor.wrap(this, true),
              getBitPattern()
      );

      assert newBitPattern == computeBitPattern(k_, newN); // internal consistency check
      //bit pattern on direct is always derived, no need to save it.
    }
    putN(newN);
    doublesSV = null;
  }

  @Override
  public void reset() {
    if (seg_.isReadOnly()) { throw new SketchesReadOnlyException("This sketch is read only."); }
    if (seg_.byteSize() >= COMBINED_BUFFER) {
      seg_.set(JAVA_BYTE, FLAGS_BYTE, (byte) EMPTY_FLAG_MASK); //not compact, not ordered
      seg_.set(JAVA_LONG_UNALIGNED, N_LONG, 0L);
      seg_.set(JAVA_DOUBLE_UNALIGNED, MIN_DOUBLE, Double.NaN);
      seg_.set(JAVA_DOUBLE_UNALIGNED, MAX_DOUBLE, Double.NaN);
    }
  }

  //Restricted overrides

  @Override
  int getBaseBufferCount() {
    return ClassicUtil.computeBaseBufferItems(getK(), getN());
  }

  @Override
  long getBitPattern() {
    final int k = getK();
    final long n = getN();
    return ClassicUtil.computeBitPattern(k, n);
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
  int getCombinedBufferItemCapacity() {
    return Math.max(0, (int)seg_.byteSize() - COMBINED_BUFFER) / Double.BYTES;
  }

  @Override
  MemorySegment getMemorySegment() {
    return seg_;
  }

  @Override
  UpdateDoublesSketch getSketchAndReset() {
    final HeapUpdateDoublesSketch skCopy = HeapUpdateDoublesSketch.heapifyInstance(seg_);
    reset();
    return skCopy;
  }

  @Override
  double[] growCombinedBuffer(final int curCombBufItemCap, final int itemSpaceNeeded) {
    if (seg_.isReadOnly()) { throw new SketchesReadOnlyException("This sketch is read only."); }
    seg_ = growCombinedSegBuffer(itemSpaceNeeded);
    // copy out any data that was there
    final double[] newCombBuf = new double[itemSpaceNeeded];
    MemorySegment.copy(seg_, JAVA_DOUBLE_UNALIGNED, COMBINED_BUFFER, newCombBuf, 0, curCombBufItemCap);
    return newCombBuf;
  }

  //Puts

  @Override
  void putMinItem(final double minQuantile) {
    if (seg_.isReadOnly()) { throw new SketchesReadOnlyException("This sketch is read only."); }
    assert (seg_.byteSize() >= COMBINED_BUFFER);
    seg_.set(JAVA_DOUBLE_UNALIGNED, MIN_DOUBLE, minQuantile);
  }

  @Override
  void putMaxItem(final double maxQuantile) {
    if (seg_.isReadOnly()) { throw new SketchesReadOnlyException("This sketch is read only."); }
    assert (seg_.byteSize() >= COMBINED_BUFFER);
    seg_.set(JAVA_DOUBLE_UNALIGNED, MAX_DOUBLE, maxQuantile);
  }

  @Override
  void putN(final long n) {
    if (seg_.isReadOnly()) { throw new SketchesReadOnlyException("This sketch is read only."); }
    assert (seg_.byteSize() >= COMBINED_BUFFER);
    seg_.set(JAVA_LONG_UNALIGNED, N_LONG, n);
  }

  @Override
  void putCombinedBuffer(final double[] combinedBuffer) {
    if (seg_.isReadOnly()) { throw new SketchesReadOnlyException("This sketch is read only."); }
    MemorySegment.copy(combinedBuffer, 0, seg_, JAVA_DOUBLE_UNALIGNED, COMBINED_BUFFER, combinedBuffer.length);
  }

  @Override
  void putBaseBufferCount(final int baseBufferCount) {
    if (seg_.isReadOnly()) { throw new SketchesReadOnlyException("This sketch is read only."); }
  }

  @Override
  void putBitPattern(final long bitPattern) {
    if (seg_.isReadOnly()) { throw new SketchesReadOnlyException("This sketch is read only."); }
  }

  //Direct supporting methods

  private MemorySegment growCombinedSegBuffer(final int itemSpaceNeeded) {
    final long segBytes = seg_.byteSize();
    final int needBytes = (itemSpaceNeeded << 3) + COMBINED_BUFFER; //+ preamble + min & max
    assert needBytes > segBytes;

    mSegReq_ = (mSegReq_ == null) ? MemorySegmentRequest.DEFAULT : mSegReq_;

    final MemorySegment newSeg = mSegReq_.request(seg_, needBytes);
    MemorySegment.copy(seg_, 0, newSeg, 0, segBytes);
    mSegReq_.requestClose(seg_);
    return newSeg;
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
