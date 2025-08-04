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
import static org.apache.datasketches.quantiles.PreambleUtil.COMBINED_BUFFER;
import static org.apache.datasketches.quantiles.PreambleUtil.EMPTY_FLAG_MASK;
import static org.apache.datasketches.quantiles.PreambleUtil.FLAGS_BYTE;
import static org.apache.datasketches.quantiles.PreambleUtil.MAX_DOUBLE;
import static org.apache.datasketches.quantiles.PreambleUtil.MIN_DOUBLE;
import static org.apache.datasketches.quantiles.PreambleUtil.N_LONG;
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

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.MemorySegmentRequest;

/**
 * Implements the DoublesSketch off-heap.
 *
 * @author Kevin Lang
 * @author Lee Rhodes
 *
 */
final class DirectUpdateDoublesSketch extends DirectUpdateDoublesSketchR {
  private MemorySegmentRequest mSegReq = null;

  //**CONSTRUCTORS**
  private DirectUpdateDoublesSketch(final int k, final MemorySegment seg, final MemorySegmentRequest mSegReq) {
    super(k, seg); //Checks k
    this.mSegReq = mSegReq;
  }

  /**
   * Creates a new Direct instance of a DoublesSketch, which may be off-heap.
   *
   * @param k Parameter that controls space usage of sketch and accuracy of estimates.
   * Must be greater than 1 and less than 65536 and a power of 2.
   * @param dstSeg the destination MemorySegment that will be initialized to hold the data for this sketch.
   * It must initially be at least (16 * MIN_K + 32) bytes, where MIN_K defaults to 2. As it grows
   * it will request more MemorySegment using the MemorySegmentRequest callback.
   * @return a DirectUpdateDoublesSketch
   */
  static DirectUpdateDoublesSketch newInstance(final int k, final MemorySegment dstSeg, final MemorySegmentRequest mSegReq) {
    // must be able to hold at least an empty sketch
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

  static DirectUpdateDoublesSketch(final DirectUpdateDoublesSketch skIn) {
    return null;
  }

  /**
   * Wrap this sketch around the given non-compact MemorySegment image of a DoublesSketch.
   *
   * @param srcSeg the given non-compact MemorySegment image of a DoublesSketch that may have data
   * @return a sketch that wraps the given srcSeg
   */
  static DirectUpdateDoublesSketch wrapInstance(final MemorySegment srcSeg, final MemorySegmentRequest mSegReq) {
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
    DoublesUtil.checkDoublesSerVer(serVer, MIN_DIRECT_DOUBLES_SER_VER);
    checkDirectFlags(flags); //Cannot be compact
    checkK(k);
    checkCompact(serVer, flags);
    checkDirectSegCapacity(k, n, segCap);
    checkEmptyAndN(empty, n);

    return new DirectUpdateDoublesSketch(k, srcSeg, mSegReq);
  }

  //**END CONSTRUCTORS**

  @Override
  public boolean isReadOnly() {
    return false;
  }

  @Override
  public void update(final double dataItem) {
    if (Double.isNaN(dataItem)) { return; }

    final int curBBCount = getBaseBufferCount();
    final int newBBCount = curBBCount + 1; //derived, not stored

    //must check MemorySegment capacity before we put anything in it
    final int combBufItemCap = getCombinedBufferItemCapacity();
    if (newBBCount > combBufItemCap) {
      //only changes combinedBuffer when it is only a base buffer
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
    if (seg_.byteSize() >= COMBINED_BUFFER) {
      seg_.set(JAVA_BYTE, FLAGS_BYTE, (byte) EMPTY_FLAG_MASK); //not compact, not ordered
      seg_.set(JAVA_LONG_UNALIGNED, N_LONG, 0L);
      seg_.set(JAVA_DOUBLE_UNALIGNED, MIN_DOUBLE, Double.NaN);
      seg_.set(JAVA_DOUBLE_UNALIGNED, MAX_DOUBLE, Double.NaN);
    }
  }

  //Restricted overrides
  //Puts

  @Override
  void putMinItem(final double minQuantile) {
    assert (seg_.byteSize() >= COMBINED_BUFFER);
    seg_.set(JAVA_DOUBLE_UNALIGNED, MIN_DOUBLE, minQuantile);
  }

  @Override
  void putMaxItem(final double maxQuantile) {
    assert (seg_.byteSize() >= COMBINED_BUFFER);
    seg_.set(JAVA_DOUBLE_UNALIGNED, MAX_DOUBLE, maxQuantile);
  }

  @Override
  void putN(final long n) {
    assert (seg_.byteSize() >= COMBINED_BUFFER);
    seg_.set(JAVA_LONG_UNALIGNED, N_LONG, n);
  }

  @Override
  void putCombinedBuffer(final double[] combinedBuffer) {
    MemorySegment.copy(combinedBuffer, 0, seg_, JAVA_DOUBLE_UNALIGNED, COMBINED_BUFFER, combinedBuffer.length);
  }

  @Override
  void putBaseBufferCount(final int baseBufferCount) {
    //intentionally a no-op, not kept on-heap, always derived.
  }

  @Override
  void putBitPattern(final long bitPattern) {
    //intentionally a no-op, not kept on-heap, always derived.
  }

  @Override
  double[] growCombinedBuffer(final int curCombBufItemCap, final int itemSpaceNeeded) {
    seg_ = growCombinedSegBuffer(itemSpaceNeeded);
    // copy out any data that was there
    final double[] newCombBuf = new double[itemSpaceNeeded];
    MemorySegment.copy(seg_, JAVA_DOUBLE_UNALIGNED, COMBINED_BUFFER, newCombBuf, 0, curCombBufItemCap);
    return newCombBuf;
  }

  //Direct supporting methods

  private MemorySegment growCombinedSegBuffer(final int itemSpaceNeeded) {
    final long segBytes = seg_.byteSize();
    final int needBytes = (itemSpaceNeeded << 3) + COMBINED_BUFFER; //+ preamble + min & max
    assert needBytes > segBytes;

    mSegReq = (mSegReq == null) ? MemorySegmentRequest.DEFAULT : mSegReq;

    final MemorySegment newSeg = mSegReq.request(seg_, needBytes);
    MemorySegment.copy(seg_, 0, newSeg, 0, segBytes);
    mSegReq.requestClose(seg_);
    return newSeg;
  }
}
