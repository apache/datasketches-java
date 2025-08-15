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
import static org.apache.datasketches.quantiles.ClassicUtil.DOUBLES_SER_VER;
import static org.apache.datasketches.quantiles.ClassicUtil.checkFamilyID;
import static org.apache.datasketches.quantiles.ClassicUtil.checkK;
import static org.apache.datasketches.quantiles.ClassicUtil.computeBaseBufferItems;
import static org.apache.datasketches.quantiles.ClassicUtil.computeBitPattern;
import static org.apache.datasketches.quantiles.ClassicUtil.computeRetainedItems;
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
import static org.apache.datasketches.quantiles.PreambleUtil.insertFamilyID;
import static org.apache.datasketches.quantiles.PreambleUtil.insertFlags;
import static org.apache.datasketches.quantiles.PreambleUtil.insertK;
import static org.apache.datasketches.quantiles.PreambleUtil.insertMaxDouble;
import static org.apache.datasketches.quantiles.PreambleUtil.insertMinDouble;
import static org.apache.datasketches.quantiles.PreambleUtil.insertN;
import static org.apache.datasketches.quantiles.PreambleUtil.insertPreLongs;
import static org.apache.datasketches.quantiles.PreambleUtil.insertSerVer;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.MemorySegmentStatus;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.quantilescommon.QuantilesAPI;

/**
 * Implements the DoublesSketch off-heap.
 *
 * @author Kevin Lang
 * @author Lee Rhodes
 * @author Jon Malkin
 */
final class DirectCompactDoublesSketch extends CompactDoublesSketch {
  private static final int MIN_DIRECT_DOUBLES_SER_VER = 3;
  private final MemorySegment seg_;

  //**CONSTRUCTORS**********************************************************
  private DirectCompactDoublesSketch(final int k, final MemorySegment seg) {
    super(k); //Checks k
    seg_ = seg.asReadOnly();
  }

  /**
   * Converts the given UpdateDoublesSketch to this compact form.
   *
   * @param sketch the sketch to convert
   * @param dstSeg the MemorySegment to use for the destination
   * @return a DirectCompactDoublesSketch created from an UpdateDoublesSketch
   */
  static DirectCompactDoublesSketch createFromUpdateSketch(final UpdateDoublesSketch sketch,
                                                           final MemorySegment dstSeg) {
    final long segCap = dstSeg.byteSize();
    final int k = sketch.getK();
    final long n = sketch.getN();
    checkDirectSegCapacity(k, n, segCap);

    //initialize dstSeg
    dstSeg.set(JAVA_LONG_UNALIGNED, 0, 0L); //clear pre0
    insertPreLongs(dstSeg, 2);
    insertSerVer(dstSeg, DOUBLES_SER_VER);
    insertFamilyID(dstSeg, Family.QUANTILES.getID());
    insertK(dstSeg, k);

    final int flags = COMPACT_FLAG_MASK | READ_ONLY_FLAG_MASK; // both true for all compact sketches

    if (sketch.isEmpty()) {
      insertFlags(dstSeg, flags | EMPTY_FLAG_MASK);
    } else {
      insertFlags(dstSeg, flags);
      insertN(dstSeg, n);
      insertMinDouble(dstSeg, sketch.getMinItem());
      insertMaxDouble(dstSeg, sketch.getMaxItem());

      final int bbCount = computeBaseBufferItems(k, n);

      final DoublesSketchAccessor inputAccessor = DoublesSketchAccessor.wrap(sketch, false);
      assert bbCount == inputAccessor.numItems();

      long dstSegOffset = COMBINED_BUFFER;

      // copy and sort base buffer
      final double[] bbArray = inputAccessor.getArray(0, bbCount);
      Arrays.sort(bbArray);
      MemorySegment.copy(bbArray, 0, dstSeg, JAVA_DOUBLE_UNALIGNED, dstSegOffset, bbCount);
      dstSegOffset += bbCount << 3;

      long bitPattern = computeBitPattern(k, n);
      for (int lvl = 0; bitPattern > 0; ++lvl, bitPattern >>>= 1) {
        if ((bitPattern & 1L) > 0L) {
          inputAccessor.setLevel(lvl);
          MemorySegment.copy(inputAccessor.getArray(0, k), 0, dstSeg, JAVA_DOUBLE_UNALIGNED, dstSegOffset, k);
          dstSegOffset += k << 3;
        }
      }
    }

    return new DirectCompactDoublesSketch(k, dstSeg);
  }

  /**
   * Wrap this sketch around the given compact MemorySegment image of a DoublesSketch.
   *
   * @param srcSeg the given compact MemorySegment image of a DoublesSketch,
   * @return a sketch that wraps the given srcSeg.
   */
  static DirectCompactDoublesSketch wrapInstance(final MemorySegment srcSeg) {
    final long segCap = srcSeg.byteSize();

    final int preLongs = extractPreLongs(srcSeg);
    final int serVer = extractSerVer(srcSeg);
    final int familyID = extractFamilyID(srcSeg);
    final int flags = extractFlags(srcSeg);
    final int k = extractK(srcSeg);

    final boolean empty = (flags & EMPTY_FLAG_MASK) > 0;
    final long n = empty ? 0 : extractN(srcSeg);

    //VALIDITY CHECKS
    DirectUpdateDoublesSketch.checkPreLongs(preLongs);
    checkFamilyID(familyID);
    DoublesUtil.checkDoublesSerVer(serVer, MIN_DIRECT_DOUBLES_SER_VER);
    checkCompact(serVer, flags);
    checkK(k);
    checkDirectSegCapacity(k, n, segCap);
    DirectUpdateDoublesSketch.checkEmptyAndN(empty, n);

    return  new DirectCompactDoublesSketch(k, srcSeg);
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
  public boolean isSameResource(final MemorySegment that) {
    return MemorySegmentStatus.isSameResource(seg_, that);
  }

  //Restricted overrides
  //Gets

  @Override
  int getBaseBufferCount() {
    return computeBaseBufferItems(getK(), getN());
  }

  @Override
  int getCombinedBufferItemCapacity() {
    return ((int)seg_.byteSize() - COMBINED_BUFFER) / 8;
  }

  @Override
  double[] getCombinedBuffer() {
    final int k = getK();
    if (isEmpty()) { return new double[k << 1]; } //2K
    final long n = getN();
    final int itemCap = computeRetainedItems(k, n);
    final double[] combinedBuffer = new double[itemCap];
    MemorySegment.copy(seg_, JAVA_DOUBLE_UNALIGNED, COMBINED_BUFFER, combinedBuffer, 0, itemCap);
    return combinedBuffer;
  }

  @Override
  long getBitPattern() {
    final int k = getK();
    final long n = getN();
    return computeBitPattern(k, n);
  }

  @Override
  MemorySegment getMemorySegment() {
    return seg_;
  }

  //Checks

  /**
   * Checks the validity of the direct MemorySegment capacity assuming n, k.
   * @param k the given k
   * @param n the given n
   * @param segCapBytes the current MemorySegment capacity in bytes
   */
  static void checkDirectSegCapacity(final int k, final long n, final long segCapBytes) {
    final int reqBufBytes = getCompactSerialiedSizeBytes(k, n);

    if (segCapBytes < reqBufBytes) {
      throw new SketchesArgumentException("Possible corruption: MemorySegment capacity too small: "
          + segCapBytes + " < " + reqBufBytes);
    }
  }

  /**
   * Checks a sketch's serial version and flags to see if the sketch can be wrapped as a
   * DirectCompactDoubleSketch. Throws an exception if the sketch is neither empty nor compact
   * and ordered, unless the sketch uses serialization version 2.
   * @param serVer the serialization version
   * @param flags Flags from the sketch to evaluate
   */
  static void checkCompact(final int serVer, final int flags) {
    final int compactFlagMask = COMPACT_FLAG_MASK | ORDERED_FLAG_MASK;
    if ((serVer != 2)
            && ((flags & EMPTY_FLAG_MASK) == 0)
            && ((flags & compactFlagMask) != compactFlagMask)) {
      throw new SketchesArgumentException(
              "Possible corruption: Must be v2, empty, or compact and ordered. Flags field: "
                      + Integer.toBinaryString(flags) + ", SerVer: " + serVer);
    }
  }
}
