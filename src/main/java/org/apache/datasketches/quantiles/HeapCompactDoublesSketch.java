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

import static org.apache.datasketches.quantiles.PreambleUtil.COMPACT_FLAG_MASK;
import static org.apache.datasketches.quantiles.PreambleUtil.EMPTY_FLAG_MASK;
import static org.apache.datasketches.quantiles.PreambleUtil.MAX_DOUBLE;
import static org.apache.datasketches.quantiles.PreambleUtil.MIN_DOUBLE;
import static org.apache.datasketches.quantiles.PreambleUtil.READ_ONLY_FLAG_MASK;
import static org.apache.datasketches.quantiles.PreambleUtil.extractFamilyID;
import static org.apache.datasketches.quantiles.PreambleUtil.extractFlags;
import static org.apache.datasketches.quantiles.PreambleUtil.extractK;
import static org.apache.datasketches.quantiles.PreambleUtil.extractN;
import static org.apache.datasketches.quantiles.PreambleUtil.extractPreLongs;
import static org.apache.datasketches.quantiles.PreambleUtil.extractSerVer;
import static org.apache.datasketches.quantiles.Util.computeBaseBufferItems;
import static org.apache.datasketches.quantiles.Util.computeBitPattern;
import static org.apache.datasketches.quantiles.Util.computeRetainedItems;

import java.util.Arrays;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * Implements the DoublesSketch on the Java heap.
 *
 * @author Lee Rhodes
 * @author Jon Malkin
 */
final class HeapCompactDoublesSketch extends CompactDoublesSketch {
  static final int MIN_HEAP_DOUBLES_SER_VER = 1;

  /**
   * The smallest value ever seen in the stream.
   */
  private double minValue_;

  /**
   * The largest value ever seen in the stream.
   */
  private double maxValue_;

  /**
   * The total count of items seen.
   */
  private long n_;

  /**
   * Number of samples currently in base buffer.
   *
   * <p>Count = N % (2*K)
   */
  private int baseBufferCount_;

  /**
   * Active levels expressed as a bit pattern.
   *
   * <p>Pattern = N / (2 * K)
   */
  private long bitPattern_;

  /**
   * This single array contains the base buffer plus all used levels.
   * A level is of size K and is either full and sorted.
   * Whether a level buffer is present is indicated by the bitPattern_.
   * The base buffer is sorted and has max length 2*K but uses only baseBufferCount_ items.
   * The base buffer precedes the level buffers. This buffer does not include the min, max values.
   *
   * <p>The levels arrays require quite a bit of explanation, which we defer until later.</p>
   */
  private double[] combinedBuffer_;

  //**CONSTRUCTORS**********************************************************
  private HeapCompactDoublesSketch(final int k) {
    super(k); //Checks k
  }

  /**
   * Converts the given UpdateDoublesSketch to this compact form.
   *
   * @param sketch the sketch to convert
   * @return a HeapCompactDoublesSketch created from an UpdateDoublesSketch
   */
  static HeapCompactDoublesSketch createFromUpdateSketch(final UpdateDoublesSketch sketch) {
    final int k = sketch.getK();
    final long n = sketch.getN();

    final HeapCompactDoublesSketch hcds = new HeapCompactDoublesSketch(k); // checks k

    hcds.n_ = n;
    hcds.bitPattern_ = computeBitPattern(k, n);
    assert hcds.bitPattern_ == sketch.getBitPattern();

    hcds.minValue_ = sketch.getMinValue();
    hcds.maxValue_ = sketch.getMaxValue();
    hcds.baseBufferCount_ = computeBaseBufferItems(k, n);
    assert hcds.baseBufferCount_ == sketch.getBaseBufferCount();

    //if (sketch.isEmpty()) {
    //  hcds.combinedBuffer_ = null;
    //  return hcds;
    //}

    final int retainedItems = computeRetainedItems(k, n);
    final double[] combinedBuffer = new double[retainedItems];

    final DoublesSketchAccessor accessor = DoublesSketchAccessor.wrap(sketch);
    assert hcds.baseBufferCount_ == accessor.numItems();

    // copy and sort base buffer
    System.arraycopy(accessor.getArray(0, hcds.baseBufferCount_), 0,
            combinedBuffer, 0,
            hcds.baseBufferCount_);
    Arrays.sort(combinedBuffer, 0, hcds.baseBufferCount_);

    int combinedBufferOffset = hcds.baseBufferCount_;
    long bitPattern = hcds.bitPattern_;
    for (int lvl = 0; bitPattern > 0; ++lvl, bitPattern >>>= 1) {
      if ((bitPattern & 1L) > 0L) {
        accessor.setLevel(lvl);
        System.arraycopy(accessor.getArray(0, k), 0,
                combinedBuffer, combinedBufferOffset, k);
        combinedBufferOffset += k;
      }
    }
    hcds.combinedBuffer_ = combinedBuffer;

    return hcds;
  }

  /**
   * Heapifies the given srcMem, which must be a Memory image of a DoublesSketch and may have data.
   *
   * @param srcMem a Memory image of a sketch, which may be in compact or not compact form.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return a DoublesSketch on the Java heap.
   */
  static HeapCompactDoublesSketch heapifyInstance(final Memory srcMem) {
    final long memCapBytes = srcMem.getCapacity();
    if (memCapBytes < Long.BYTES) {
      throw new SketchesArgumentException("Source Memory too small: " + memCapBytes + " < 8");
    }

    final int preLongs = extractPreLongs(srcMem);
    final int serVer = extractSerVer(srcMem);
    final int familyID = extractFamilyID(srcMem);
    final int flags = extractFlags(srcMem);
    final int k = extractK(srcMem);

    final boolean empty = (flags & EMPTY_FLAG_MASK) > 0; //Preamble flags empty state
    final long n = empty ? 0 : extractN(srcMem);

    //VALIDITY CHECKS
    DoublesUtil.checkDoublesSerVer(serVer, MIN_HEAP_DOUBLES_SER_VER);
    Util.checkHeapFlags(flags);
    HeapUpdateDoublesSketch.checkPreLongsFlagsSerVer(flags, serVer, preLongs);
    Util.checkFamilyID(familyID);

    final HeapCompactDoublesSketch hds = new HeapCompactDoublesSketch(k); //checks k
    if (empty) {
      hds.n_ = 0;
      hds.combinedBuffer_ = null;
      hds.baseBufferCount_ = 0;
      hds.bitPattern_ = 0;
      hds.minValue_ = Double.NaN;
      hds.maxValue_ = Double.NaN;
      return hds;
    }

    //Not empty, must have valid preamble + min, max, n.
    //Forward compatibility from SerVer = 1 :
    final boolean srcIsCompact = (serVer == 2) | ((flags & (COMPACT_FLAG_MASK | READ_ONLY_FLAG_MASK)) > 0);

    HeapUpdateDoublesSketch.checkHeapMemCapacity(k, n, srcIsCompact, serVer, memCapBytes);

    //set class members by computing them
    hds.n_ = n;
    hds.baseBufferCount_ = computeBaseBufferItems(k, n);
    hds.bitPattern_ = computeBitPattern(k, n);
    hds.minValue_ = srcMem.getDouble(MIN_DOUBLE);
    hds.maxValue_ = srcMem.getDouble(MAX_DOUBLE);

    final int totItems = Util.computeRetainedItems(k, n);
    hds.srcMemoryToCombinedBuffer(srcMem, serVer, srcIsCompact, totItems);

    return hds;
  }

  @Override
  public long getN() {
    return n_;
  }

  @Override
  public boolean isDirect() {
    return false;
  }

  @Override
  public double getMinValue() {
    return minValue_;
  }

  @Override
  public double getMaxValue() {
    return maxValue_;
  }

  /**
   * Loads the Combined Buffer from the given source Memory.
   * The resulting Combined Buffer is allocated in this method and is always in compact form.
   * @param srcMem the given source Memory
   * @param serVer the serialization version of the source
   * @param srcIsCompact true if the given source Memory is in compact form
   * @param combBufCap total items for the combined buffer (size in doubles)
   */
  private void srcMemoryToCombinedBuffer(final Memory srcMem, final int serVer,
                                         final boolean srcIsCompact, final int combBufCap) {
    final int preLongs = 2;
    final int extra = (serVer == 1) ? 3 : 2; // space for min and max values, buf alloc (SerVer 1)
    final int preBytes = (preLongs + extra) << 3;

    final int k = getK();
    combinedBuffer_ = new double[combBufCap];

    if (srcIsCompact) {
      // just load the array, sort base buffer if serVer 2
      srcMem.getDoubleArray(preBytes, combinedBuffer_, 0, combBufCap);
      if (serVer == 2) {
        Arrays.sort(combinedBuffer_, 0, baseBufferCount_);
      }
    } else {
      // non-compact source
      // load base buffer and ensure it's sorted
      srcMem.getDoubleArray(preBytes, combinedBuffer_, 0, baseBufferCount_);
      Arrays.sort(combinedBuffer_, 0, baseBufferCount_);

      // iterate through levels
      int srcOffset = preBytes + ((2 * k) << 3);
      int dstOffset = baseBufferCount_;
      long bitPattern = bitPattern_;
      for (; bitPattern != 0; srcOffset += (k << 3), bitPattern >>>= 1) {
        if ((bitPattern & 1L) > 0L) {
          srcMem.getDoubleArray(srcOffset, combinedBuffer_, dstOffset, k);
          dstOffset += k;
        }
      }
    }
  }

  //Restricted overrides
  //Gets

  @Override
  int getBaseBufferCount() {
    return baseBufferCount_;
  }

  @Override
  int getCombinedBufferItemCapacity() {
    return combinedBuffer_.length;
  }

  @Override
  double[] getCombinedBuffer() {
    return combinedBuffer_;
  }

  @Override
  long getBitPattern() {
    return bitPattern_;
  }

  @Override
  WritableMemory getMemory() {
    return null;
  }
}
