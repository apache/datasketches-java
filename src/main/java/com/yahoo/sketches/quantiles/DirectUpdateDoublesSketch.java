/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.PreambleUtil.COMBINED_BUFFER;
import static com.yahoo.sketches.quantiles.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.quantiles.PreambleUtil.MAX_DOUBLE;
import static com.yahoo.sketches.quantiles.PreambleUtil.MIN_DOUBLE;
import static com.yahoo.sketches.quantiles.PreambleUtil.N_LONG;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractFlags;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractK;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractN;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertFamilyID;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertFlags;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertK;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertMaxDouble;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertMinDouble;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertN;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertPreLongs;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertSerVer;
import static com.yahoo.sketches.quantiles.Util.computeBitPattern;

import com.yahoo.memory.MemoryRequestServer;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;

/**
 * Implements the DoublesSketch off-heap.
 *
 * @author Kevin Lang
 * @author Lee Rhodes
 *
 */
final class DirectUpdateDoublesSketch extends DirectUpdateDoublesSketchR {
  MemoryRequestServer memReqSvr = null;


  private DirectUpdateDoublesSketch(final int k) {
    super(k); //Checks k
  }

  /**
   * Obtains a new Direct instance of a DoublesSketch, which may be off-heap.
   *
   * @param k Parameter that controls space usage of sketch and accuracy of estimates.
   * Must be greater than 1 and less than 65536 and a power of 2.
   * @param dstMem the destination Memory that will be initialized to hold the data for this sketch.
   * It must initially be at least (16 * MIN_K + 32) bytes, where MIN_K defaults to 2. As it grows
   * it will request more memory using the MemoryRequest callback.
   * @return a DirectUpdateDoublesSketch
   */
  static DirectUpdateDoublesSketch newInstance(final int k, final WritableMemory dstMem) {
    // must be able to hold at least an empty sketch
    final long memCap = dstMem.getCapacity();
    checkDirectMemCapacity(k, 0, memCap);

    //initialize dstMem
    dstMem.putLong(0, 0L); //clear pre0
    insertPreLongs(dstMem, 2);
    insertSerVer(dstMem, DoublesSketch.DOUBLES_SER_VER);
    insertFamilyID(dstMem, Family.QUANTILES.getID());
    insertFlags(dstMem, EMPTY_FLAG_MASK);
    insertK(dstMem, k);

    if (memCap >= COMBINED_BUFFER) {
      insertN(dstMem, 0L);
      insertMinDouble(dstMem, Double.NaN);
      insertMaxDouble(dstMem, Double.NaN);
    }

    final DirectUpdateDoublesSketch dds = new DirectUpdateDoublesSketch(k);
    dds.mem_ = dstMem;
    return dds;
  }

  /**
   * Wrap this sketch around the given non-compact Memory image of a DoublesSketch.
   *
   * @param srcMem the given non-compact Memory image of a DoublesSketch that may have data
   * @return a sketch that wraps the given srcMem
   */
  static DirectUpdateDoublesSketch wrapInstance(final WritableMemory srcMem) {
    final long memCap = srcMem.getCapacity();

    final int preLongs = extractPreLongs(srcMem);
    final int serVer = extractSerVer(srcMem);
    final int familyID = extractFamilyID(srcMem);
    final int flags = extractFlags(srcMem);
    final int k = extractK(srcMem);

    final boolean empty = (flags & EMPTY_FLAG_MASK) > 0; //Preamble flags empty state
    final long n = empty ? 0 : extractN(srcMem);

    //VALIDITY CHECKS
    checkPreLongs(preLongs);
    Util.checkFamilyID(familyID);
    DoublesUtil.checkDoublesSerVer(serVer, MIN_DIRECT_DOUBLES_SER_VER);
    checkDirectFlags(flags); //Cannot be compact
    Util.checkK(k);
    checkCompact(serVer, flags);
    checkDirectMemCapacity(k, n, memCap);
    checkEmptyAndN(empty, n);

    final DirectUpdateDoublesSketch dds = new DirectUpdateDoublesSketch(k);
    dds.mem_ = srcMem;
    return dds;
  }

  @Override
  public void update(final double dataItem) {
    if (Double.isNaN(dataItem)) { return; }

    final int curBBCount = getBaseBufferCount();
    final int newBBCount = curBBCount + 1; //derived, not stored

    //must check memory capacity before we put anything in it
    final int combBufItemCap = getCombinedBufferItemCapacity();
    if (newBBCount > combBufItemCap) {
      //only changes combinedBuffer when it is only a base buffer
      mem_ = growCombinedMemBuffer(2 * getK());
    }

    final long curN = getN();
    final long newN = curN + 1;

    if (curN == 0) { //set min and max values
      putMaxValue(dataItem);
      putMinValue(dataItem);
    } else {
      if (dataItem > getMaxValue()) { putMaxValue(dataItem); }
      if (dataItem < getMinValue()) { putMinValue(dataItem); }
    }

    mem_.putDouble(COMBINED_BUFFER + ((long) curBBCount * Double.BYTES), dataItem); //put the item
    mem_.putByte(FLAGS_BYTE, (byte) 0); //not compact, not ordered, not empty

    if (newBBCount == (2 * k_)) { //Propagate
      // make sure there will be enough levels for the propagation
      final int curMemItemCap = getCombinedBufferItemCapacity();
      final int itemSpaceNeeded = DoublesUpdateImpl.getRequiredItemCapacity(k_, newN);

      //check mem has capacity to accommodate new level
      if (itemSpaceNeeded > curMemItemCap) {
        // copies base buffer plus old levels, adds space for new level
        mem_ = growCombinedMemBuffer(itemSpaceNeeded);
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
  }

  @Override
  public void reset() {
    if (mem_.getCapacity() >= COMBINED_BUFFER) {
      mem_.putByte(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK); //not compact, not ordered
      mem_.putLong(N_LONG, 0L);
      mem_.putDouble(MIN_DOUBLE, Double.NaN);
      mem_.putDouble(MAX_DOUBLE, Double.NaN);
    }
  }

  //Restricted overrides
  //Puts

  @Override
  void putMinValue(final double minValue) {
    assert (mem_.getCapacity() >= COMBINED_BUFFER);
    mem_.putDouble(MIN_DOUBLE, minValue);
  }

  @Override
  void putMaxValue(final double maxValue) {
    assert (mem_.getCapacity() >= COMBINED_BUFFER);
    mem_.putDouble(MAX_DOUBLE, maxValue);
  }

  @Override
  void putN(final long n) {
    assert (mem_.getCapacity() >= COMBINED_BUFFER);
    mem_.putLong(N_LONG, n);
  }

  @Override
  void putCombinedBuffer(final double[] combinedBuffer) {
    mem_.putDoubleArray(COMBINED_BUFFER, combinedBuffer, 0, combinedBuffer.length);
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
    mem_ = growCombinedMemBuffer(itemSpaceNeeded);
    // copy out any data that was there
    final double[] newCombBuf = new double[itemSpaceNeeded];
    mem_.getDoubleArray(COMBINED_BUFFER, newCombBuf, 0, curCombBufItemCap);
    return newCombBuf;
  }

  //Direct supporting methods

  private WritableMemory growCombinedMemBuffer(final int itemSpaceNeeded) {
    final long memBytes = mem_.getCapacity();
    final int needBytes = (itemSpaceNeeded << 3) + COMBINED_BUFFER; //+ preamble + min & max
    assert needBytes > memBytes;

    memReqSvr = (memReqSvr == null) ? mem_.getMemoryRequestServer() : memReqSvr;

    final WritableMemory newMem = memReqSvr.request(needBytes);

    mem_.copyTo(0, newMem, 0, memBytes);

    memReqSvr.requestClose(mem_, newMem);

    return newMem;
  }
}
