/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.PreambleUtil.COMBINED_BUFFER;
import static com.yahoo.sketches.quantiles.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.quantiles.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.quantiles.PreambleUtil.K_SHORT;
import static com.yahoo.sketches.quantiles.PreambleUtil.MAX_DOUBLE;
import static com.yahoo.sketches.quantiles.PreambleUtil.MIN_DOUBLE;
import static com.yahoo.sketches.quantiles.PreambleUtil.N_LONG;
import static com.yahoo.sketches.quantiles.PreambleUtil.ORDERED_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.quantiles.PreambleUtil.READ_ONLY_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.SER_VER_BYTE;
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

import com.yahoo.memory.Memory;
import com.yahoo.memory.MemoryUtil;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * Implements the DoublesSketch off-heap.
 *
 * @author Kevin Lang
 * @author Lee Rhodes
 *
 */
final class DirectUpdateDoublesSketch extends UpdateDoublesSketch {
  private static final int MIN_DIRECT_DOUBLES_SER_VER = 3;
  private Memory mem_;

  //**CONSTRUCTORS**********************************************************
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
  static DirectUpdateDoublesSketch newInstance(final int k, final Memory dstMem) {
    // must be able to hold at least an empty sketch
    final long memCap = dstMem.getCapacity();
    checkDirectMemCapacity(k, 0, memCap);

    final Object memObj = dstMem.array();
    final long memAdd = dstMem.getCumulativeOffset(0L);

    //initialize dstMem
    dstMem.putLong(0, 0L); //clear pre0
    insertPreLongs(memObj, memAdd, 2);
    insertSerVer(memObj, memAdd, DoublesSketch.DOUBLES_SER_VER);
    insertFamilyID(memObj, memAdd, Family.QUANTILES.getID());
    insertFlags(memObj, memAdd, EMPTY_FLAG_MASK);
    insertK(memObj, memAdd, k);

    if (memCap >= COMBINED_BUFFER) {
      insertN(memObj, memAdd, 0L);
      insertMinDouble(memObj, memAdd, Double.POSITIVE_INFINITY);
      insertMaxDouble(memObj, memAdd, Double.NEGATIVE_INFINITY);
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
  static DirectUpdateDoublesSketch wrapInstance(Memory srcMem) {
    final long memCap = srcMem.getCapacity();

    final int preLongs;
    final int serVer;
    final int familyID;
    final int flags;
    final int k;
    final boolean empty;
    final long n;

    if (srcMem.isReadOnly() && !srcMem.isDirect()) {
      preLongs = srcMem.getByte(PREAMBLE_LONGS_BYTE) & 0XFF;
      serVer = srcMem.getByte(SER_VER_BYTE) & 0XFF;
      familyID = srcMem.getByte(FAMILY_BYTE) & 0XFF;
      flags = srcMem.getByte(FLAGS_BYTE) & 0XFF;
      k = srcMem.getShort(K_SHORT) & 0XFFFF;

      empty = (flags & EMPTY_FLAG_MASK) > 0; //Preamble flags empty state
      n = empty ? 0 : srcMem.getLong(N_LONG);
    } else {
      final Object memObj = srcMem.array(); //may be null
      final long memAdd = srcMem.getCumulativeOffset(0L);

      preLongs = extractPreLongs(memObj, memAdd);
      serVer = extractSerVer(memObj, memAdd);
      familyID = extractFamilyID(memObj, memAdd);
      flags = extractFlags(memObj, memAdd);
      k = extractK(memObj, memAdd);

      empty = (flags & EMPTY_FLAG_MASK) > 0; //Preamble flags empty state
      n = empty ? 0 : extractN(memObj, memAdd);
    }

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
    final long curN = getN();
    final long newN = curN + 1;

    final int combBufItemCap = getCombinedBufferItemCapacity();
    if (newBBCount > combBufItemCap) {
      //only changes combinedBuffer when it is only a base buffer
      mem_ = growCombinedMemBuffer(mem_, 2 * getK());
    }

    final double maxValue = getMaxValue();
    final double minValue = getMinValue();

    if (dataItem > maxValue) { putMaxValue(dataItem); }
    if (dataItem < minValue) { putMinValue(dataItem); }

    mem_.putDouble(COMBINED_BUFFER + curBBCount * Double.BYTES, dataItem); //put the item
    mem_.putByte(FLAGS_BYTE, (byte) 0); //not compact, not ordered, not empty

    if (newBBCount == 2 * k_) { //Propagate
      // make sure there will be enough levels for the propagation
      final int curMemItemCap = getCombinedBufferItemCapacity();
      final int itemSpaceNeeded = DoublesUpdateImpl.getRequiredItemCapacity(k_, newN);

      //check mem has capacity to accommodate new level
      if (itemSpaceNeeded > curMemItemCap) {
        // copies base buffer plus old levels, adds space for new level
        mem_ = growCombinedMemBuffer(mem_, itemSpaceNeeded);
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
  public long getN() {
    if (mem_.getCapacity() < COMBINED_BUFFER) {
      return 0;
    } else {
      return mem_.getLong(N_LONG);
    }
  }

  @Override
  public boolean isDirect() {
    return true;
  }

  @Override
  public double getMinValue() {
    if (mem_.getCapacity() < COMBINED_BUFFER) {
      return Double.POSITIVE_INFINITY;
    } else {
      return mem_.getDouble(MIN_DOUBLE);
    }
  }

  @Override
  public double getMaxValue() {
    if (mem_.getCapacity() < COMBINED_BUFFER) {
      return Double.NEGATIVE_INFINITY;
    } else {
      return mem_.getDouble(MAX_DOUBLE);
    }
  }

  @Override
  public void reset() {
    if (mem_.getCapacity() >= COMBINED_BUFFER) {
      mem_.putByte(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK); //not compact, not ordered
      mem_.putLong(N_LONG, 0L);
      mem_.putDouble(MIN_DOUBLE, Double.POSITIVE_INFINITY);
      mem_.putDouble(MAX_DOUBLE, Double.NEGATIVE_INFINITY);
    }
  }

  //Restricted overrides
  //Gets

  @Override
  int getBaseBufferCount() {
    return Util.computeBaseBufferItems(getK(), getN());
  }

  @Override
  int getCombinedBufferItemCapacity() {
    return ((int)mem_.getCapacity() - COMBINED_BUFFER) / 8;
  }

  @Override
  double[] getCombinedBuffer() {
    final int k = getK();
    if (isEmpty()) { return new double[k << 1]; } //2K
    final long n = getN();
    final int itemCap = Util.computeCombinedBufferItemCapacity(k, n, false);
    final double[] combinedBuffer = new double[itemCap];
    mem_.getDoubleArray(COMBINED_BUFFER, combinedBuffer, 0, itemCap);
    return combinedBuffer;
  }

  @Override
  long getBitPattern() {
    final int k = getK();
    final long n = getN();
    return Util.computeBitPattern(k, n);
  }

  @Override
  Memory getMemory() {
    return mem_;
  }

  //Puts

  @Override
  void putMinValue(final double minValue) {
    if (mem_.getCapacity() < COMBINED_BUFFER) {
      mem_ = growCombinedMemBuffer(mem_, 2 * getK());
    }
    mem_.putDouble(MIN_DOUBLE, minValue);
  }

  @Override
  void putMaxValue(final double maxValue) {
    if (mem_.getCapacity() < COMBINED_BUFFER) {
      mem_ = growCombinedMemBuffer(mem_, 2 * getK());
    }
    mem_.putDouble(MAX_DOUBLE, maxValue);
  }

  @Override
  void putN(final long n) {
    if (mem_.getCapacity() < COMBINED_BUFFER) {
      mem_ = growCombinedMemBuffer(mem_, 2 * getK());
    }
    mem_.putLong(N_LONG, n);
  }

  @Override
  void putCombinedBuffer(final double[] combinedBuffer) {
    mem_.putDoubleArray(COMBINED_BUFFER, combinedBuffer, 0, combinedBuffer.length);
  }

  @Override
  void putCombinedBufferItemCapacity(final int combBufItemCap) {
    //intentionally a no-op, not kept on-heap, always derived.
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
    mem_ = growCombinedMemBuffer(mem_, itemSpaceNeeded);
    // copy out any data that was there
    final double[] newCombBuf = new double[itemSpaceNeeded];
    mem_.getDoubleArray(COMBINED_BUFFER, newCombBuf, 0, curCombBufItemCap);
    return newCombBuf;
  }

  //Direct supporting methods

  Memory growCombinedMemBuffer(final Memory mem, final int itemSpaceNeeded) {
    final long memBytes = mem.getCapacity();
    final int needBytes = (itemSpaceNeeded << 3) + COMBINED_BUFFER; //+ preamble + min & max
    assert needBytes > memBytes;

    final Memory newMem = MemoryUtil.memoryRequestHandler(mem, needBytes, true);
    //the free has already been handled
    return newMem;
  }

  //Checks

  /**
   * Checks the validity of the direct memory capacity assuming n, k.
   * @param k the given value of k
   * @param n the given value of n
   * @param memCapBytes the current memory capacity in bytes
   */
  static void checkDirectMemCapacity(final int k, final long n, final long memCapBytes) {
    final int reqBufBytes = getUpdatableStorageBytes(k, n, true);

    if (memCapBytes < reqBufBytes) {
      throw new SketchesArgumentException("Possible corruption: Memory capacity too small: "
          + memCapBytes + " < " + reqBufBytes);
    }
  }

  static void checkCompact(final int serVer, final int flags) {
    final boolean compact = (serVer == 2) | ((flags & COMPACT_FLAG_MASK) > 0);
    if (compact) {
      throw new SketchesArgumentException("Compact Memory is not supported for Wrap Instance.");
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
