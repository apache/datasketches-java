/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.PreambleUtil.COMBINED_BUFFER;
import static com.yahoo.sketches.quantiles.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.MAX_DOUBLE;
import static com.yahoo.sketches.quantiles.PreambleUtil.MIN_DOUBLE;
import static com.yahoo.sketches.quantiles.PreambleUtil.N_LONG;
import static com.yahoo.sketches.quantiles.PreambleUtil.ORDERED_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.READ_ONLY_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractFlags;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractK;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractN;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractSerVer;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesReadOnlyException;

/**
 * Implements the DoublesSketch off-heap.
 *
 * @author Kevin Lang
 * @author Lee Rhodes
 *
 */
class DirectUpdateDoublesSketchR extends UpdateDoublesSketch {
  static final int MIN_DIRECT_DOUBLES_SER_VER = 3;
  WritableMemory mem_;

  //**CONSTRUCTORS**********************************************************
  DirectUpdateDoublesSketchR(final int k) {
    super(k); //Checks k
  }

  /**
   * Wrap this sketch around the given non-compact Memory image of a DoublesSketch.
   *
   * @param srcMem the given non-compact Memory image of a DoublesSketch that may have data
   * @return a sketch that wraps the given srcMem
   */
  static DirectUpdateDoublesSketchR wrapInstance(final Memory srcMem) {
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

    final DirectUpdateDoublesSketchR dds = new DirectUpdateDoublesSketchR(k);
    dds.mem_ = (WritableMemory) srcMem;
    return dds;
  }

  @Override
  public double getMaxValue() {
    return isEmpty() ? Double.NaN : mem_.getDouble(MAX_DOUBLE);
  }

  @Override
  public double getMinValue() {
    return isEmpty() ? Double.NaN : mem_.getDouble(MIN_DOUBLE);
  }

  @Override
  public long getN() {
    return (mem_.getCapacity() < COMBINED_BUFFER) ? 0 : mem_.getLong(N_LONG);
  }

  @Override
  public boolean isDirect() {
    return true;
  }

  @Override
  public boolean isSameResource(final Memory that) {
    return mem_.isSameResource(that);
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
    final int itemCap = Util.computeCombinedBufferItemCapacity(k, n);
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
  WritableMemory getMemory() {
    return mem_;
  }

  //Puts

  @Override
  void putMinValue(final double minValue) {
    throw new SketchesReadOnlyException("Call to putMinValue() on read-only buffer");
  }

  @Override
  void putMaxValue(final double maxValue) {
    throw new SketchesReadOnlyException("Call to putMaxValue() on read-only buffer");
  }

  @Override
  void putN(final long n) {
    throw new SketchesReadOnlyException("Call to putN() on read-only buffer");
  }

  @Override
  void putCombinedBuffer(final double[] combinedBuffer) {
    throw new SketchesReadOnlyException("Call to putCombinedBuffer() on read-only buffer");
  }

  @Override
  void putBaseBufferCount(final int baseBufferCount) {
    throw new SketchesReadOnlyException("Call to putBaseBufferCount() on read-only buffer");
  }

  @Override
  void putBitPattern(final long bitPattern) {
    throw new SketchesReadOnlyException("Call to putBaseBufferCount() on read-only buffer");
  }

  @Override
  double[] growCombinedBuffer(final int curCombBufItemCap, final int itemSpaceNeeded) {
    throw new SketchesReadOnlyException("Call to growCombinedBuffer() on read-only buffer");
  }

  //Checks

  /**
   * Checks the validity of the direct memory capacity assuming n, k.
   * @param k the given value of k
   * @param n the given value of n
   * @param memCapBytes the current memory capacity in bytes
   */
  static void checkDirectMemCapacity(final int k, final long n, final long memCapBytes) {
    final int reqBufBytes = getUpdatableStorageBytes(k, n);

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
