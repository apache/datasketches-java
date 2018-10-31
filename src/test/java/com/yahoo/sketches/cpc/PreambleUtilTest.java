/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.Util.computeSeedHash;
import static com.yahoo.sketches.cpc.PreambleUtil.COMPRESSED_FLAG_MASK;
import static com.yahoo.sketches.cpc.PreambleUtil.SER_VER;
import static com.yahoo.sketches.cpc.PreambleUtil.getDefinedPreInts;
import static com.yahoo.sketches.cpc.PreambleUtil.getFamily;
import static com.yahoo.sketches.cpc.PreambleUtil.getFiCol;
import static com.yahoo.sketches.cpc.PreambleUtil.getFlags;
import static com.yahoo.sketches.cpc.PreambleUtil.getFormat;
import static com.yahoo.sketches.cpc.PreambleUtil.getHipAccum;
import static com.yahoo.sketches.cpc.PreambleUtil.getKxP;
import static com.yahoo.sketches.cpc.PreambleUtil.getLgK;
import static com.yahoo.sketches.cpc.PreambleUtil.getNumCoupons;
import static com.yahoo.sketches.cpc.PreambleUtil.getNumSv;
import static com.yahoo.sketches.cpc.PreambleUtil.getPreInts;
import static com.yahoo.sketches.cpc.PreambleUtil.getSeedHash;
import static com.yahoo.sketches.cpc.PreambleUtil.getSerVer;
import static com.yahoo.sketches.cpc.PreambleUtil.getSvLengthInts;
import static com.yahoo.sketches.cpc.PreambleUtil.getSvStreamOffset;
import static com.yahoo.sketches.cpc.PreambleUtil.getWLengthInts;
import static com.yahoo.sketches.cpc.PreambleUtil.getWStreamOffset;
import static com.yahoo.sketches.cpc.PreambleUtil.hasHip;
import static com.yahoo.sketches.cpc.PreambleUtil.putEmptyMerged;
import static com.yahoo.sketches.cpc.PreambleUtil.putPinnedSlidingHip;
import static com.yahoo.sketches.cpc.PreambleUtil.putPinnedSlidingHipNoSv;
import static com.yahoo.sketches.cpc.PreambleUtil.putPinnedSlidingMerged;
import static com.yahoo.sketches.cpc.PreambleUtil.putPinnedSlidingMergedNoSv;
import static com.yahoo.sketches.cpc.PreambleUtil.putSparseHybridHip;
import static com.yahoo.sketches.cpc.PreambleUtil.putSparseHybridMerged;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesStateException;
import com.yahoo.sketches.cpc.PreambleUtil.HiField;

/**
 * @author Lee Rhodes
 */
public class PreambleUtilTest {
  static final short defaultSeedHash = computeSeedHash(DEFAULT_UPDATE_SEED) ;

  private static void checkFirst8(WritableMemory wmem, Format format, int lgK, int fiCol) {
    assertEquals(getFormat(wmem), format);
    assertEquals(getPreInts(wmem), getDefinedPreInts(format));
    assertEquals(getSerVer(wmem), SER_VER);
    assertEquals(getFamily(wmem), Family.CPC);
    assertEquals(getLgK(wmem), lgK);
    assertEquals(getFiCol(wmem), fiCol);
    assertEquals(getFlags(wmem), (format.ordinal() << 2) | COMPRESSED_FLAG_MASK);
    assertEquals(getSeedHash(wmem), defaultSeedHash);
  }

  @Test
  public void checkNormalPutMemory() {
    int lgK = 12;
    double kxp = lgK;
    double hipAccum = 1005;
    int fiCol = 1;
    int[] csvStream = new int[] {1, 2, 3};
    int numCoupons = csvStream.length;
    int csvLength = csvStream.length;
    short seedHash = defaultSeedHash;
    int[] cwStream = new int[] {4, 5, 6};
    int cwLength = cwStream.length;
    int numSv = cwStream.length;
    int maxInts = 10 + csvLength + cwLength;
    WritableMemory wmem = WritableMemory.allocate(4 * maxInts);

    Format format;

    format = Format.EMPTY_MERGED;
    putEmptyMerged(wmem, lgK, seedHash);
    println(CpcSketch.toString((byte[])wmem.getArray(), true));
    checkFirst8(wmem, format, lgK, (byte) 0);
    assertFalse(hasHip(wmem));

    format = Format.SPARSE_HYBRID_MERGED;
    putSparseHybridMerged(wmem, lgK, numCoupons, csvLength, seedHash, csvStream);
    println(CpcSketch.toString(wmem, true));
    println(CpcSketch.toString(wmem, false));
    checkFirst8(wmem, format, lgK, (byte) 0);
    assertEquals(getNumCoupons(wmem), numCoupons);
    assertEquals(getSvLengthInts(wmem), csvLength);

    format = Format.SPARSE_HYBRID_HIP;
    putSparseHybridHip(wmem, lgK, numCoupons, csvLength, kxp, hipAccum, seedHash, csvStream);
    println(CpcSketch.toString(wmem, true));
    println(CpcSketch.toString(wmem, false));
    checkFirst8(wmem, format, lgK, (byte) 0);
    assertEquals(getNumCoupons(wmem), numCoupons);
    assertEquals(getSvLengthInts(wmem), csvLength);
    assertEquals(getKxP(wmem), kxp);
    assertEquals(getHipAccum(wmem), hipAccum);
    assertTrue(hasHip(wmem));

    format = Format.PINNED_SLIDING_MERGED_NOSV;
    putPinnedSlidingMergedNoSv(wmem, lgK, fiCol, numCoupons, cwLength, seedHash, cwStream);
    println(CpcSketch.toString(wmem, true));
    println(CpcSketch.toString(wmem, false));
    checkFirst8(wmem, format, lgK, fiCol);
    assertEquals(getNumCoupons(wmem), numCoupons);
    assertEquals(getWLengthInts(wmem), cwLength);

    format = Format.PINNED_SLIDING_HIP_NOSV;
    putPinnedSlidingHipNoSv(wmem, lgK, fiCol, numCoupons, cwLength, kxp, hipAccum, seedHash,
        cwStream);
    println(CpcSketch.toString(wmem, true));
    println(CpcSketch.toString(wmem, false));
    checkFirst8(wmem, format, lgK, fiCol);
    assertEquals(getNumCoupons(wmem), numCoupons);
    assertEquals(getWLengthInts(wmem), cwLength);
    assertEquals(getKxP(wmem), kxp);
    assertEquals(getHipAccum(wmem), hipAccum);

    format = Format.PINNED_SLIDING_MERGED;
    putPinnedSlidingMerged(wmem, lgK, fiCol, numCoupons, numSv, csvLength, cwLength, seedHash,
        csvStream, cwStream);
    println(CpcSketch.toString(wmem, true));
    println(CpcSketch.toString(wmem, false));
    checkFirst8(wmem, format, lgK, fiCol);
    assertEquals(getNumCoupons(wmem), numCoupons);
    assertEquals(getNumSv(wmem), numSv);
    assertEquals(getSvLengthInts(wmem), csvLength);
    assertEquals(getWLengthInts(wmem), cwLength);

    format = Format.PINNED_SLIDING_HIP;
    putPinnedSlidingHip(wmem, lgK, fiCol, numCoupons, numSv, kxp, hipAccum, csvLength, cwLength,
        seedHash, csvStream, cwStream);
    println(CpcSketch.toString(wmem, true));
    println(CpcSketch.toString(wmem, false));
    checkFirst8(wmem, format, lgK, fiCol);
    assertEquals(getNumCoupons(wmem), numCoupons);
    assertEquals(getNumSv(wmem), numSv);
    assertEquals(getSvLengthInts(wmem), csvLength);
    assertEquals(getWLengthInts(wmem), cwLength);
    assertEquals(getKxP(wmem), kxp);
    assertEquals(getHipAccum(wmem), hipAccum);
  }

  @Test
  public void checkStreamErrors() {
    WritableMemory wmem = WritableMemory.allocate(4 * 10);
    putEmptyMerged(wmem, (byte) 12, defaultSeedHash);
    try { getSvStreamOffset(wmem); fail(); } catch (SketchesArgumentException e) { }
    wmem.putByte(5, (byte) (7 << 2));
    try { getSvStreamOffset(wmem); fail(); } catch (SketchesStateException e) { }
    wmem.putByte(5, (byte) 0);
    try { getWStreamOffset(wmem); fail(); } catch (SketchesArgumentException e) { }
    wmem.putByte(5, (byte) (7 << 2));
    try { getWStreamOffset(wmem); fail(); } catch (SketchesStateException e) { }
  }

  @Test
  public void checkStreamErrors2() {
    WritableMemory wmem = WritableMemory.allocate(4 * 10);
    int[] svStream = { 1 };
    int[] wStream = { 2 };
    try {
      putPinnedSlidingMerged(wmem, 4, 0, 1, 1, 1, 0, (short) 0, svStream, wStream);
      fail();
    } catch (SketchesStateException e) { }
    assertTrue(PreambleUtil.isCompressed(wmem));
  }

  @Test
  public void checkEmptyMemory() {
    WritableMemory wmem = WritableMemory.allocate(4 * 10);
    wmem.putByte(2, (byte) 16); //legal Family
    wmem.putByte(5, (byte) (1 << 2)); //select NONE
    println(CpcSketch.toString(wmem, false));
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkFieldError() {
    PreambleUtil.fieldError(Format.EMPTY_MERGED, HiField.NUM_COUPONS);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkCapacity() {
    PreambleUtil.checkCapacity(100, 101);
  }

  @Test(expectedExceptions = SketchesStateException.class)
  public void checkHiFieldError() {
    PreambleUtil.getHiFieldOffset(Format.EMPTY_MERGED, HiField.NUM_COUPONS);
  }

  @Test
  public void checkWindowOffset() {
    long offset = CpcUtil.determineCorrectOffset(4, 54);
    assertEquals(offset, 1L);
  }

  @Test
  public void checkFormatEnum() {
    assertEquals(Format.EMPTY_MERGED, Format.ordinalToFormat(0));
    assertEquals(Format.EMPTY_HIP, Format.ordinalToFormat(1));
    assertEquals(Format.SPARSE_HYBRID_MERGED, Format.ordinalToFormat(2));
    assertEquals(Format.SPARSE_HYBRID_HIP, Format.ordinalToFormat(3));
    assertEquals(Format.PINNED_SLIDING_MERGED_NOSV, Format.ordinalToFormat(4));
    assertEquals(Format.PINNED_SLIDING_HIP_NOSV, Format.ordinalToFormat(5));
    assertEquals(Format.PINNED_SLIDING_MERGED, Format.ordinalToFormat(6));
    assertEquals(Format.PINNED_SLIDING_HIP, Format.ordinalToFormat(7));
  }

  @Test
  public void checkFlavorEnum() {
    assertEquals(Flavor.EMPTY, Flavor.ordinalToFlavor(0));
    assertEquals(Flavor.SPARSE, Flavor.ordinalToFlavor(1));
    assertEquals(Flavor.HYBRID, Flavor.ordinalToFlavor(2));
    assertEquals(Flavor.PINNED, Flavor.ordinalToFlavor(3));
    assertEquals(Flavor.SLIDING, Flavor.ordinalToFlavor(4));
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }

}
