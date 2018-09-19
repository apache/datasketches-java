/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.Util.computeSeedHash;
import static com.yahoo.sketches.cpc.PreambleUtil.READ_ONLY_FLAG_MASK;
import static com.yahoo.sketches.cpc.PreambleUtil.SER_VER;
import static com.yahoo.sketches.cpc.PreambleUtil.getCsvLength;
import static com.yahoo.sketches.cpc.PreambleUtil.getCsvStreamOffset;
import static com.yahoo.sketches.cpc.PreambleUtil.getCwLength;
import static com.yahoo.sketches.cpc.PreambleUtil.getCwStreamOffset;
import static com.yahoo.sketches.cpc.PreambleUtil.getDefinedPreInts;
import static com.yahoo.sketches.cpc.PreambleUtil.getFamily;
import static com.yahoo.sketches.cpc.PreambleUtil.getFiCol;
import static com.yahoo.sketches.cpc.PreambleUtil.getFlags;
import static com.yahoo.sketches.cpc.PreambleUtil.getFormat;
import static com.yahoo.sketches.cpc.PreambleUtil.getHipAccum;
import static com.yahoo.sketches.cpc.PreambleUtil.getKxP;
import static com.yahoo.sketches.cpc.PreambleUtil.getLgK;
import static com.yahoo.sketches.cpc.PreambleUtil.getNumCoupons;
import static com.yahoo.sketches.cpc.PreambleUtil.getNumSV;
import static com.yahoo.sketches.cpc.PreambleUtil.getPreInts;
import static com.yahoo.sketches.cpc.PreambleUtil.getSeedHash;
import static com.yahoo.sketches.cpc.PreambleUtil.getSerVer;
import static com.yahoo.sketches.cpc.PreambleUtil.hasHip;
import static com.yahoo.sketches.cpc.PreambleUtil.putEmpty;
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
import com.yahoo.sketches.cpc.PreambleUtil.Format;
import com.yahoo.sketches.cpc.PreambleUtil.HiField;

/**
 * @author Lee Rhodes
 */
public class PreambleUtilTest {
  static final short defaultSeedHash = computeSeedHash(DEFAULT_UPDATE_SEED) ;

  private static void checkFirst8(WritableMemory wmem, Format format, byte lgK, byte fiCol) {
    assertEquals(getFormat(wmem), format);
    assertEquals(getPreInts(wmem), getDefinedPreInts(format));
    assertEquals(getSerVer(wmem), SER_VER);
    assertEquals(getFamily(wmem), Family.CPC);
    assertEquals(getLgK(wmem), lgK);
    assertEquals(getFiCol(wmem), fiCol);
    assertEquals(getFlags(wmem), (format.ordinal() << 2) | READ_ONLY_FLAG_MASK);
    assertEquals(getSeedHash(wmem), defaultSeedHash);

  }

  @Test
  public void checkNormalPutMemory() {
    byte lgK = 12;
    double kxp = lgK;
    double hipAccum = 1005;
    byte fiCol = 1;
    int[] csvStream = new int[] {1, 2, 3};
    long numCoupons = csvStream.length;
    long csvLength = csvStream.length;
    short seedHash = defaultSeedHash;
    int[] cwStream = new int[] {4, 5, 6};
    long cwLength = cwStream.length;
    long numSv = cwStream.length;
    int maxInts = 10 + (int) csvLength + (int) cwLength;
    WritableMemory wmem = WritableMemory.allocate(4 * maxInts);

    Format format;

    format = Format.EMPTY;
    putEmpty(wmem, lgK, seedHash);
    println(PreambleUtil.toString((byte[])wmem.getArray(), true));
    checkFirst8(wmem, format, lgK, (byte) 0);
    assertFalse(hasHip(wmem));

    format = Format.SPARSE_HYBRID_MERGED;
    putSparseHybridMerged(wmem, lgK, numCoupons, csvLength, seedHash, csvStream);
    println(PreambleUtil.toString(wmem, true));
    PreambleUtil.toString(wmem, false);
    checkFirst8(wmem, format, lgK, (byte) 0);
    assertEquals(getNumCoupons(wmem), numCoupons);
    assertEquals(getCsvLength(wmem), csvLength);

    format = Format.SPARSE_HYBRID_HIP;
    putSparseHybridHip(wmem, lgK, numCoupons, csvLength, kxp, hipAccum, seedHash, csvStream);
    println(PreambleUtil.toString(wmem, true));
    PreambleUtil.toString(wmem, false);
    checkFirst8(wmem, format, lgK, (byte) 0);
    assertEquals(getNumCoupons(wmem), numCoupons);
    assertEquals(getCsvLength(wmem), csvLength);
    assertEquals(getKxP(wmem), kxp);
    assertEquals(getHipAccum(wmem), hipAccum);
    assertTrue(hasHip(wmem));

    format = Format.PINNED_SLIDING_MERGED_NOSV;
    putPinnedSlidingMergedNoSv(wmem, lgK, fiCol, numCoupons, cwLength, seedHash, cwStream);
    println(PreambleUtil.toString(wmem, true));
    PreambleUtil.toString(wmem, false);
    checkFirst8(wmem, format, lgK, fiCol);
    assertEquals(getNumCoupons(wmem), numCoupons);
    assertEquals(getCwLength(wmem), cwLength);

    format = Format.PINNED_SLIDING_HIP_NOSV;
    putPinnedSlidingHipNoSv(wmem, lgK, fiCol, numCoupons, cwLength, kxp, hipAccum, seedHash,
        cwStream);
    println(PreambleUtil.toString(wmem, true));
    PreambleUtil.toString(wmem, false);
    checkFirst8(wmem, format, lgK, fiCol);
    assertEquals(getNumCoupons(wmem), numCoupons);
    assertEquals(getCwLength(wmem), cwLength);
    assertEquals(getKxP(wmem), kxp);
    assertEquals(getHipAccum(wmem), hipAccum);

    format = Format.PINNED_SLIDING_MERGED;
    putPinnedSlidingMerged(wmem, lgK, fiCol, numCoupons, numSv, csvLength, cwLength, seedHash,
        csvStream, cwStream);
    println(PreambleUtil.toString(wmem, true));
    PreambleUtil.toString(wmem, false);
    checkFirst8(wmem, format, lgK, fiCol);
    assertEquals(getNumCoupons(wmem), numCoupons);
    assertEquals(getNumSV(wmem), numSv);
    assertEquals(getCsvLength(wmem), csvLength);
    assertEquals(getCwLength(wmem), cwLength);

    format = Format.PINNED_SLIDING_HIP;
    putPinnedSlidingHip(wmem, lgK, fiCol, numCoupons, numSv, kxp, hipAccum, csvLength, cwLength,
        seedHash, csvStream, cwStream);
    println(PreambleUtil.toString(wmem, true));
    PreambleUtil.toString(wmem, false);
    checkFirst8(wmem, format, lgK, fiCol);
    assertEquals(getNumCoupons(wmem), numCoupons);
    assertEquals(getNumSV(wmem), numSv);
    assertEquals(getCsvLength(wmem), csvLength);
    assertEquals(getCwLength(wmem), cwLength);
    assertEquals(getKxP(wmem), kxp);
    assertEquals(getHipAccum(wmem), hipAccum);
  }

  @Test
  public void checkStreamErrors() {
    WritableMemory wmem = WritableMemory.allocate(4 * 10);
    putEmpty(wmem, (byte) 12, defaultSeedHash);
    try { getCsvStreamOffset(wmem); fail(); } catch (SketchesArgumentException e) { }
    wmem.putByte(5, (byte) (7 << 2));
    try { getCsvStreamOffset(wmem); fail(); } catch (SketchesStateException e) { }
    wmem.putByte(5, (byte) 0);
    try { getCwStreamOffset(wmem); fail(); } catch (SketchesArgumentException e) { }
    wmem.putByte(5, (byte) (7 << 2));
    try { getCwStreamOffset(wmem); fail(); } catch (SketchesStateException e) { }
  }

  @Test
  public void checkEmptyMemory() {
    WritableMemory wmem = WritableMemory.allocate(4 * 10);
    wmem.putByte(2, (byte) 16); //legal Family
    wmem.putByte(5, (byte) (1 << 2)); //select NONE
    println(PreambleUtil.toString(wmem, false));
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkFieldError() {
    PreambleUtil.fieldError(Format.EMPTY, HiField.NUM_COUPONS);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkCapacity() {
    PreambleUtil.checkCapacity(100, 101);
  }

  @Test(expectedExceptions = SketchesStateException.class)
  public void checkHiFieldError() {
    PreambleUtil.getHiFieldOffset(Format.EMPTY, HiField.NUM_COUPONS);
  }

  @Test
  public void checkWindowOffset() {
    long offset = CpcSketch.determineCorrectOffset(4, 54);
    assertEquals(offset, 1L);
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
