/*
 * Copyright 2016-17, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.sampling;

import static com.yahoo.sketches.sampling.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.sampling.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.sampling.PreambleUtil.RESERVOIR_SIZE_INT;
import static com.yahoo.sketches.sampling.PreambleUtil.RESERVOIR_SIZE_SHORT;
import static com.yahoo.sketches.sampling.PreambleUtil.SER_VER_BYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;

public class ReservoirLongsUnionTest {
  @Test
  public void checkEmptyUnion() {
    ReservoirLongsUnion rlu = ReservoirLongsUnion.getInstance(1024);
    byte[] unionBytes = rlu.toByteArray();

    // will intentionally break if changing empty union serialization
    assertEquals(unionBytes.length, 8);

    println(rlu.toString());
  }

  @Test
  public void checkInstantiation() {
    int n = 100;
    int k = 25;

    // create empty unions
    ReservoirLongsUnion rlu = ReservoirLongsUnion.getInstance(k);
    assertNull(rlu.getResult());
    rlu.update(5);
    assertNotNull(rlu.getResult());

    // pass in a sketch, as both an object and memory
    ReservoirLongsSketch rls = ReservoirLongsSketch.getInstance(k);
    for (int i = 0; i < n; ++i) {
      rls.update(i);
    }

    rlu = ReservoirLongsUnion.getInstance(rls.getK());
    rlu.update(rls);
    assertNotNull(rlu.getResult());

    byte[] sketchBytes = rls.toByteArray();
    Memory mem = new NativeMemory(sketchBytes);
    rlu = ReservoirLongsUnion.getInstance(rls.getK());
    rlu.update(mem);
    assertNotNull(rlu.getResult());

    println(rlu.toString());
  }

  @Test
  public void checkNullUpdate() {
    ReservoirLongsUnion rlu = ReservoirLongsUnion.getInstance(1024);
    assertNull(rlu.getResult());

    // null sketch
    rlu.update((ReservoirLongsSketch) null);
    assertNull(rlu.getResult());

    // null memory
    rlu.update((Memory) null);
    assertNull(rlu.getResult());

    // valid input
    rlu.update(5L);
    assertNotNull(rlu.getResult());
  }

  @Test
  public void checkSerialization() {
    int n = 100;
    int k = 25;

    ReservoirLongsUnion rlu = ReservoirLongsUnion.getInstance(k);
    for (int i = 0; i < n; ++i) {
      rlu.update(i);
    }

    byte[] unionBytes = rlu.toByteArray();
    Memory mem = new NativeMemory(unionBytes);

    ReservoirLongsUnion rebuiltUnion = ReservoirLongsUnion.getInstance(mem);
    validateUnionEquality(rlu, rebuiltUnion);
  }

  @Test
  public void checkVersionConversionWithEmptyGadget() {
    int k = 32768;
    short encK = ReservoirSize.computeSize(k);

    ReservoirLongsUnion rlu = ReservoirLongsUnion.getInstance(k);
    byte[] unionBytesOrig = rlu.toByteArray();

    // get a new byte[], manually revert to v1, then reconstruct
    byte[] unionBytes = rlu.toByteArray();
    Memory unionMem = new NativeMemory(unionBytes);

    unionMem.putByte(SER_VER_BYTE, (byte) 1);
    unionMem.putInt(RESERVOIR_SIZE_INT, 0); // zero out all 4 bytes
    unionMem.putShort(RESERVOIR_SIZE_SHORT, encK);

    ReservoirLongsUnion rebuilt = ReservoirLongsUnion.getInstance(unionMem);
    byte[] rebuiltBytes = rebuilt.toByteArray();

    assertEquals(unionBytesOrig.length, rebuiltBytes.length);
    for (int i = 0; i < unionBytesOrig.length; ++i) {
      assertEquals(unionBytesOrig[i], rebuiltBytes[i]);
    }
  }

  @Test
  public void checkVersionConversionWithGadget() {
    long n = 32;
    int k = 256;
    short encK = ReservoirSize.computeSize(k);

    ReservoirLongsUnion rlu = ReservoirLongsUnion.getInstance(k);
    for (long i = 0; i < n; ++i) {
      rlu.update(i);
    }
    byte[] unionBytesOrig = rlu.toByteArray();

    // get a new byte[], manually revert to v1, then reconstruct
    byte[] unionBytes = rlu.toByteArray();
    Memory unionMem = new NativeMemory(unionBytes);

    unionMem.putByte(SER_VER_BYTE, (byte) 1);
    unionMem.putInt(RESERVOIR_SIZE_INT, 0); // zero out all 4 bytes
    unionMem.putShort(RESERVOIR_SIZE_SHORT, encK);

    // force gadget header to v1, too
    int offset = Family.RESERVOIR_UNION.getMaxPreLongs() << 3;
    unionMem.putByte(offset + SER_VER_BYTE, (byte) 1);
    unionMem.putInt(offset + RESERVOIR_SIZE_INT, 0); // zero out all 4 bytes
    unionMem.putShort(offset + RESERVOIR_SIZE_SHORT, encK);

    ReservoirLongsUnion rebuilt = ReservoirLongsUnion.getInstance(unionMem);
    byte[] rebuiltBytes = rebuilt.toByteArray();

    assertEquals(unionBytesOrig.length, rebuiltBytes.length);
    for (int i = 0; i < unionBytesOrig.length; ++i) {
      assertEquals(unionBytesOrig[i], rebuiltBytes[i]);
    }
  }

  //@SuppressWarnings("null") // this is the point of the test
  @Test(expectedExceptions = java.lang.NullPointerException.class)
  public void checkNullMemoryInstantiation() {
    ReservoirLongsUnion.getInstance(null);
  }

  @Test
  public void checkDownsampledUpdate() {
    int bigK = 1024;
    int smallK = 256;
    int n = 2048;
    ReservoirLongsSketch sketch1 = getBasicSketch(n, smallK);
    ReservoirLongsSketch sketch2 = getBasicSketch(n, bigK);

    ReservoirLongsUnion rlu = ReservoirLongsUnion.getInstance(smallK);
    assertEquals(rlu.getMaxK(), smallK);

    rlu.update(sketch1);
    assertEquals(rlu.getResult().getK(), smallK);

    rlu.update(sketch2);
    assertEquals(rlu.getResult().getK(), smallK);
    assertEquals(rlu.getResult().getNumSamples(), smallK);
  }

  @Test
  public void checkStandardMergeNoCopy() {
    int k = 1024;
    int n1 = 256;
    int n2 = 256;
    ReservoirLongsSketch sketch1 = getBasicSketch(n1, k);
    ReservoirLongsSketch sketch2 = getBasicSketch(n2, k);

    ReservoirLongsUnion rlu = ReservoirLongsUnion.getInstance(k);
    rlu.update(sketch1);
    rlu.update(sketch2);

    assertEquals(rlu.getResult().getK(), k);
    assertEquals(rlu.getResult().getN(), n1 + n2);
    assertEquals(rlu.getResult().getNumSamples(), n1 + n2);

    // creating from Memory should avoid a copy
    int n3 = 2048;
    ReservoirLongsSketch sketch3 = getBasicSketch(n3, k);
    byte[] sketch3Bytes = sketch3.toByteArray();
    Memory mem = new NativeMemory(sketch3Bytes);
    rlu.update(mem);

    assertEquals(rlu.getResult().getK(), k);
    assertEquals(rlu.getResult().getN(), n1 + n2 + n3);
    assertEquals(rlu.getResult().getNumSamples(), k);
  }

  @Test
  public void checkStandardMergeWithCopy() {
    // this will check the other code route to a standard merge,
    // but will copy sketch2 to be non-destructive.
    int k = 1024;
    int n1 = 768;
    int n2 = 2048;
    ReservoirLongsSketch sketch1 = getBasicSketch(n1, k);
    ReservoirLongsSketch sketch2 = getBasicSketch(n2, k);

    ReservoirLongsUnion rlu = ReservoirLongsUnion.getInstance(k);
    rlu.update(sketch1);
    rlu.update(sketch2);
    rlu.update(10);

    assertEquals(rlu.getResult().getK(), k);
    assertEquals(rlu.getResult().getN(), n1 + n2 + 1);
    assertEquals(rlu.getResult().getNumSamples(), k);
  }

  @Test
  public void checkWeightedMerge() {
    int k = 1024;
    int n1 = 16384;
    int n2 = 2048;
    ReservoirLongsSketch sketch1 = getBasicSketch(n1, k);
    ReservoirLongsSketch sketch2 = getBasicSketch(n2, k);

    ReservoirLongsUnion rlu = ReservoirLongsUnion.getInstance(k);
    rlu.update(sketch1);
    rlu.update(sketch2);

    assertEquals(rlu.getResult().getK(), k);
    assertEquals(rlu.getResult().getN(), n1 + n2);
    assertEquals(rlu.getResult().getNumSamples(), k);

    // now merge into the sketch for updating -- results should match
    rlu = ReservoirLongsUnion.getInstance(k);
    rlu.update(sketch2);
    rlu.update(sketch1);

    assertEquals(rlu.getResult().getK(), k);
    assertEquals(rlu.getResult().getN(), n1 + n2);
    assertEquals(rlu.getResult().getNumSamples(), k);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadPreLongs() {
    ReservoirLongsUnion rlu = ReservoirLongsUnion.getInstance(1024);
    Memory mem = new NativeMemory(rlu.toByteArray());
    mem.putByte(PREAMBLE_LONGS_BYTE, (byte) 0); // corrupt the preLongs count

    ReservoirLongsUnion.getInstance(mem);
    fail();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSerVer() {
    ReservoirLongsUnion rlu = ReservoirLongsUnion.getInstance(1024);
    Memory mem = new NativeMemory(rlu.toByteArray());
    mem.putByte(SER_VER_BYTE, (byte) 0); // corrupt the serialization version

    ReservoirLongsUnion.getInstance(mem);
    fail();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadFamily() {
    ReservoirLongsUnion rlu = ReservoirLongsUnion.getInstance(1024);
    Memory mem = new NativeMemory(rlu.toByteArray());
    mem.putByte(FAMILY_BYTE, (byte) 0); // corrupt the family ID

    ReservoirLongsUnion.getInstance(mem);
    fail();
  }

  private static void validateUnionEquality(final ReservoirLongsUnion rlu1,
                                            final ReservoirLongsUnion rlu2) {
    assertEquals(rlu1.getMaxK(), rlu2.getMaxK());

    ReservoirLongsSketchTest.validateReservoirEquality(rlu1.getResult(), rlu2.getResult());
  }

  private static ReservoirLongsSketch getBasicSketch(final int n, final int k) {
    ReservoirLongsSketch rls = ReservoirLongsSketch.getInstance(k);

    for (int i = 0; i < n; ++i) {
      rls.update(i);
    }

    return rls;
  }

  /**
   * Wrapper around System.out.println() allowing a simple way to disable logging in tests
   *
   * @param msg The message to print
   */
  private static void println(String msg) {
    //System.out.println(msg);
  }
}
