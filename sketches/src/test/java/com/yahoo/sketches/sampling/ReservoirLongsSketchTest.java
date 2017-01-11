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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesException;
import com.yahoo.sketches.SketchesStateException;

public class ReservoirLongsSketchTest {
  private static final double EPS = 1e-8;

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidK() {
    ReservoirLongsSketch.getInstance(0);
    fail();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadPreLongs() {
    Memory mem = getBasicSerializedRLS();
    mem.putByte(PREAMBLE_LONGS_BYTE, (byte) 0); // corrupt the preLongs count

    ReservoirLongsSketch.getInstance(mem);
    fail();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSerVer() {
    Memory mem = getBasicSerializedRLS();
    mem.putByte(SER_VER_BYTE, (byte) 0); // corrupt the serialization version

    ReservoirLongsSketch.getInstance(mem);
    fail();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadFamily() {
    Memory mem = getBasicSerializedRLS();
    mem.putByte(FAMILY_BYTE, (byte) 0); // corrupt the family ID

    ReservoirLongsSketch.getInstance(mem);
    fail();
  }

  @Test
  public void checkEmptySketch() {
    ReservoirLongsSketch rls = ReservoirLongsSketch.getInstance(5);
    assertTrue(rls.getSamples() == null);

    byte[] sketchBytes = rls.toByteArray();
    Memory mem = new NativeMemory(sketchBytes);

    // only minPreLongs bytes and should deserialize to empty
    assertEquals(sketchBytes.length, Family.RESERVOIR.getMinPreLongs() << 3);
    ReservoirLongsSketch loadedRls = ReservoirLongsSketch.getInstance(mem);
    assertEquals(loadedRls.getNumSamples(), 0);

    println("Empty sketch:");
    println(rls.toString());
  }

  @Test
  public void checkUnderFullReservoir() {
    int k = 128;
    int n = 64;

    ReservoirLongsSketch rls = ReservoirLongsSketch.getInstance(k);

    for (int i = 0; i < n; ++i) {
      rls.update(i);
    }
    assertEquals(rls.getNumSamples(), n);

    long[] data = rls.getSamples();
    assertEquals(rls.getNumSamples(), rls.getN());
    assertEquals(data.length, n);

    // items in submit order until reservoir at capacity so check
    for (int i = 0; i < n; ++i) {
      assertEquals(data[i], i);
    }

    validateSerializeAndDeserialize(rls);
  }

  @Test
  public void checkFullReservoir() {
    int k = 1000;
    int n = 2000;

    // specify smaller ResizeFactor to ensure multiple resizes
    ReservoirLongsSketch rls = ReservoirLongsSketch.getInstance(k, ResizeFactor.X2);

    for (int i = 0; i < n; ++i) {
      rls.update(i);
    }
    assertEquals(rls.getNumSamples(), rls.getK());

    validateSerializeAndDeserialize(rls);

    println("Full reservoir:");
    println(rls.toString());
  }

  @Test
  public void checkDownsampledCopy() {
    int k = 256;
    int tgtK = 64;

    ReservoirLongsSketch rls = ReservoirLongsSketch.getInstance(k);

    // check status at 3 points:
    // 1. n < encTgtK
    // 2. encTgtK < n < k
    // 3. n > k

    int i;
    for (i = 0; i < tgtK - 1; ++i) {
      rls.update(i);
    }

    ReservoirLongsSketch dsCopy = rls.downsampledCopy(tgtK);
    assertEquals(dsCopy.getK(), tgtK);

    // should be identical other than value of k, which isn't checked here
    validateReservoirEquality(rls, dsCopy);

    // check condition 2 next
    for (; i < k - 1; ++i) {
      rls.update(i);
    }
    assertEquals(rls.getN(), k - 1);

    dsCopy = rls.downsampledCopy(tgtK);
    assertEquals(dsCopy.getN(), rls.getN());
    assertEquals(dsCopy.getNumSamples(), tgtK);

    // and now condition 3
    for (; i < 2 * k; ++i) {
      rls.update(i);
    }
    assertEquals(rls.getN(), 2 * k);

    dsCopy = rls.downsampledCopy(tgtK);
    assertEquals(dsCopy.getN(), rls.getN());
    assertEquals(dsCopy.getNumSamples(), tgtK);
  }

  @Test
  public void checkBadConstructorArgs() {
    long[] data = new long[128];
    for (int i = 0; i < 128; ++i) {
      data[i] = i;
    }

    ResizeFactor rf = ResizeFactor.X8;

    // no data
    try {
      ReservoirLongsSketch.getInstance(null, 128, rf, 128);
      fail();
    } catch (SketchesException e) {
      assertTrue(e.getMessage().contains("null reservoir"));
    }

    // size too small
    try {
      ReservoirLongsSketch.getInstance(data, 128, rf, 1);
      fail();
    } catch (SketchesException e) {
      assertTrue(e.getMessage().contains("size less than 2"));
    }

    // configured reservoir size smaller than data length
    try {
      ReservoirLongsSketch.getInstance(data, 128, rf, 64);
      fail();
    } catch (SketchesException e) {
      assertTrue(e.getMessage().contains("max size less than array length"));
    }

    // too many items seen vs data length, full sketch
    try {
      ReservoirLongsSketch.getInstance(data, 512, rf, 256);
      fail();
    } catch (SketchesException e) {
      assertTrue(e.getMessage().contains("too few samples"));
    }

    // too many items seen vs data length, under-full sketch
    try {
      ReservoirLongsSketch.getInstance(data, 256, rf, 256);
      fail();
    } catch (SketchesException e) {
      assertTrue(e.getMessage().contains("too few samples"));
    }
  }

  @Test
  public void checkSketchCapacity() {
    long[] data = new long[64];
    long itemsSeen = (1L << 48) - 2;

    ReservoirLongsSketch rls = ReservoirLongsSketch.getInstance(data, itemsSeen,
            ResizeFactor.X8, data.length);

    // this should work, the next should fail
    rls.update(0);

    try {
      rls.update(0);
      fail();
    } catch (SketchesStateException e) {
      assertTrue(e.getMessage().contains("Sketch has exceeded capacity for total items seen"));
    }
  }

  @Test
  public void checkSampleWeight() {
    int k = 32;
    ReservoirLongsSketch rls = ReservoirLongsSketch.getInstance(k);

    for (int i = 0; i < (k / 2); ++i) {
      rls.update(i);
    }
    assertEquals(rls.getImplicitSampleWeight(), 1.0); // should be exact value here

    // will have 3k/2 total samples when done
    for (int i = 0; i < k; ++i) {
      rls.update(i);
    }
    assertTrue(Math.abs(rls.getImplicitSampleWeight() - 1.5) < EPS);
  }

  @Test
  public void checkVersionConversion() {
    // version change from 1 to 2 only impact first preamble long, so empty sketch is sufficient
    int k = 32768;
    short encK = ReservoirSize.computeSize(k);

    ReservoirLongsSketch rls = ReservoirLongsSketch.getInstance(k);
    byte[] sketchBytesOrig = rls.toByteArray();

    // get a new byte[], manually revert to v1, then reconstruct
    byte[] sketchBytes = rls.toByteArray();
    Memory sketchMem = new NativeMemory(sketchBytes);

    sketchMem.putByte(SER_VER_BYTE, (byte) 1);
    sketchMem.putInt(RESERVOIR_SIZE_INT, 0); // zero out all 4 bytes
    sketchMem.putShort(RESERVOIR_SIZE_SHORT, encK);

    ReservoirLongsSketch rebuilt = ReservoirLongsSketch.getInstance(sketchMem);
    byte[] rebuiltBytes = rebuilt.toByteArray();

    assertEquals(sketchBytesOrig.length, rebuiltBytes.length);
    for (int i = 0; i < sketchBytesOrig.length; ++i) {
      assertEquals(sketchBytesOrig[i], rebuiltBytes[i]);
    }
  }

  @Test
  public void checkSetAndGetValue() {
    int k = 20;
    int tgtIdx = 5;
    ReservoirLongsSketch rls = ReservoirLongsSketch.getInstance(k);
    for (int i = 0; i < k; ++i) {
      rls.update(i);
    }

    assertEquals(rls.getValueAtPosition(tgtIdx), tgtIdx);
    rls.insertValueAtPosition(-1, tgtIdx);
    assertEquals(rls.getValueAtPosition(tgtIdx), -1);
  }

  @Test
  public void checkBadSetAndGetValue() {
    int k = 20;
    int tgtIdx = 5;
    ReservoirLongsSketch rls = ReservoirLongsSketch.getInstance(k);

    try {
      rls.getValueAtPosition(0);
      fail();
    } catch (SketchesArgumentException e) {
      // expected
    }

    for (int i = 0; i < k; ++i) {
      rls.update(i);
    }
    assertEquals(rls.getValueAtPosition(tgtIdx), tgtIdx);

    try {
      rls.insertValueAtPosition(-1, -1);
      fail();
    } catch (SketchesArgumentException e) {
      // expected
    }

    try {
      rls.insertValueAtPosition(-1, k + 1);
      fail();
    } catch (SketchesArgumentException e) {
      // expected
    }

    try {
      rls.getValueAtPosition(-1);
      fail();
    } catch (SketchesArgumentException e) {
      // expected
    }

    try {
      rls.getValueAtPosition(k + 1);
      fail();
    } catch (SketchesArgumentException e) {
      // expected
    }
  }

  @Test
  public void checkForceIncrement() {
    int k = 100;
    ReservoirLongsSketch rls = ReservoirLongsSketch.getInstance(k);

    for (int i = 0; i < 2 * k; ++i) {
      rls.update(i);
    }

    assertEquals(rls.getN(), 2 * k);
    rls.forceIncrementItemsSeen(k);
    assertEquals(rls.getN(), 3 * k);

    try {
      rls.forceIncrementItemsSeen((1L << 48) - 1);
      fail();
    } catch (SketchesStateException e) {
      // expected
    }
  }

  static Memory getBasicSerializedRLS() {
    int k = 10;
    int n = 20;

    ReservoirLongsSketch rls = ReservoirLongsSketch.getInstance(k);
    assertEquals(rls.getNumSamples(), 0);

    for (int i = 0; i < n; ++i) {
      rls.update(i);
    }
    assertEquals(rls.getNumSamples(), Math.min(n, k));
    assertEquals(rls.getN(), n);
    assertEquals(rls.getK(), k);

    byte[] sketchBytes = rls.toByteArray();
    return new NativeMemory(sketchBytes);
  }

  static void validateSerializeAndDeserialize(ReservoirLongsSketch rls) {
    byte[] sketchBytes = rls.toByteArray();
    assertEquals(sketchBytes.length,
            (Family.RESERVOIR.getMaxPreLongs() + rls.getNumSamples()) << 3);

    // ensure full reservoir rebuilds correctly
    Memory mem = new NativeMemory(sketchBytes);
    ReservoirLongsSketch loadedRls = ReservoirLongsSketch.getInstance(mem);

    validateReservoirEquality(rls, loadedRls);
  }

  static void validateReservoirEquality(ReservoirLongsSketch rls1, ReservoirLongsSketch rls2) {
    assertEquals(rls1.getNumSamples(), rls2.getNumSamples());

    if (rls1.getNumSamples() == 0) {
      return;
    }

    long[] samples1 = rls1.getSamples();
    long[] samples2 = rls2.getSamples();
    assertEquals(samples1.length, samples2.length);

    for (int i = 0; i < samples1.length; ++i) {
      assertEquals(samples1[i], samples2[i]);
    }
  }

  static String printBytesAsLongs(byte[] byteArr) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < byteArr.length; i += 8) {
      for (int j = i + 7; j >= i; --j) {
        String str = Integer.toHexString(byteArr[j] & 0XFF);
        sb.append(com.yahoo.sketches.Util.zeroPad(str, 2));
      }
      sb.append(com.yahoo.sketches.Util.LS);

    }

    return sb.toString();
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
