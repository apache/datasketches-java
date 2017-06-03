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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.ArrayList;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.ArrayOfDoublesSerDe;
import com.yahoo.sketches.ArrayOfLongsSerDe;
import com.yahoo.sketches.ArrayOfNumbersSerDe;
import com.yahoo.sketches.ArrayOfStringsSerDe;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;

// Tests mostly focus on Long since other types are already tested in ReservoirItemsSketchTest.
public class ReservoirItemsUnionTest {
  @Test
  public void checkEmptyUnion() {
    final ReservoirItemsUnion<Long> riu = ReservoirItemsUnion.newInstance(1024);
    final byte[] unionBytes = riu.toByteArray(new ArrayOfLongsSerDe());

    // will intentionally break if changing empty union serialization
    assertEquals(unionBytes.length, 8);

    println(riu.toString());
  }

  @Test
  public void checkInstantiation() {
    final int n = 100;
    final int k = 25;

    // create empty unions
    ReservoirItemsUnion<Long> riu = ReservoirItemsUnion.newInstance(k);
    assertNull(riu.getResult());
    riu.update(5L);
    assertNotNull(riu.getResult());

    // pass in a sketch, as both an object and memory
    final ReservoirItemsSketch<Long> ris = ReservoirItemsSketch.newInstance(k);
    for (long i = 0; i < n; ++i) {
      ris.update(i);
    }

    riu.reset();
    assertEquals(riu.getResult().getN(), 0);
    riu.update(ris);
    assertEquals(riu.getResult().getN(), ris.getN());

    final ArrayOfLongsSerDe serDe = new ArrayOfLongsSerDe();
    final byte[] sketchBytes = ris.toByteArray(serDe); // only the gadget is serialized
    final Memory mem = Memory.wrap(sketchBytes);
    riu = ReservoirItemsUnion.newInstance(ris.getK());
    riu.update(mem, serDe);
    assertNotNull(riu.getResult());

    println(riu.toString());
  }

  /*
  @Test
  public void checkReadOnlyInstantiation() {
    final int k = 100;
    final ReservoirItemsUnion<Long> union = ReservoirItemsUnion.newInstance(k);
    for (long i = 0; i < 2 * k; ++i) {
      union.update(i);
    }

    final byte[] unionBytes = union.toByteArray(new ArrayOfLongsSerDe());
    final Memory mem = Memory.wrap(unionBytes);

    final ReservoirItemsUnion<Long> riu;
    riu = ReservoirItemsUnion.heapify(mem, new ArrayOfLongsSerDe());

    assertNotNull(riu);
    assertEquals(riu.getMaxK(), k);
    ReservoirItemsSketchTest.validateReservoirEquality(riu.getResult(), union.getResult());
  }
  */

  @Test
  public void checkNullUpdate() {
    final ReservoirItemsUnion<Long> riu = ReservoirItemsUnion.newInstance(1024);
    assertNull(riu.getResult());

    // null sketch
    final ReservoirItemsSketch<Long> nullSketch = null;
    riu.update(nullSketch);
    assertNull(riu.getResult());

    // null memory
    riu.update(null, new ArrayOfLongsSerDe());
    assertNull(riu.getResult());

    // null item
    riu.update((Long) null);
    assertNull(riu.getResult());

    // valid input
    riu.update(5L);
    assertNotNull(riu.getResult());
  }

  @Test
  public void checkSerialization() {
    final int n = 100;
    final int k = 25;

    final ReservoirItemsUnion<Long> riu = ReservoirItemsUnion.newInstance(k);
    for (long i = 0; i < n; ++i) {
      riu.update(i);
    }

    final ArrayOfLongsSerDe serDe = new ArrayOfLongsSerDe();
    final byte[] unionBytes = riu.toByteArray(serDe);
    final Memory mem = Memory.wrap(unionBytes);
    println(PreambleUtil.preambleToString(mem));

    final ReservoirItemsUnion<Long> rebuiltUnion = ReservoirItemsUnion.heapify(mem, serDe);
    assertEquals(riu.getMaxK(), rebuiltUnion.getMaxK());
    ReservoirItemsSketchTest.validateReservoirEquality(riu.getResult(), rebuiltUnion.getResult());
  }

  @Test
  public void checkVersionConversionWithEmptyGadget() {
    final int k = 32768;
    final short encK = ReservoirSize.computeSize(k);
    final ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();

    final ReservoirItemsUnion<String> riu = ReservoirItemsUnion.newInstance(k);
    final byte[] unionBytesOrig = riu.toByteArray(serDe);

    // get a new byte[], manually revert to v1, then reconstruct
    final byte[] unionBytes = riu.toByteArray(serDe);
    final WritableMemory unionMem = WritableMemory.wrap(unionBytes);

    unionMem.putByte(SER_VER_BYTE, (byte) 1);
    unionMem.putInt(RESERVOIR_SIZE_INT, 0); // zero out all 4 bytes
    unionMem.putShort(RESERVOIR_SIZE_SHORT, encK);
    println(PreambleUtil.preambleToString(unionMem));

    final ReservoirItemsUnion<String> rebuilt = ReservoirItemsUnion.heapify(unionMem, serDe);
    final byte[] rebuiltBytes = rebuilt.toByteArray(serDe);

    assertEquals(unionBytesOrig.length, rebuiltBytes.length);
    for (int i = 0; i < unionBytesOrig.length; ++i) {
      assertEquals(unionBytesOrig[i], rebuiltBytes[i]);
    }
  }

  @Test
  public void checkVersionConversionWithGadget() {
    final long n = 32;
    final int k = 256;
    final short encK = ReservoirSize.computeSize(k);
    final ArrayOfNumbersSerDe serDe = new ArrayOfNumbersSerDe();

    final ReservoirItemsUnion<Number> rlu = ReservoirItemsUnion.newInstance(k);
    for (long i = 0; i < n; ++i) {
      rlu.update(i);
    }
    final byte[] unionBytesOrig = rlu.toByteArray(serDe);

    // get a new byte[], manually revert to v1, then reconstruct
    final byte[] unionBytes = rlu.toByteArray(serDe);
    final WritableMemory unionMem = WritableMemory.wrap(unionBytes);

    unionMem.putByte(SER_VER_BYTE, (byte) 1);
    unionMem.putInt(RESERVOIR_SIZE_INT, 0); // zero out all 4 bytes
    unionMem.putShort(RESERVOIR_SIZE_SHORT, encK);

    // force gadget header to v1, too
    final int offset = Family.RESERVOIR_UNION.getMaxPreLongs() << 3;
    unionMem.putByte(offset + SER_VER_BYTE, (byte) 1);
    unionMem.putInt(offset + RESERVOIR_SIZE_INT, 0); // zero out all 4 bytes
    unionMem.putShort(offset + RESERVOIR_SIZE_SHORT, encK);

    final ReservoirItemsUnion<Number> rebuilt = ReservoirItemsUnion.heapify(unionMem, serDe);
    final byte[] rebuiltBytes = rebuilt.toByteArray(serDe);

    assertEquals(unionBytesOrig.length, rebuiltBytes.length);
    for (int i = 0; i < unionBytesOrig.length; ++i) {
      assertEquals(unionBytesOrig[i], rebuiltBytes[i]);
    }
  }

  //@SuppressWarnings("null") // this is the point of the test
  @Test(expectedExceptions = NullPointerException.class)
  public void checkNullMemoryInstantiation() {
    ReservoirItemsUnion.heapify(null, new ArrayOfStringsSerDe());
  }

  @Test
  public void checkDownsampledUpdate() {
    final int bigK = 1024;
    final int smallK = 256;
    final int n = 2048;
    final ReservoirItemsSketch<Long> sketch1 = getBasicSketch(n, smallK);
    final ReservoirItemsSketch<Long> sketch2 = getBasicSketch(2 * n, bigK);

    final ReservoirItemsUnion<Long> riu = ReservoirItemsUnion.newInstance(smallK);
    assertEquals(riu.getMaxK(), smallK);

    riu.update(sketch1);
    assertNotNull(riu.getResult());
    assertEquals(riu.getResult().getK(), smallK);

    riu.update(sketch2);
    assertEquals(riu.getResult().getK(), smallK);
    assertEquals(riu.getResult().getNumSamples(), smallK);
  }

  @Test
  public void checkUnionResetWithInitialSmallK() {
    final int maxK = 25;
    final int sketchK = 10;
    final ReservoirItemsUnion<Long> riu = ReservoirItemsUnion.newInstance(maxK);

    ReservoirItemsSketch<Long> ris = getBasicSketch(2 * sketchK, sketchK); // in sampling mode
    riu.update(ris);
    assertEquals(riu.getMaxK(), maxK);
    assertNotNull(riu.getResult());
    assertEquals(riu.getResult().getK(), sketchK);

    riu.reset();
    assertNotNull(riu.getResult());

    // feed in sketch in sampling mode, with larger k than old gadget
    ris = getBasicSketch(2 * maxK, maxK + 1);
    riu.update(ris);
    assertEquals(riu.getMaxK(), maxK);
    assertNotNull(riu.getResult());
    assertEquals(riu.getResult().getK(), maxK);
  }

  @Test
  public void checkNewGadget() {
    final int maxK = 1024;
    final int bigK = 1536;
    final int smallK = 128;

    // downsample input sketch, use as gadget (exact mode, but irrelevant here)
    final ReservoirItemsSketch<Long> bigKSketch = getBasicSketch(maxK / 2, bigK);
    final byte[] bigKBytes = bigKSketch.toByteArray(new ArrayOfLongsSerDe());
    final Memory bigKMem = Memory.wrap(bigKBytes);

    ReservoirItemsUnion<Long> riu = ReservoirItemsUnion.newInstance(maxK);
    riu.update(bigKMem, new ArrayOfLongsSerDe());
    assertNotNull(riu.getResult());
    assertEquals(riu.getResult().getK(), maxK);
    assertEquals(riu.getResult().getN(), maxK / 2);

    // sketch k < maxK but in sampling mode
    final ReservoirItemsSketch<Long> smallKSketch = getBasicSketch(maxK, smallK);
    final byte[] smallKBytes = smallKSketch.toByteArray(new ArrayOfLongsSerDe());
    final Memory smallKMem = Memory.wrap(smallKBytes);

    riu = ReservoirItemsUnion.newInstance(maxK);
    riu.update(smallKMem, new ArrayOfLongsSerDe());
    assertNotNull(riu.getResult());
    assertTrue(riu.getResult().getK() < maxK);
    assertEquals(riu.getResult().getK(), smallK);
    assertEquals(riu.getResult().getN(), maxK);

    // sketch k < maxK and in exact mode
    final ReservoirItemsSketch<Long> smallKExactSketch = getBasicSketch(smallK, smallK);
    final byte[] smallKExactBytes = smallKExactSketch.toByteArray(new ArrayOfLongsSerDe());
    final Memory smallKExactMem = Memory.wrap(smallKExactBytes);

    riu = ReservoirItemsUnion.newInstance(maxK);
    riu.update(smallKExactMem, new ArrayOfLongsSerDe());
    assertNotNull(riu.getResult());
    assertEquals(riu.getResult().getK(), maxK);
    assertEquals(riu.getResult().getN(), smallK);
  }

  @Test
  public void checkListInputUpdate() {
    final int k = 32;
    final int n = 64;
    final ReservoirItemsUnion<Integer> riu = ReservoirItemsUnion.newInstance(k);

    ArrayList<Integer> data = new ArrayList<>(k);
    for (int i = 0; i < k; ++i) {
      data.add(i);
    }
    riu.update(n, k, data);
    assertNotNull(riu.getResult());
    assertEquals(riu.getResult().getN(), n);
    assertEquals(riu.getResult().getK(), k); // power of 2, so exact

    data = new ArrayList<>(2 * k);
    for (int i = 0; i < 2 * k; ++i) {
      data.add(i);
    }
    riu.update(10 * n, 2 * k, data);
    assertEquals(riu.getResult().getN(), 11 * n); // total = n + 10n
    assertEquals(riu.getResult().getK(), k); // should have downsampled the 2nd
  }

  @Test
  public void checkStandardMergeNoCopy() {
    final int k = 1024;
    final int n1 = 256;
    final int n2 = 256;
    final ReservoirItemsSketch<Long> sketch1 = getBasicSketch(n1, k);
    final ReservoirItemsSketch<Long> sketch2 = getBasicSketch(n2, k);

    final ReservoirItemsUnion<Long> riu = ReservoirItemsUnion.newInstance(k);
    riu.update(sketch1);
    riu.update(sketch2);

    assertNotNull(riu.getResult());
    assertEquals(riu.getResult().getK(), k);
    assertEquals(riu.getResult().getN(), n1 + n2);
    assertEquals(riu.getResult().getNumSamples(), n1 + n2);

    // creating from Memory should avoid a copy
    final int n3 = 2048;
    final ArrayOfLongsSerDe serDe = new ArrayOfLongsSerDe();
    final ReservoirItemsSketch<Long> sketch3 = getBasicSketch(n3, k);
    final byte[] sketch3Bytes = sketch3.toByteArray(serDe);
    final Memory mem = Memory.wrap(sketch3Bytes);
    riu.update(mem, serDe);

    assertEquals(riu.getResult().getK(), k);
    assertEquals(riu.getResult().getN(), n1 + n2 + n3);
    assertEquals(riu.getResult().getNumSamples(), k);
  }

  @Test
  public void checkStandardMergeWithCopy() {
    // this will check the other code route to a standard merge,
    // but will copy sketch2 to be non-destructive.
    final int k = 1024;
    final int n1 = 768;
    final int n2 = 2048;
    final ReservoirItemsSketch<Long> sketch1 = getBasicSketch(n1, k);
    final ReservoirItemsSketch<Long> sketch2 = getBasicSketch(n2, k);

    final ReservoirItemsUnion<Long> riu = ReservoirItemsUnion.newInstance(k);
    riu.update(sketch1);
    riu.update(sketch2);
    riu.update(10L);

    assertNotNull(riu.getResult());
    assertEquals(riu.getResult().getK(), k);
    assertEquals(riu.getResult().getN(), n1 + n2 + 1);
    assertEquals(riu.getResult().getNumSamples(), k);
  }

  @Test
  public void checkWeightedMerge() {
    final int k = 1024;
    final int n1 = 16384;
    final int n2 = 2048;
    final ReservoirItemsSketch<Long> sketch1 = getBasicSketch(n1, k);
    final ReservoirItemsSketch<Long> sketch2 = getBasicSketch(n2, k);

    ReservoirItemsUnion<Long> riu = ReservoirItemsUnion.newInstance(k);
    riu.update(sketch1);
    riu.update(sketch2);

    assertNotNull(riu.getResult());
    assertEquals(riu.getResult().getK(), k);
    assertEquals(riu.getResult().getN(), n1 + n2);
    assertEquals(riu.getResult().getNumSamples(), k);

    // now merge into the sketch for updating -- results should match
    riu = ReservoirItemsUnion.newInstance(k);
    riu.update(sketch2);
    riu.update(sketch1);

    assertNotNull(riu.getResult());
    assertEquals(riu.getResult().getK(), k);
    assertEquals(riu.getResult().getN(), n1 + n2);
    assertEquals(riu.getResult().getNumSamples(), k);
  }

  @Test
  public void checkPolymorphicType() {
    final int k = 4;

    final ReservoirItemsUnion<Number> riu = ReservoirItemsUnion.newInstance(k);
    riu.update(2.2);
    riu.update(6L);

    final ReservoirItemsSketch<Number> ris = ReservoirItemsSketch.newInstance(k);
    ris.update(1);
    ris.update(3.7f);

    riu.update(ris);

    final ArrayOfNumbersSerDe serDe = new ArrayOfNumbersSerDe();
    final byte[] sketchBytes = riu.toByteArray(serDe, Number.class);
    final Memory mem = Memory.wrap(sketchBytes);

    final ReservoirItemsUnion<Number> rebuiltRiu = ReservoirItemsUnion.heapify(mem, serDe);

    // validateReservoirEquality can't handle abstract base class
    assertNotNull(riu.getResult());
    assertNotNull(rebuiltRiu.getResult());
    assertEquals(riu.getResult().getNumSamples(), rebuiltRiu.getResult().getNumSamples());

    final Number[] samples1 = riu.getResult().getSamples(Number.class);
    final Number[] samples2 = rebuiltRiu.getResult().getSamples(Number.class);
    assertNotNull(samples1);
    assertNotNull(samples2);
    assertEquals(samples1.length, samples2.length);

    for (int i = 0; i < samples1.length; ++i) {
      assertEquals(samples1[i], samples2[i]);
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadPreLongs() {
    final ReservoirItemsUnion<Number> riu = ReservoirItemsUnion.newInstance(1024);
    final WritableMemory mem = WritableMemory.wrap(riu.toByteArray(new ArrayOfNumbersSerDe()));
    mem.putByte(PREAMBLE_LONGS_BYTE, (byte) 0); // corrupt the preLongs count

    ReservoirItemsUnion.heapify(mem, new ArrayOfNumbersSerDe());
    fail();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSerVer() {
    final ReservoirItemsUnion<String> riu = ReservoirItemsUnion.newInstance(1024);
    final WritableMemory mem = WritableMemory.wrap(riu.toByteArray(new ArrayOfStringsSerDe()));
    mem.putByte(SER_VER_BYTE, (byte) 0); // corrupt the serialization version

    ReservoirItemsUnion.heapify(mem, new ArrayOfStringsSerDe());
    fail();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadFamily() {
    final ReservoirItemsUnion<Double> rlu = ReservoirItemsUnion.newInstance(1024);
    final WritableMemory mem = WritableMemory.wrap(rlu.toByteArray(new ArrayOfDoublesSerDe()));
    mem.putByte(FAMILY_BYTE, (byte) 0); // corrupt the family ID

    ReservoirItemsUnion.heapify(mem, new ArrayOfDoublesSerDe());
    fail();
  }

  private static ReservoirItemsSketch<Long> getBasicSketch(final int n, final int k) {
    final ReservoirItemsSketch<Long> rls = ReservoirItemsSketch.newInstance(k);

    for (long i = 0; i < n; ++i) {
      rls.update(i);
    }

    return rls;
  }

  /**
   * Wrapper around System.out.println() allowing a simple way to disable logging in tests
   *
   * @param msg The message to print
   */
  private static void println(final String msg) {
    //System.out.println(msg);
  }
}
