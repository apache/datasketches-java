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

import java.util.ArrayList;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
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
    ReservoirItemsUnion<Long> riu = ReservoirItemsUnion.getInstance(1024);
    byte[] unionBytes = riu.toByteArray(new ArrayOfLongsSerDe());

    // will intentionally break if changing empty union serialization
    assertEquals(unionBytes.length, 8);

    println(riu.toString());
  }

  @Test
  public void checkInstantiation() {
    int n = 100;
    int k = 25;

    // create empty unions
    ReservoirItemsUnion<Long> riu = ReservoirItemsUnion.getInstance(k);
    assertNull(riu.getResult());
    riu.update(5L);
    assertNotNull(riu.getResult());

    // pass in a sketch, as both an object and memory
    ReservoirItemsSketch<Long> ris = ReservoirItemsSketch.getInstance(k);
    for (long i = 0; i < n; ++i) {
      ris.update(i);
    }

    riu = ReservoirItemsUnion.getInstance(ris.getK());
    riu.update(ris);
    assertNotNull(riu.getResult());

    ArrayOfLongsSerDe serDe = new ArrayOfLongsSerDe();
    byte[] sketchBytes = ris.toByteArray(serDe); // only the gadget is serialized
    Memory mem = new NativeMemory(sketchBytes);
    riu = ReservoirItemsUnion.getInstance(ris.getK());
    riu.update(mem, serDe);
    assertNotNull(riu.getResult());

    println(riu.toString());
  }

  @Test
  public void checkSerialization() {
    int n = 100;
    int k = 25;

    ReservoirItemsUnion<Long> riu = ReservoirItemsUnion.getInstance(k);
    for (long i = 0; i < n; ++i) {
      riu.update(i);
    }

    ArrayOfLongsSerDe serDe = new ArrayOfLongsSerDe();
    byte[] unionBytes = riu.toByteArray(serDe);
    Memory mem = new NativeMemory(unionBytes);

    ReservoirItemsUnion<Long> rebuiltUnion = ReservoirItemsUnion.getInstance(mem, serDe);
    assertEquals(riu.getMaxK(), rebuiltUnion.getMaxK());
    ReservoirItemsSketchTest.validateReservoirEquality(riu.getResult(), rebuiltUnion.getResult());
  }

  @Test
  public void checkVersionConversionWithEmptyGadget() {
    int k = 32768;
    short encK = ReservoirSize.computeSize(k);
    ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();

    ReservoirItemsUnion<String> riu = ReservoirItemsUnion.getInstance(k);
    byte[] unionBytesOrig = riu.toByteArray(serDe);

    // get a new byte[], manually revert to v1, then reconstruct
    byte[] unionBytes = riu.toByteArray(serDe);
    Memory unionMem = new NativeMemory(unionBytes);

    unionMem.putByte(SER_VER_BYTE, (byte) 1);
    unionMem.putInt(RESERVOIR_SIZE_INT, 0); // zero out all 4 bytes
    unionMem.putShort(RESERVOIR_SIZE_SHORT, encK);

    ReservoirItemsUnion<String> rebuilt = ReservoirItemsUnion.getInstance(unionMem, serDe);
    byte[] rebuiltBytes = rebuilt.toByteArray(serDe);

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
    ArrayOfNumbersSerDe serDe = new ArrayOfNumbersSerDe();

    ReservoirItemsUnion<Number> rlu = ReservoirItemsUnion.getInstance(k);
    for (long i = 0; i < n; ++i) {
      rlu.update(i);
    }
    byte[] unionBytesOrig = rlu.toByteArray(serDe);

    // get a new byte[], manually revert to v1, then reconstruct
    byte[] unionBytes = rlu.toByteArray(serDe);
    Memory unionMem = new NativeMemory(unionBytes);

    unionMem.putByte(SER_VER_BYTE, (byte) 1);
    unionMem.putInt(RESERVOIR_SIZE_INT, 0); // zero out all 4 bytes
    unionMem.putShort(RESERVOIR_SIZE_SHORT, encK);

    // force gadget header to v1, too
    int offset = Family.RESERVOIR_UNION.getMaxPreLongs() << 3;
    unionMem.putByte(offset + SER_VER_BYTE, (byte) 1);
    unionMem.putInt(offset + RESERVOIR_SIZE_INT, 0); // zero out all 4 bytes
    unionMem.putShort(offset + RESERVOIR_SIZE_SHORT, encK);

    ReservoirItemsUnion<Number> rebuilt = ReservoirItemsUnion.getInstance(unionMem, serDe);
    byte[] rebuiltBytes = rebuilt.toByteArray(serDe);

    assertEquals(unionBytesOrig.length, rebuiltBytes.length);
    for (int i = 0; i < unionBytesOrig.length; ++i) {
      assertEquals(unionBytesOrig[i], rebuiltBytes[i]);
    }
  }


  @Test
  public void checkNullUpdate() {
    ReservoirItemsUnion<Long> riu = ReservoirItemsUnion.getInstance(1024);
    assertNull(riu.getResult());

    // null sketch
    riu.update((ReservoirItemsSketch<Long>) null);
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

  //@SuppressWarnings("null") // this is the point of the test
  @Test(expectedExceptions = NullPointerException.class)
  public void checkNullMemoryInstantiation() {
    ReservoirItemsUnion.getInstance(null, new ArrayOfStringsSerDe());
  }

  @Test
  public void checkDownsampledUpdate() {
    int bigK = 1024;
    int smallK = 256;
    int n = 2048;
    ReservoirItemsSketch<Long> sketch1 = getBasicSketch(n, smallK);
    ReservoirItemsSketch<Long> sketch2 = getBasicSketch(n, bigK);

    ReservoirItemsUnion<Long> riu = ReservoirItemsUnion.getInstance(smallK);
    assertEquals(riu.getMaxK(), smallK);

    riu.update(sketch1);
    assertEquals(riu.getResult().getK(), smallK);

    riu.update(sketch2);
    assertEquals(riu.getResult().getK(), smallK);
    assertEquals(riu.getResult().getNumSamples(), smallK);
  }

  @Test
  public void checkListInputUpdate() {
    int k = 32;
    int n = 64;
    ReservoirItemsUnion<Integer> riu = ReservoirItemsUnion.getInstance(k);

    ArrayList<Integer> data = new ArrayList<>(k);
    for (int i = 0; i < k; ++i) {
      data.add(i);
    }
    riu.update(n, k, data);
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
    int k = 1024;
    int n1 = 256;
    int n2 = 256;
    ReservoirItemsSketch<Long> sketch1 = getBasicSketch(n1, k);
    ReservoirItemsSketch<Long> sketch2 = getBasicSketch(n2, k);

    ReservoirItemsUnion<Long> riu = ReservoirItemsUnion.getInstance(k);
    riu.update(sketch1);
    riu.update(sketch2);

    assertEquals(riu.getResult().getK(), k);
    assertEquals(riu.getResult().getN(), n1 + n2);
    assertEquals(riu.getResult().getNumSamples(), n1 + n2);

    // creating from Memory should avoid a copy
    int n3 = 2048;
    ArrayOfLongsSerDe serDe = new ArrayOfLongsSerDe();
    ReservoirItemsSketch<Long> sketch3 = getBasicSketch(n3, k);
    byte[] sketch3Bytes = sketch3.toByteArray(serDe);
    Memory mem = new NativeMemory(sketch3Bytes);
    riu.update(mem, serDe);

    assertEquals(riu.getResult().getK(), k);
    assertEquals(riu.getResult().getN(), n1 + n2 + n3);
    assertEquals(riu.getResult().getNumSamples(), k);
  }

  @Test
  public void checkStandardMergeWithCopy() {
    // this will check the other code route to a standard merge,
    // but will copy sketch2 to be non-destructive.
    int k = 1024;
    int n1 = 768;
    int n2 = 2048;
    ReservoirItemsSketch<Long> sketch1 = getBasicSketch(n1, k);
    ReservoirItemsSketch<Long> sketch2 = getBasicSketch(n2, k);

    ReservoirItemsUnion<Long> riu = ReservoirItemsUnion.getInstance(k);
    riu.update(sketch1);
    riu.update(sketch2);
    riu.update(10L);

    assertEquals(riu.getResult().getK(), k);
    assertEquals(riu.getResult().getN(), n1 + n2 + 1);
    assertEquals(riu.getResult().getNumSamples(), k);
  }

  @Test
  public void checkWeightedMerge() {
    int k = 1024;
    int n1 = 16384;
    int n2 = 2048;
    ReservoirItemsSketch<Long> sketch1 = getBasicSketch(n1, k);
    ReservoirItemsSketch<Long> sketch2 = getBasicSketch(n2, k);

    ReservoirItemsUnion<Long> riu = ReservoirItemsUnion.getInstance(k);
    riu.update(sketch1);
    riu.update(sketch2);

    assertEquals(riu.getResult().getK(), k);
    assertEquals(riu.getResult().getN(), n1 + n2);
    assertEquals(riu.getResult().getNumSamples(), k);

    // now merge into the sketch for updating -- results should match
    riu = ReservoirItemsUnion.getInstance(k);
    riu.update(sketch2);
    riu.update(sketch1);

    assertEquals(riu.getResult().getK(), k);
    assertEquals(riu.getResult().getN(), n1 + n2);
    assertEquals(riu.getResult().getNumSamples(), k);

  }

  @Test
  public void checkPolymorphicType() {
    int k = 4;

    ReservoirItemsUnion<Number> rlu = ReservoirItemsUnion.getInstance(k);
    rlu.update(2.2);
    rlu.update(6L);

    ReservoirItemsSketch<Number> rls = ReservoirItemsSketch.getInstance(k);
    rls.update(1);
    rls.update(3.7f);

    rlu.update(rls);

    ArrayOfNumbersSerDe serDe = new ArrayOfNumbersSerDe();
    byte[] sketchBytes = rlu.toByteArray(serDe, Number.class);
    Memory mem = new NativeMemory(sketchBytes);

    ReservoirItemsUnion<Number> rebuiltRlu = ReservoirItemsUnion.getInstance(mem, serDe);

    // validateReservoirEquality can't handle abstract base class
    assertEquals(rlu.getResult().getNumSamples(), rebuiltRlu.getResult().getNumSamples());

    Number[] samples1 = rlu.getResult().getSamples(Number.class);
    Number[] samples2 = rebuiltRlu.getResult().getSamples(Number.class);
    assertEquals(samples1.length, samples2.length);

    for (int i = 0; i < samples1.length; ++i) {
      assertEquals(samples1[i], samples2[i]);
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadPreLongs() {
    ReservoirItemsUnion<Number> riu = ReservoirItemsUnion.getInstance(1024);
    Memory mem = new NativeMemory(riu.toByteArray(new ArrayOfNumbersSerDe()));
    mem.putByte(PREAMBLE_LONGS_BYTE, (byte) 0); // corrupt the preLongs count

    ReservoirItemsUnion.getInstance(mem, new ArrayOfNumbersSerDe());
    fail();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSerVer() {
    ReservoirItemsUnion<String> riu = ReservoirItemsUnion.getInstance(1024);
    Memory mem = new NativeMemory(riu.toByteArray(new ArrayOfStringsSerDe()));
    mem.putByte(SER_VER_BYTE, (byte) 0); // corrupt the serialization version

    ReservoirItemsUnion.getInstance(mem, new ArrayOfStringsSerDe());
    fail();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadFamily() {
    ReservoirItemsUnion<Double> rlu = ReservoirItemsUnion.getInstance(1024);
    Memory mem = new NativeMemory(rlu.toByteArray(new ArrayOfDoublesSerDe()));
    mem.putByte(FAMILY_BYTE, (byte) 0); // corrupt the family ID

    ReservoirItemsUnion.getInstance(mem, new ArrayOfDoublesSerDe());
    fail();
  }

  private static ReservoirItemsSketch<Long> getBasicSketch(final int n, final int k) {
    ReservoirItemsSketch<Long> rls = ReservoirItemsSketch.getInstance(k);

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
  private static void println(String msg) {
    //System.out.println(msg);
  }
}
