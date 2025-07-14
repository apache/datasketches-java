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

package org.apache.datasketches.sampling;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_SHORT_UNALIGNED;
import static org.apache.datasketches.sampling.PreambleUtil.FAMILY_BYTE;
import static org.apache.datasketches.sampling.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static org.apache.datasketches.sampling.PreambleUtil.RESERVOIR_SIZE_INT;
import static org.apache.datasketches.sampling.PreambleUtil.RESERVOIR_SIZE_SHORT;
import static org.apache.datasketches.sampling.PreambleUtil.SER_VER_BYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.util.ArrayList;

import org.apache.datasketches.common.ArrayOfLongsSerDe2;
import org.apache.datasketches.common.ArrayOfNumbersSerDe2;
import org.apache.datasketches.common.ArrayOfStringsSerDe2;
import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesException;
import org.apache.datasketches.common.SketchesStateException;
import org.apache.datasketches.sampling.PreambleUtil;
import org.apache.datasketches.sampling.ReservoirItemsSketch;
import org.apache.datasketches.sampling.ReservoirSize;
import org.apache.datasketches.sampling.SampleSubsetSummary;
import org.testng.annotations.Test;

public class ReservoirItemsSketchTest {
  private static final double EPS = 1e-8;

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidK() {
    ReservoirItemsSketch.<Integer>newInstance(0);
    fail();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSerVer() {
    final MemorySegment seg = getBasicSerializedLongsRIS();
    seg.set(JAVA_BYTE, SER_VER_BYTE, (byte) 0); // corrupt the serialization version

    ReservoirItemsSketch.heapify(seg, new ArrayOfLongsSerDe2());
    fail();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadFamily() {
    final MemorySegment seg = getBasicSerializedLongsRIS();
    seg.set(JAVA_BYTE, FAMILY_BYTE, (byte) Family.ALPHA.getID()); // corrupt the family ID

    try {
      PreambleUtil.preambleToString(seg);
    } catch (final SketchesArgumentException e) {
      assertTrue(e.getMessage().startsWith("Inspecting preamble with Sampling family"));
    }

    ReservoirItemsSketch.heapify(seg, new ArrayOfLongsSerDe2());
    fail();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadPreLongs() {
    final MemorySegment seg = getBasicSerializedLongsRIS();
    seg.set(JAVA_BYTE, PREAMBLE_LONGS_BYTE, (byte) 0); // corrupt the preLongs count

    ReservoirItemsSketch.heapify(seg, new ArrayOfLongsSerDe2());
    fail();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadMemorySegment() {
    byte[] bytes = new byte[4];
    MemorySegment seg = MemorySegment.ofArray(bytes);

    try {
      PreambleUtil.getAndCheckPreLongs(seg);
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }

    bytes = new byte[8];
    bytes[0] = 2; // only 1 preLong worth of items in bytearray
    seg = MemorySegment.ofArray(bytes);
    PreambleUtil.getAndCheckPreLongs(seg);
  }


  @Test
  public void checkEmptySketch() {
    final ReservoirItemsSketch<String> ris = ReservoirItemsSketch.newInstance(5);
    assertTrue(ris.getSamples() == null);

    final byte[] sketchBytes = ris.toByteArray(new ArrayOfStringsSerDe2());
    final MemorySegment seg = MemorySegment.ofArray(sketchBytes);

    // only minPreLongs bytes and should deserialize to empty
    assertEquals(sketchBytes.length, Family.RESERVOIR.getMinPreLongs() << 3);
    final ArrayOfStringsSerDe2 serDe = new ArrayOfStringsSerDe2();
    final ReservoirItemsSketch<String> loadedRis = ReservoirItemsSketch.heapify(seg, serDe);
    assertEquals(loadedRis.getNumSamples(), 0);

    println("Empty sketch:");
    println("  Preamble:");
    println(PreambleUtil.preambleToString(seg));
    println("  Sketch:");
    println(ris.toString());
  }

  @Test
  public void checkUnderFullReservoir() {
    final int k = 128;
    final int n = 64;

    final ReservoirItemsSketch<String> ris = ReservoirItemsSketch.newInstance(k);
    int expectedLength = 0;

    for (int i = 0; i < n; ++i) {
      final String intStr = Integer.toString(i);
      expectedLength += intStr.length() + Integer.BYTES;
      ris.update(intStr);
    }
    assertEquals(ris.getNumSamples(), n);

    final String[] data = ris.getSamples();
    assertNotNull(data);
    assertEquals(ris.getNumSamples(), ris.getN());
    assertEquals(data.length, n);

    // items in submit order until reservoir at capacity so check
    for (int i = 0; i < n; ++i) {
      assertEquals(data[i], Integer.toString(i));
    }

    // not using validateSerializeAndDeserialize() to check with a non-Long
    final ArrayOfStringsSerDe2 serDe = new ArrayOfStringsSerDe2();
    expectedLength += Family.RESERVOIR.getMaxPreLongs() << 3;
    final byte[] sketchBytes = ris.toByteArray(serDe);
    assertEquals(sketchBytes.length, expectedLength);

    // ensure reservoir rebuilds correctly
    final MemorySegment seg = MemorySegment.ofArray(sketchBytes);
    final ReservoirItemsSketch<String> loadedRis = ReservoirItemsSketch.heapify(seg, serDe);

    validateReservoirEquality(ris, loadedRis);

    println("Under-full reservoir:");
    println("  Preamble:");
    println(PreambleUtil.preambleToString(seg));
    println("  Sketch:");
    println(ris.toString());
  }

  @Test
  public void checkFullReservoir() {
    final int k = 1000;
    final int n = 2000;

    // specify smaller ResizeFactor to ensure multiple resizes
    final ReservoirItemsSketch<Long> ris = ReservoirItemsSketch.newInstance(k, ResizeFactor.X2);

    for (int i = 0; i < n; ++i) {
      ris.update((long) i);
    }
    assertEquals(ris.getNumSamples(), ris.getK());

    validateSerializeAndDeserialize(ris);

    println("Full reservoir:");
    println("  Preamble:");
    final byte[] byteArr = ris.toByteArray(new ArrayOfLongsSerDe2());
    println(ReservoirItemsSketch.toString(byteArr));
    ReservoirItemsSketch.toString(MemorySegment.ofArray(byteArr));
    println("  Sketch:");
    println(ris.toString());
  }

  @Test
  public void checkPolymorphicType() {
    final ReservoirItemsSketch<Number> ris = ReservoirItemsSketch.newInstance(6);

    assertNull(ris.getSamples());
    assertNull(ris.getSamples(Number.class));

    // using mixed types
    ris.update(1);
    ris.update(2L);
    ris.update(3.0);
    ris.update((short) (44023 & 0xFFFF));
    ris.update((byte) (68 & 0xFF));
    ris.update(4.0F);

    final Number[] data = ris.getSamples(Number.class);
    assertNotNull(data);
    assertEquals(data.length, 6);

    // copying samples without specifying Number.class should fail
    try {
      ris.getSamples();
      fail();
    } catch (final ArrayStoreException e) {
      // expected
    }

    // likewise for toByteArray() (which uses getDataSamples() internally for type handling)
    final ArrayOfNumbersSerDe2 serDe = new ArrayOfNumbersSerDe2();
    try {
      ris.toByteArray(serDe);
      fail();
    } catch (final ArrayStoreException e) {
      // expected
    }

    final byte[] sketchBytes = ris.toByteArray(serDe, Number.class);
    assertEquals(sketchBytes.length, 49);

    final MemorySegment seg = MemorySegment.ofArray(sketchBytes);
    final ReservoirItemsSketch<Number> loadedRis = ReservoirItemsSketch.heapify(seg, serDe);

    assertEquals(ris.getNumSamples(), loadedRis.getNumSamples());

    final Number[] samples1 = ris.getSamples(Number.class);
    final Number[] samples2 = loadedRis.getSamples(Number.class);
    assertNotNull(samples1);
    assertNotNull(samples2);
    assertEquals(samples1.length, samples2.length);

    for (int i = 0; i < samples1.length; ++i) {
      assertEquals(samples1[i], samples2[i]);
    }
  }

  @Test
  public void checkArrayOfNumbersSerDeErrors() {
    // Highly debatable whether this belongs here vs a stand-alone test class
    final ReservoirItemsSketch<Number> ris = ReservoirItemsSketch.newInstance(6);

    assertNull(ris.getSamples());
    assertNull(ris.getSamples(Number.class));

    // using mixed types, but BigDecimal not supported by serde class
    ris.update(1);
    ris.update(new BigDecimal(2));

    // this should work since BigDecimal is an instance of Number
    final Number[] data = ris.getSamples(Number.class);
    assertNotNull(data);
    assertEquals(data.length, 2);

    // toByteArray() should fail
    final ArrayOfNumbersSerDe2 serDe = new ArrayOfNumbersSerDe2();
    try {
      ris.toByteArray(serDe, Number.class);
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }

    // force entry to a supported type
    data[1] = 3.0;
    final byte[] bytes = serDe.serializeToByteArray(data);

    // change first element to indicate something unsupported
    bytes[0] = 'q';
    try {
      serDe.deserializeFromMemorySegment(MemorySegment.ofArray(bytes), 0, 2);
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }
  }

  @Test
  public void checkBadConstructorArgs() {
    final ArrayList<String> data = new ArrayList<>(128);
    for (int i = 0; i < 128; ++i) {
      data.add(Integer.toString(i));
    }

    final ResizeFactor rf = ResizeFactor.X8;

    // no items
    try {
      ReservoirItemsSketch.<Byte>newInstance(null, 128, rf, 128);
      fail();
    } catch (final SketchesException e) {
      assertTrue(e.getMessage().contains("null reservoir"));
    }

    // size too small
    try {
      ReservoirItemsSketch.newInstance(data, 128, rf, 1);
      fail();
    } catch (final SketchesException e) {
      assertTrue(e.getMessage().contains("size less than 2"));
    }

    // configured reservoir size smaller than items length
    try {
      ReservoirItemsSketch.newInstance(data, 128, rf, 64);
      fail();
    } catch (final SketchesException e) {
      assertTrue(e.getMessage().contains("max size less than array length"));
    }

    // too many items seen vs items length, full sketch
    try {
      ReservoirItemsSketch.newInstance(data, 512, rf, 256);
      fail();
    } catch (final SketchesException e) {
      assertTrue(e.getMessage().contains("too few samples"));
    }

    // too many items seen vs items length, under-full sketch
    try {
      ReservoirItemsSketch.newInstance(data, 256, rf, 256);
      fail();
    } catch (final SketchesException e) {
      assertTrue(e.getMessage().contains("too few samples"));
    }
  }

  @Test
  public void checkRawSamples() {
    final int  k = 32;
    final long n = 12;
    final ReservoirItemsSketch<Long> ris = ReservoirItemsSketch.newInstance(k, ResizeFactor.X2);

    for (long i = 0; i < n; ++i) {
      ris.update(i);
    }

    Long[] samples = ris.getSamples();
    assertNotNull(samples);
    assertEquals(samples.length, n);

    final ArrayList<Long> rawSamples = ris.getRawSamplesAsList();
    assertEquals(rawSamples.size(), n);

    // change a value and make sure getDataSamples() reflects that change
    assertEquals((long) rawSamples.get(0), 0L);
    rawSamples.set(0, -1L);

    samples = ris.getSamples();
    assertEquals((long) samples[0], -1L);
    assertEquals(samples.length, n);
  }


  @Test
  public void checkSketchCapacity() {
    final ArrayList<Long> data = new ArrayList<>(64);
    for (long i = 0; i < 64; ++i) {
      data.add(i);
    }
    final long itemsSeen = (1L << 48) - 2;

    final ReservoirItemsSketch<Long> ris = ReservoirItemsSketch.newInstance(data, itemsSeen,
            ResizeFactor.X8, 64);

    // this should work, the next should fail
    ris.update(0L);

    try {
      ris.update(0L);
      fail();
    } catch (final SketchesStateException e) {
      assertTrue(e.getMessage().contains("Sketch has exceeded capacity for total items seen"));
    }

    ris.reset();
    assertEquals(ris.getN(), 0);
    ris.update(1L);
    assertEquals(ris.getN(), 1L);
  }

  @Test
  public void checkSampleWeight() {
    final int k = 32;
    final ReservoirItemsSketch<Integer> ris = ReservoirItemsSketch.newInstance(k);

    for (int i = 0; i < (k / 2); ++i) {
      ris.update(i);
    }
    assertEquals(ris.getImplicitSampleWeight(), 1.0); // should be exact value here

    // will have 3k/2 total samples when done
    for (int i = 0; i < k; ++i) {
      ris.update(i);
    }
    assertTrue((ris.getImplicitSampleWeight() - 1.5) < EPS);
  }

  @Test
  public void checkVersionConversion() {
    // version change from 1 to 2 only impact first preamble long, so empty sketch is sufficient
    final int k = 32768;
    final short encK = ReservoirSize.computeSize(k);
    final ArrayOfLongsSerDe2 serDe = new ArrayOfLongsSerDe2();

    final ReservoirItemsSketch<Long> ris = ReservoirItemsSketch.newInstance(k);
    final byte[] sketchBytesOrig = ris.toByteArray(serDe);

    // get a new byte[], manually revert to v1, then reconstruct
    final byte[] sketchBytes = ris.toByteArray(serDe);
    final MemorySegment sketchSeg = MemorySegment.ofArray(sketchBytes);

    sketchSeg.set(JAVA_BYTE, SER_VER_BYTE, (byte) 1);
    sketchSeg.set(JAVA_INT_UNALIGNED, RESERVOIR_SIZE_INT, 0); // zero out all 4 bytes
    sketchSeg.set(JAVA_SHORT_UNALIGNED, RESERVOIR_SIZE_SHORT, encK);
    println(PreambleUtil.preambleToString(sketchSeg));

    final ReservoirItemsSketch<Long> rebuilt = ReservoirItemsSketch.heapify(sketchSeg, serDe);
    final byte[] rebuiltBytes = rebuilt.toByteArray(serDe);

    assertEquals(sketchBytesOrig.length, rebuiltBytes.length);
    for (int i = 0; i < sketchBytesOrig.length; ++i) {
      assertEquals(sketchBytesOrig[i], rebuiltBytes[i]);
    }
  }

  @Test
  public void checkSetAndGetValue() {
    final int k = 20;
    final short tgtIdx = 5;
    final ReservoirItemsSketch<Short> ris = ReservoirItemsSketch.newInstance(k);

    ris.update(null);
    assertEquals(ris.getN(), 0);

    for (short i = 0; i < k; ++i) {
      ris.update(i);
    }

    assertEquals((short) ris.getValueAtPosition(tgtIdx), tgtIdx);
    ris.insertValueAtPosition((short) -1, tgtIdx);
    assertEquals((short) ris.getValueAtPosition(tgtIdx), (short) -1);
  }

  @Test
  public void checkBadSetAndGetValue() {
    final int k = 20;
    final int tgtIdx = 5;
    final ReservoirItemsSketch<Integer> ris = ReservoirItemsSketch.newInstance(k);

    try {
      ris.getValueAtPosition(0);
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }

    for (int i = 0; i < k; ++i) {
      ris.update(i);
    }
    assertEquals((int) ris.getValueAtPosition(tgtIdx), tgtIdx);

    try {
      ris.insertValueAtPosition(-1, -1);
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }

    try {
      ris.insertValueAtPosition(-1, k + 1);
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }

    try {
      ris.getValueAtPosition(-1);
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }

    try {
      ris.getValueAtPosition(k + 1);
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }
  }

  @Test
  public void checkForceIncrement() {
    final int k = 100;
    final ReservoirItemsSketch<Long> rls = ReservoirItemsSketch.newInstance(k);

    for (long i = 0; i < (2 * k); ++i) {
      rls.update(i);
    }

    assertEquals(rls.getN(), 2 * k);
    rls.forceIncrementItemsSeen(k);
    assertEquals(rls.getN(), 3 * k);

    try {
      rls.forceIncrementItemsSeen((1L << 48) - 2);
      fail();
    } catch (final SketchesStateException e) {
      // expected
    }
  }

  @Test
  public void checkEstimateSubsetSum() {
    final int k = 10;
    final ReservoirItemsSketch<Long> sketch = ReservoirItemsSketch.newInstance(k);

    // empty sketch -- all zeros
    SampleSubsetSummary ss = sketch.estimateSubsetSum(item -> true);
    assertEquals(ss.getEstimate(), 0.0);
    assertEquals(ss.getTotalSketchWeight(), 0.0);

    // add items, keeping in exact mode
    double itemCount = 0.0;
    for (long i = 1; i <= (k - 1); ++i) {
      sketch.update(i);
      itemCount += 1.0;
    }

    ss = sketch.estimateSubsetSum(item -> true);
    assertEquals(ss.getEstimate(), itemCount);
    assertEquals(ss.getLowerBound(), itemCount);
    assertEquals(ss.getUpperBound(), itemCount);
    assertEquals(ss.getTotalSketchWeight(), itemCount);

    // add a few more items, pushing to sampling mode
    for (long i = k; i <= (k + 1); ++i) {
      sketch.update(i);
      itemCount += 1.0;
    }

    // predicate always true so estimate == upper bound
    ss = sketch.estimateSubsetSum(item -> true);
    assertEquals(ss.getEstimate(), itemCount);
    assertEquals(ss.getUpperBound(), itemCount);
    assertTrue(ss.getLowerBound() < itemCount);
    assertEquals(ss.getTotalSketchWeight(), itemCount);

    // predicate always false so estimate == lower bound == 0.0
    ss = sketch.estimateSubsetSum(item -> false);
    assertEquals(ss.getEstimate(), 0.0);
    assertEquals(ss.getLowerBound(), 0.0);
    assertTrue(ss.getUpperBound() > 0.0);
    assertEquals(ss.getTotalSketchWeight(), itemCount);

    // finally, a non-degenerate predicate
    // insert negative items with identical weights, filter for negative weights only
    for (long i = 1; i <= (k + 1); ++i) {
      sketch.update(-i);
      itemCount += 1.0;
    }

    ss = sketch.estimateSubsetSum(item -> item < 0);
    assertTrue(ss.getEstimate() >= ss.getLowerBound());
    assertTrue(ss.getEstimate() <= ss.getUpperBound());

    // allow pretty generous bounds when testing
    assertTrue(ss.getLowerBound() < (itemCount / 1.4));
    assertTrue(ss.getUpperBound() > (itemCount / 2.6));
    assertEquals(ss.getTotalSketchWeight(), itemCount);
  }

  private static MemorySegment getBasicSerializedLongsRIS() {
    final int k = 10;
    final int n = 20;

    final ReservoirItemsSketch<Long> ris = ReservoirItemsSketch.newInstance(k);
    assertEquals(ris.getNumSamples(), 0);

    for (int i = 0; i < n; ++i) {
      ris.update((long) i);
    }
    assertEquals(ris.getNumSamples(), Math.min(n, k));
    assertEquals(ris.getN(), n);
    assertEquals(ris.getK(), k);

    final byte[] sketchBytes = ris.toByteArray(new ArrayOfLongsSerDe2());
    return MemorySegment.ofArray(sketchBytes);
  }

  private static void validateSerializeAndDeserialize(final ReservoirItemsSketch<Long> ris) {
    final byte[] sketchBytes = ris.toByteArray(new ArrayOfLongsSerDe2());
    assertEquals(sketchBytes.length,
            (Family.RESERVOIR.getMaxPreLongs() + ris.getNumSamples()) << 3);

    // ensure full reservoir rebuilds correctly
    final MemorySegment seg = MemorySegment.ofArray(sketchBytes);
    final ArrayOfLongsSerDe2 serDe = new ArrayOfLongsSerDe2();
    final ReservoirItemsSketch<Long> loadedRis = ReservoirItemsSketch.heapify(seg, serDe);

    validateReservoirEquality(ris, loadedRis);
  }

  static <T> void validateReservoirEquality(final ReservoirItemsSketch<T> ris1,
                                            final ReservoirItemsSketch<T> ris2) {
    assertEquals(ris1.getNumSamples(), ris2.getNumSamples());

    if (ris1.getNumSamples() == 0) { return; }

    final Object[] samples1 = ris1.getSamples();
    final Object[] samples2 = ris2.getSamples();
    assertNotNull(samples1);
    assertNotNull(samples2);
    assertEquals(samples1.length, samples2.length);

    for (int i = 0; i < samples1.length; ++i) {
      assertEquals(samples1[i], samples2[i]);
    }
  }

  /**
   * Wrapper around System.out.println() allowing a simple way to disable logging in tests
   * @param msg The message to print
   */
  private static void println(final String msg) {
    //System.out.println(msg);
  }
}
