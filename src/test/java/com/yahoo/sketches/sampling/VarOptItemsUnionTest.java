/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.sampling;

import static com.yahoo.sketches.sampling.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.sampling.PreambleUtil.SER_VER_BYTE;
import static com.yahoo.sketches.sampling.VarOptItemsSketchTest.EPS;
import static com.yahoo.sketches.sampling.VarOptItemsSketchTest.getUnweightedLongsVIS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.ArrayOfLongsSerDe;
import com.yahoo.sketches.ArrayOfStringsSerDe;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;


/**
 * @author Jon Malkin
 */
public class VarOptItemsUnionTest {
  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSerVer() {
    final int k = 25;
    final int n = 30;
    final VarOptItemsUnion<Long> union = VarOptItemsUnion.newInstance(k);
    union.update(getUnweightedLongsVIS(k, n));
    final byte[] bytes = union.toByteArray(new ArrayOfLongsSerDe());
    final WritableMemory mem = WritableMemory.wrap(bytes);

    mem.putByte(SER_VER_BYTE, (byte) 0); // corrupt the serialization version

    VarOptItemsUnion.heapify(mem, new ArrayOfLongsSerDe());
    fail();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadPreLongs() {
    final int k = 25;
    final int n = 30;
    final VarOptItemsUnion<Long> union = VarOptItemsUnion.newInstance(k);
    union.update(getUnweightedLongsVIS(k, n));
    final byte[] bytes = union.toByteArray(new ArrayOfLongsSerDe());
    final WritableMemory mem = WritableMemory.wrap(bytes);

    // corrupt the preLongs count to 0
    mem.putByte(PREAMBLE_LONGS_BYTE, (byte) (Family.VAROPT.getMinPreLongs() - 1));
    VarOptItemsUnion.heapify(mem, new ArrayOfLongsSerDe());
    fail();
  }

  @Test
  public void unionEmptySketch() {
    final int k = 2048;
    final ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();

    // we'll union from Memory for good measure
    final byte[] sketchBytes = VarOptItemsSketch.<String>newInstance(k).toByteArray(serDe);
    final Memory mem = Memory.wrap(sketchBytes);

    final VarOptItemsUnion<String> union = VarOptItemsUnion.newInstance(k);
    union.update(mem, serDe);

    final VarOptItemsSketch<String> result = union.getResult();
    assertEquals(result.getN(), 0);
    assertEquals(result.getHRegionCount(), 0);
    assertEquals(result.getRRegionCount(), 0);
    assertTrue(Double.isNaN(result.getTau()));
  }

  @Test
  public void unionTwoExactSketches() {
    final int n = 4; // 2n < k
    final int k = 10;
    final VarOptItemsSketch<Integer> sk1 = VarOptItemsSketch.newInstance(k);
    final VarOptItemsSketch<Integer> sk2 = VarOptItemsSketch.newInstance(k);

    for (int i = 1; i <= n; ++i) {
      sk1.update(i, i);
      sk2.update(-i, i);
    }

    final VarOptItemsUnion<Integer> union = VarOptItemsUnion.newInstance(k);
    union.update(sk1);
    union.update(sk2);

    final VarOptItemsSketch<Integer> result = union.getResult();
    assertEquals(result.getN(), 2 * n);
    assertEquals(result.getHRegionCount(), 2 * n);
    assertEquals(result.getRRegionCount(), 0);
  }

  @Test
  public void unionHeavySamplingSketch() {
    final int n1 = 20;
    final int k1 = 10;
    final int n2 = 6;
    final int k2 = 5;
    final VarOptItemsSketch<Integer> sk1 = VarOptItemsSketch.newInstance(k1);
    final VarOptItemsSketch<Integer> sk2 = VarOptItemsSketch.newInstance(k2);

    for (int i = 1; i <= n1; ++i) {
      sk1.update(i, i);
    }

    for (int i = 1; i < n2; ++i) { // we'll add a very heavy one later
      sk2.update(-i, i + 1000.0);
    }
    sk2.update(-n2, 1000000.0);

    final VarOptItemsUnion<Integer> union = VarOptItemsUnion.newInstance(k1);
    union.update(sk1);
    union.update(sk2);

    VarOptItemsSketch<Integer> result = union.getResult();
    assertEquals(result.getN(), n1 + n2);
    assertEquals(result.getK(), k2); // heavy enough it'll pull back to k2
    assertEquals(result.getHRegionCount(), 1);
    assertEquals(result.getRRegionCount(), k2 - 1);

    union.reset();
    assertEquals(union.getOuterTau(), 0.0);
    result = union.getResult();
    assertEquals(result.getK(), k1);
    assertEquals(result.getN(), 0);
  }

  @Test
  public void unionIdenticalSamplingSketches() {
    final int k = 20;
    final int n = 50;
    VarOptItemsSketch<Long> sketch = getUnweightedLongsVIS(k, n);

    final VarOptItemsUnion<Long> union = VarOptItemsUnion.newInstance(k);
    union.update(sketch);
    union.update(sketch);

    VarOptItemsSketch<Long> result = union.getResult();
    double expectedWeight = 2.0 * n; // unweighted, aka uniform weight of 1.0
    assertEquals(result.getN(), 2 * n);
    assertEquals(result.getTotalWtR(), expectedWeight);

    // add another sketch, such that sketchTau < outerTau
    sketch = getUnweightedLongsVIS(k, k + 1); // tau = (k + 1) / k
    union.update(sketch);
    result = union.getResult();
    expectedWeight = 2.0 * n + k + 1;
    assertEquals(result.getN(), 2 * n + k + 1);
    assertEquals(result.getTotalWtR(), expectedWeight, EPS);

    union.reset();
    assertEquals(union.getOuterTau(), 0.0);
    result = union.getResult();
    assertEquals(result.getK(), k);
    assertEquals(result.getN(), 0);
  }

  @Test
  public void unionSmallSamplingSketch() {
    final int kSmall = 16;
    final int n1 = 32;
    final int n2 = 64;
    final int kMax = 128;

    // small k sketch, but sampling
    VarOptItemsSketch<Long> sketch = getUnweightedLongsVIS(kSmall, n1);
    sketch.update(-1L, n1 ^ 2); // add a heavy item

    final VarOptItemsUnion<Long> union = VarOptItemsUnion.newInstance(kMax);
    union.update(sketch);

    // another one, but different n to get a different per-item weight
    sketch = getUnweightedLongsVIS(kSmall, n2);
    union.update(sketch);

    // should trigger migrateMarkedItemsByDecreasingK()
    final VarOptItemsSketch<Long> result = union.getResult();
    assertEquals(result.getN(), n1 + n2 + 1);
    assertEquals(result.getTotalWtR(), 96.0, EPS); // n1+n2 light items, ignore the heavy one
  }

  @Test
  public void unionExactReservoirSketch() {
    // build a varopt union which contains both heavy and light items, then copy it and
    // compare unioning:
    // 1. A varopt sketch of items with weight 1.0
    // 2. A reservoir sample made of the same input items as above
    // and we should find that the resulting unions are equivalent.

    final int k = 20;
    final long n = 2 * k;

    final VarOptItemsSketch<Long> baseVis = VarOptItemsSketch.newInstance(k);
    for (long i = 1; i <= n; ++i) {
      baseVis.update(-i, i);
    }
    baseVis.update(-n - 1L, n * n);
    baseVis.update(-n - 2L, n * n);
    baseVis.update(-n - 3L, n * n);

    final VarOptItemsUnion<Long> union1 = VarOptItemsUnion.newInstance(k);
    union1.update(baseVis);

    final ArrayOfLongsSerDe serDe = new ArrayOfLongsSerDe();
    final Memory unionImg = Memory.wrap(union1.toByteArray(serDe));
    final VarOptItemsUnion<Long> union2 = VarOptItemsUnion.heapify(unionImg, serDe);

    compareUnionsExact(union1, union2); // sanity check

    final VarOptItemsSketch<Long> vis = VarOptItemsSketch.newInstance(k);
    final ReservoirItemsSketch<Long> ris = ReservoirItemsSketch.newInstance(k);

    union2.update((ReservoirItemsSketch<Long>) null);
    union2.update(ris); // empty

    compareUnionsExact(union1, union2); // union2 should be unchanged

    for (long i = 1; i < k - 1; ++i) {
      ris.update(i);
      vis.update(i, 1.0);
    }

    union1.update(vis);
    union2.update(ris);

    compareUnionsEquivalent(union1, union2);
  }

  @Test
  public void unionSamplingReservoirSketch() {
    // Like unionExactReservoirSketch, but merge in reservoir first, with reservoir in sampling mode
    final int k = 20;
    final long n = k * k;

    final VarOptItemsUnion<Long> union1 = VarOptItemsUnion.newInstance(k);
    final VarOptItemsUnion<Long> union2 = VarOptItemsUnion.newInstance(k);

    compareUnionsExact(union1, union2); // sanity check

    final VarOptItemsSketch<Long> vis = VarOptItemsSketch.newInstance(k);
    final ReservoirItemsSketch<Long> ris = ReservoirItemsSketch.newInstance(k);

    for (long i = 1; i < n; ++i) {
      ris.update(i);
      vis.update(i, 1.0);
    }

    union1.update(vis);
    union2.update(ris);
    compareUnionsEquivalent(union1, union2);

    // repeat to trigger equal tau scenario
    union1.update(vis);
    union2.update(ris);
    compareUnionsEquivalent(union1, union2);

    // create and add a sketch with some heavy items
    final VarOptItemsSketch<Long> newVis = VarOptItemsSketch.newInstance(k);
    for (long i = 1; i <= n; ++i) {
      newVis.update(-i, i);
    }
    newVis.update(-n - 1L, n * n);
    newVis.update(-n - 2L, n * n);
    newVis.update(-n - 3L, n * n);

    union1.update(newVis);
    union2.update(newVis);
    compareUnionsEquivalent(union1, union2);
  }

  @Test
  public void unionReservoirVariousTauValues() {
    final int k = 20;
    final long n = 2 * k;

    final VarOptItemsSketch<Long> baseVis = VarOptItemsSketch.newInstance(k);
    for (long i = 1; i <= n; ++i) {
      baseVis.update(-i, 1.0);
    }

    final VarOptItemsUnion<Long> union1 = VarOptItemsUnion.newInstance(k);
    union1.update(baseVis);

    final ArrayOfLongsSerDe serDe = new ArrayOfLongsSerDe();
    final Memory unionImg = Memory.wrap(union1.toByteArray(serDe));
    final VarOptItemsUnion<Long> union2 = VarOptItemsUnion.heapify(unionImg, serDe);

    compareUnionsExact(union1, union2); // sanity check

    // reservoir tau will be greater than gadget's tau
    VarOptItemsSketch<Long> vis = VarOptItemsSketch.newInstance(k);
    ReservoirItemsSketch<Long> ris = ReservoirItemsSketch.newInstance(k);
    for (long i = 1; i < 2 * n; ++i) {
      ris.update(i);
      vis.update(i, 1.0);
    }

    union1.update(vis);
    union2.update(ris);
    compareUnionsEquivalent(union1, union2);

    // reservoir tau will be smaller than gadget's tau
    vis = VarOptItemsSketch.newInstance(k);
    ris = ReservoirItemsSketch.newInstance(k);
    for (long i = 1; i <= k + 1; ++i) {
      ris.update(i);
      vis.update(i, 1.0);
    }

    union1.update(vis);
    union2.update(ris);
    compareUnionsEquivalent(union1, union2);
  }

  @Test
  public void serializeEmptyUnion() {
    final int k = 100;
    final VarOptItemsUnion<String> union = VarOptItemsUnion.newInstance(k);
    // null inputs to update() should leave the union empty
    union.update((VarOptItemsSketch<String>) null);
    union.update(null, new ArrayOfStringsSerDe());

    final ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();
    final byte[] bytes = union.toByteArray(serDe);
    assertEquals(bytes.length, 8);

    final Memory mem = Memory.wrap(bytes);
    final VarOptItemsUnion<String> rebuilt = VarOptItemsUnion.heapify(mem, serDe);

    final VarOptItemsSketch<String> sketch = rebuilt.getResult();
    assertEquals(sketch.getN(), 0);

    assertEquals(rebuilt.toString(), union.toString());
  }

  @Test
  public void serializeExactUnion() {
    final int n1 = 32;
    final int n2 = 64;
    final int k = 128;
    final VarOptItemsSketch<Long> sketch1 = getUnweightedLongsVIS(k, n1);
    final VarOptItemsSketch<Long> sketch2 = getUnweightedLongsVIS(k, n2);

    final VarOptItemsUnion<Long> union = VarOptItemsUnion.newInstance(k);
    union.update(sketch1);
    union.update(sketch2);

    final ArrayOfLongsSerDe serDe = new ArrayOfLongsSerDe();
    final byte[] unionBytes = union.toByteArray(serDe);
    final Memory mem = Memory.wrap(unionBytes);

    final VarOptItemsUnion<Long> rebuilt = VarOptItemsUnion.heapify(mem, serDe);
    compareUnionsExact(rebuilt, union);

    assertEquals(rebuilt.toString(), union.toString());
  }

  @Test
  public void serializeSamplingUnion() {
    final int n = 256;
    final int k = 128;
    final VarOptItemsSketch<Long> sketch = getUnweightedLongsVIS(k, n);
    sketch.update(n + 1L, 1000.0);
    sketch.update(n + 2L, 1001.0);
    sketch.update(n + 3L, 1002.0);
    sketch.update(n + 4L, 1003.0);
    sketch.update(n + 5L, 1004.0);
    sketch.update(n + 6L, 1005.0);
    sketch.update(n + 7L, 1006.0);
    sketch.update(n + 8L, 1007.0);

    final VarOptItemsUnion<Long> union = VarOptItemsUnion.newInstance(k);
    union.update(sketch);

    final ArrayOfLongsSerDe serDe = new ArrayOfLongsSerDe();
    final byte[] unionBytes = union.toByteArray(serDe);
    final Memory mem = Memory.wrap(unionBytes);

    final VarOptItemsUnion<Long> rebuilt = VarOptItemsUnion.heapify(mem, serDe);
    compareUnionsExact(rebuilt, union);

    assertEquals(rebuilt.toString(), union.toString());
  }

  private static <T> void compareUnionsExact(final VarOptItemsUnion<T> u1,
                                             final VarOptItemsUnion<T> u2) {
    assertEquals(u1.getOuterTau(), u2.getOuterTau());

    final VarOptItemsSketch<T> sketch1 = u1.getResult();
    final VarOptItemsSketch<T> sketch2 = u2.getResult();
    assertEquals(sketch1.getN(), sketch2.getN());
    assertEquals(sketch1.getHRegionCount(), sketch2.getHRegionCount());
    assertEquals(sketch1.getRRegionCount(), sketch2.getRRegionCount());

    final VarOptItemsSamples<T> s1 = sketch1.getSketchSamples();
    final VarOptItemsSamples<T> s2 = sketch2.getSketchSamples();

    assertEquals(s1.getNumSamples(), s2.getNumSamples());
    assertEquals(s1.weights(), s2.weights());
    assertEquals(s1.items(), s2.items());
  }

  private static <T> void compareUnionsEquivalent(final VarOptItemsUnion<T> u1,
                                                  final VarOptItemsUnion<T> u2) {
    assertEquals(u1.getOuterTau(), u2.getOuterTau());

    final VarOptItemsSketch<T> sketch1 = u1.getResult();
    final VarOptItemsSketch<T> sketch2 = u2.getResult();
    assertEquals(sketch1.getN(), sketch2.getN());
    assertEquals(sketch1.getHRegionCount(), sketch2.getHRegionCount());
    assertEquals(sketch1.getRRegionCount(), sketch2.getRRegionCount());

    final VarOptItemsSamples<T> s1 = sketch1.getSketchSamples();
    final VarOptItemsSamples<T> s2 = sketch2.getSketchSamples();

    assertEquals(s1.getNumSamples(), s2.getNumSamples());
    assertEquals(s1.weights(), s2.weights());
    // only compare exact items; others can differ as long as weights match
    for (int i = 0; i < sketch1.getHRegionCount(); ++i) {
      assertEquals(s1.items(i), s2.items(i));
    }
  }

  /**
   * Wrapper around System.out.println() allowing a simple way to disable logging in tests
   * @param msg The message to print
   */
  @SuppressWarnings("unused")
  private static void println(final String msg) {
    //System.out.println(msg);
  }
}
