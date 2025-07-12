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
import static java.lang.foreign.ValueLayout.JAVA_DOUBLE_UNALIGNED;
import static org.apache.datasketches.sampling.PreambleUtil.FAMILY_BYTE;
import static org.apache.datasketches.sampling.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static org.apache.datasketches.sampling.PreambleUtil.SER_VER_BYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.ArrayOfLongsSerDe2;
import org.apache.datasketches.common.ArrayOfStringsSerDe2;
import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesStateException;
import org.apache.datasketches.sampling.PreambleUtil;
import org.apache.datasketches.sampling.SampleSubsetSummary;
import org.apache.datasketches.sampling.SamplingUtil;
import org.apache.datasketches.sampling.VarOptItemsSamples;
import org.apache.datasketches.sampling.VarOptItemsSketch;
import org.testng.annotations.Test;

public class VarOptItemsSketchTest {
  static final double EPS = 1e-10;

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidK() {
    VarOptItemsSketch.<Integer>newInstance(0);
    fail();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSerVer() {
    final VarOptItemsSketch<Long> sketch = getUnweightedLongsVIS(16, 16);
    final byte[] bytes = sketch.toByteArray(new ArrayOfLongsSerDe2());
    final MemorySegment seg = MemorySegment.ofArray(bytes);

    seg.set(JAVA_BYTE, SER_VER_BYTE, (byte) 0); // corrupt the serialization version

    VarOptItemsSketch.heapify(seg, new ArrayOfLongsSerDe2());
    fail();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadFamily() {
    final VarOptItemsSketch<Long> sketch = getUnweightedLongsVIS(32, 16);
    final byte[] bytes = sketch.toByteArray(new ArrayOfLongsSerDe2());
    final MemorySegment seg = MemorySegment.ofArray(bytes);

    seg.set(JAVA_BYTE, FAMILY_BYTE, (byte) 0); // corrupt the family ID

    VarOptItemsSketch.heapify(seg, new ArrayOfLongsSerDe2());
    fail();
  }

  @Test
  public void checkBadPreLongs() {
    final VarOptItemsSketch<Long> sketch = getUnweightedLongsVIS(32, 33);
    final byte[] bytes = sketch.toByteArray(new ArrayOfLongsSerDe2());
    final MemorySegment seg = MemorySegment.ofArray(bytes);

    // corrupt the preLongs count to 0
    seg.set(JAVA_BYTE, PREAMBLE_LONGS_BYTE, (byte) (Family.VAROPT.getMinPreLongs() - 1));
    try {
      VarOptItemsSketch.heapify(seg, new ArrayOfLongsSerDe2());
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }

    seg.set(JAVA_BYTE, PREAMBLE_LONGS_BYTE, (byte) 2); // corrupt the preLongs count to 2
    try {
      VarOptItemsSketch.heapify(seg, new ArrayOfLongsSerDe2());
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }

    // corrupt the preLongs count to be too large
    seg.set(JAVA_BYTE, PREAMBLE_LONGS_BYTE, (byte) (Family.VAROPT.getMaxPreLongs() + 1));
    try {
      VarOptItemsSketch.heapify(seg, new ArrayOfLongsSerDe2());
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }
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
  public void checkMalformedPreamble() {
    final int k = 50;
    final VarOptItemsSketch<Long> sketch = getUnweightedLongsVIS(k, k);

    final byte[] sketchBytes = sketch.toByteArray(new ArrayOfLongsSerDe2());
    final MemorySegment srcSeg = MemorySegment.ofArray(sketchBytes);

    // we'll use the same initial sketch a few times, so grab a copy of it
    final byte[] copyBytes = new byte[sketchBytes.length];
    final MemorySegment seg = MemorySegment.ofArray(copyBytes);

    // copy the bytes
    MemorySegment.copy(srcSeg, 0, seg, 0, sketchBytes.length);
    assertEquals(PreambleUtil.extractPreLongs(seg), PreambleUtil.VO_PRELONGS_WARMUP);

    // no items in R but max preLongs
    try {
      PreambleUtil.insertPreLongs(seg, Family.VAROPT.getMaxPreLongs());
      VarOptItemsSketch.heapify(seg, new ArrayOfLongsSerDe2());
      fail();
    } catch (final SketchesArgumentException e) {
      assertTrue(e.getMessage().startsWith("Possible Corruption: "
              + Family.VAROPT.getMaxPreLongs() + " preLongs but"));
    }

    // refresh the copy
    MemorySegment.copy(srcSeg, 0, seg, 0, sketchBytes.length);
    assertEquals(PreambleUtil.extractPreLongs(seg), PreambleUtil.VO_PRELONGS_WARMUP);

    // negative H region count
    try {
      PreambleUtil.insertHRegionItemCount(seg, -1);
      VarOptItemsSketch.heapify(seg, new ArrayOfLongsSerDe2());
      fail();
    } catch (final SketchesArgumentException e) {
      assertTrue(e.getMessage().equals("Possible Corruption: H region count cannot be negative: -1"));
    }

    // refresh the copy
    MemorySegment.copy(srcSeg, 0, seg, 0, sketchBytes.length);
    assertEquals(PreambleUtil.extractHRegionItemCount(seg), k);

    // negative R region count
    try {
      PreambleUtil.insertRRegionItemCount(seg, -128);
      VarOptItemsSketch.heapify(seg, new ArrayOfLongsSerDe2());
      fail();
    } catch (final SketchesArgumentException e) {
      assertTrue(e.getMessage().equals("Possible Corruption: R region count cannot be negative: -128"));
    }

    // refresh the copy
    MemorySegment.copy(srcSeg, 0, seg, 0, sketchBytes.length);
    assertEquals(PreambleUtil.extractRRegionItemCount(seg), 0);

    // invalid k < 1
    try {
      PreambleUtil.insertK(seg, 0);
      VarOptItemsSketch.heapify(seg, new ArrayOfLongsSerDe2());
      fail();
    } catch (final SketchesArgumentException e) {
      assertTrue(e.getMessage().equals("Possible Corruption: k must be at least 1: 0"));
    }

    // refresh the copy
    MemorySegment.copy(srcSeg, 0, seg, 0, sketchBytes.length);
    assertEquals(PreambleUtil.extractK(seg), k);

    // invalid n < 0
    try {
      PreambleUtil.insertN(seg, -1024);
      VarOptItemsSketch.heapify(seg, new ArrayOfLongsSerDe2());
      fail();
    } catch (final SketchesArgumentException e) {
      assertTrue(e.getMessage().equals("Possible Corruption: n cannot be negative: -1024"));
    }
  }

  @Test
  public void checkEmptySketch() {
    final VarOptItemsSketch<String> vis = VarOptItemsSketch.newInstance(5);
    assertEquals(vis.getN(), 0);
    assertEquals(vis.getNumSamples(), 0);
    assertNull(vis.getSamplesAsArrays());
    assertNull(vis.getSamplesAsArrays(Long.class));

    final byte[] sketchBytes = vis.toByteArray(new ArrayOfStringsSerDe2());
    final MemorySegment seg = MemorySegment.ofArray(sketchBytes);

    // only minPreLongs bytes and should deserialize to empty
    assertEquals(sketchBytes.length, Family.VAROPT.getMinPreLongs() << 3);
    final ArrayOfStringsSerDe2 serDe = new ArrayOfStringsSerDe2();
    final VarOptItemsSketch<String> loadedVis = VarOptItemsSketch.heapify(seg, serDe);
    assertEquals(loadedVis.getNumSamples(), 0);

    println("Empty sketch:");
    println("  Preamble:");
    VarOptItemsSketch.toString(sketchBytes);
    println(VarOptItemsSketch.toString(seg));
    println("  Sketch:");
    println(vis.toString());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkNonEmptyDegenerateSketch() {
    // make an empty serialized sketch, then copy the items into a
    // PreambleUtil.VO_WARMUP_PRELONGS-sized byte array
    // so there'll be no items, then clear the empty flag so it will try to load
    // the rest.
    final VarOptItemsSketch<String> vis = VarOptItemsSketch.newInstance(12, ResizeFactor.X2);
    final byte[] sketchBytes = vis.toByteArray(new ArrayOfStringsSerDe2());
    final byte[] dstByteArr = new byte[PreambleUtil.VO_PRELONGS_WARMUP << 3];
    final MemorySegment seg = MemorySegment.ofArray(dstByteArr);
    MemorySegment.copy(sketchBytes, 0, seg, JAVA_BYTE, 0, sketchBytes.length);

    // ensure non-empty but with H and R region sizes set to 0
    PreambleUtil.insertFlags(seg, 0); // set not-empty
    PreambleUtil.insertHRegionItemCount(seg, 0);
    PreambleUtil.insertRRegionItemCount(seg, 0);

    VarOptItemsSketch.heapify(seg, new ArrayOfStringsSerDe2());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidWeight() {
    final VarOptItemsSketch<String> vis = VarOptItemsSketch.newInstance(5);
    try {
      vis.update(null, 1.0); // should work fine
    } catch (final SketchesArgumentException e) {
      fail();
    }

    vis.update("invalidWeight", -1.0); // should fail
  }

  @Test
  public void checkCorruptSerializedWeight() {
    final VarOptItemsSketch<String> vis = VarOptItemsSketch.newInstance(24);
    for (int i = 1; i < 10; ++i) {
      vis.update(Integer.toString(i), i);
    }

    final byte[] sketchBytes = vis.toByteArray(new ArrayOfStringsSerDe2(), String.class);
    final MemorySegment seg = MemorySegment.ofArray(sketchBytes);

    // weights will be stored in the first double after the preamble
    final int numPreLongs = PreambleUtil.extractPreLongs(seg);
    final int weightOffset = numPreLongs << 3;
    seg.set(JAVA_DOUBLE_UNALIGNED, weightOffset, -1.25); // inject a negative weight

    try {
      VarOptItemsSketch.heapify(seg, new ArrayOfStringsSerDe2());
      fail();
    } catch (final SketchesArgumentException e) {
      assertTrue(e.getMessage().equals("Possible Corruption: Non-positive weight in "
              + "heapify(): -1.25"));
    }
  }

  @Test
  public void checkCumulativeWeight() {
    final int k = 256;
    final int n = 10 * k;
    final VarOptItemsSketch<Long> sketch = VarOptItemsSketch.newInstance(k);

    double inputSum = 0.0;
    for (long i = 0; i < n; ++i) {
      // generate weights above and below 1.0 using w ~ exp(5*N(0,1)) which covers about
      // 10 orders of magnitude
      final double w = Math.exp(5 * SamplingUtil.rand().nextGaussian());
      inputSum += w;
      sketch.update(i, w);
    }

    final VarOptItemsSamples<Long> samples = sketch.getSketchSamples();
    double outputSum = 0;
    for (final VarOptItemsSamples<Long>.WeightedSample ws : samples) {
      outputSum += ws.getWeight();
    }

    final double wtRatio = outputSum / inputSum;
    assertTrue(Math.abs(wtRatio - 1.0) < EPS);
  }

  @Test
  public void checkUnderFullSketchSerialization() {
    final VarOptItemsSketch<Long> sketch = VarOptItemsSketch.newInstance(2048);
    for (long i = 0; i < 10; ++i) {
      sketch.update(i, 1.0);
    }
    assertEquals(sketch.getNumSamples(), 10);

    final byte[] bytes = sketch.toByteArray(new ArrayOfLongsSerDe2());
    final MemorySegment seg = MemorySegment.ofArray(bytes);

    // ensure correct number of preLongs
    assertEquals(PreambleUtil.extractPreLongs(seg), PreambleUtil.VO_PRELONGS_WARMUP);

    final VarOptItemsSketch<Long> rebuilt
            = VarOptItemsSketch.heapify(seg, new ArrayOfLongsSerDe2());
    checkIfEqual(rebuilt, sketch);
  }

  @Test
  public void checkEndOfWarmupSketchSerialization() {
    final int k = 2048;
    final VarOptItemsSketch<Long> sketch = getUnweightedLongsVIS(k, k);

    final byte[] bytes = sketch.toByteArray(new ArrayOfLongsSerDe2());
    final MemorySegment seg = MemorySegment.ofArray(bytes);

    // ensure still only 2 preLongs
    assertEquals(PreambleUtil.extractPreLongs(seg), PreambleUtil.VO_PRELONGS_WARMUP);

    final VarOptItemsSketch<Long> rebuilt
            = VarOptItemsSketch.heapify(seg, new ArrayOfLongsSerDe2());
    checkIfEqual(rebuilt, sketch);
  }

  @Test
  public void checkFullSketchSerialization() {
    final VarOptItemsSketch<Long> sketch = VarOptItemsSketch.newInstance(32);
    for (long i = 0; i < 32; ++i) {
      sketch.update(i, 1.0);
    }
    sketch.update(100L, 100.0);
    sketch.update(101L, 101.0);
    assertEquals(sketch.getNumSamples(), 32);

    // first 2 entries should be heavy and in heap order (smallest at root)
    final VarOptItemsSamples<Long> samples = sketch.getSketchSamples();
    final Long[] data = samples.items();
    final double[] weights = samples.weights();
    assertEquals(weights[0], 100.0);
    assertEquals(weights[1], 101.0);
    assertEquals((long) data[0], 100L);
    assertEquals((long) data[1], 101L);

    final byte[] bytes = sketch.toByteArray(new ArrayOfLongsSerDe2());
    final MemorySegment seg = MemorySegment.ofArray(bytes);

    // ensure 3 preLongs
    assertEquals(PreambleUtil.extractPreLongs(seg), Family.VAROPT.getMaxPreLongs());

    final VarOptItemsSketch<Long> rebuilt
            = VarOptItemsSketch.heapify(seg, new ArrayOfLongsSerDe2());
    checkIfEqual(rebuilt, sketch);
  }

  @Test
  public void checkPseudoLightUpdate() {
    final int k = 1024;
    final VarOptItemsSketch<Long> sketch = getUnweightedLongsVIS(k, k + 1);
    sketch.update(0L, 1.0); // k+2-nd update

    // checking weights(0), assuming all k items are unweighted (and consequently in R)
    // Expected: (k + 2) / |R| = (k+2) / k
    final VarOptItemsSamples<Long> samples = sketch.getSketchSamples();
    final double wtDiff = samples.weights(0) - ((1.0 * (k + 2)) / k);
    assertTrue(Math.abs(wtDiff) < EPS);
  }

  @Test
  public void checkPseudoHeavyUpdates() {
    final int k = 1024;
    final double wtScale = 10.0 * k;
    final VarOptItemsSketch<Long> sketch = VarOptItemsSketch.newInstance(k);
    for (long i = 0; i <= k; ++i) {
      sketch.update(i, 1.0);
    }

    // Next k-1 updates should be updatePseudoHeavyGeneral()
    // Last one should call updatePseudoHeavyREq1(), since we'll have added k-1 heavy
    // items, leaving only 1 item left in R
    for (long i = 1; i <= k; ++i) {
      sketch.update(-i, k + (i * wtScale));
    }

    final VarOptItemsSamples<Long> samples = sketch.getSketchSamples();
    final double[] weights = samples.weights();

    // Don't know which R item is left, but should be only one at the end of the array
    // Expected: k+1 + (min "heavy" item) / |R| = ((k+1) + (k+wtScale)) / 1 = wtScale + 2k + 1
    double wtDiff = weights[k - 1] - (1.0 * (wtScale + (2 * k) + 1));
    assertTrue(Math.abs(wtDiff) < EPS);

    // Expected: 2nd lightest "heavy" item: k + 2*wtScale
    wtDiff = weights[0] - (1.0 * (k + (2 * wtScale)));
    assertTrue(Math.abs(wtDiff) < EPS);
  }

  @Test(expectedExceptions = SketchesStateException.class)
  public void checkDecreaseKWithUnderfullSketch() {
    final VarOptItemsSketch<Integer> sketch = VarOptItemsSketch.newInstanceAsGadget(5);
    assertEquals(sketch.getK(), 5);

    // shrink empty sketch
    sketch.decreaseKBy1();
    assertEquals(sketch.getK(), 4);

    // insert 3 values
    sketch.update(1, 1.0);
    sketch.update(2, 2.0);
    sketch.update(3, 3.0);

    // shrink to k=3, should do nothing
    assertEquals(sketch.getTotalWtR(), 0.0);
    sketch.decreaseKBy1();
    assertEquals(sketch.getTotalWtR(), 0.0);

    // one more time, to k=2, which exist warmup phase
    sketch.decreaseKBy1();
    assertEquals(sketch.getHRegionCount(), 1);
    assertEquals(sketch.getRRegionCount(), 1);
    assertEquals(sketch.getTotalWtR(), 3.0);

    // decrease twice more to trigger an exception
    sketch.decreaseKBy1();
    sketch.decreaseKBy1();
  }

  @Test
  public void checkDecreaseKWithFullSketch() {
    final int[] itemList = {10, 1, 9, 2, 8, 3, 7, 4, 6, 5};

    final int startK = 7;
    final int tgtK = 5;

    // Create sketch with k = startK and another with k = tgtK. We'll then decrease k until
    // they're equal and ensure the results "match"
    final VarOptItemsSketch<Integer> sketch = VarOptItemsSketch.newInstanceAsGadget(startK);
    final VarOptItemsSketch<Integer> tgtSketch = VarOptItemsSketch.newInstanceAsGadget(tgtK);

    double totalWeight = 0.0;
    for (final int val : itemList) {
      sketch.update(val, val);
      tgtSketch.update(val, val);
      totalWeight += val;
    }

    // larger sketch has heavy items, smaller does not
    assertEquals(sketch.getHRegionCount(), 4);
    assertEquals(sketch.getRRegionCount(), 3);
    assertEquals(tgtSketch.getHRegionCount(), 0);
    assertEquals(tgtSketch.getRRegionCount(), 5);

    while (sketch.getK() > tgtK) {
      sketch.decreaseKBy1();
    }

    assertEquals(sketch.getK(), tgtSketch.getK());
    assertEquals(sketch.getHRegionCount(), 0);
    assertTrue(Math.abs(sketch.getTau() - tgtSketch.getTau()) < EPS);

    // decrease again from reservoir-only mode
    sketch.decreaseKBy1();

    assertEquals(sketch.getK(), tgtK - 1);
    assertEquals(sketch.getK(), sketch.getRRegionCount());
    assertEquals(sketch.getTotalWtR(), totalWeight);
  }

  @Test
  public void checkReset() {
    final int k = 25;
    final VarOptItemsSketch<String> sketch = VarOptItemsSketch.newInstanceAsGadget(k);
    sketch.update("a", 1.0);
    sketch.update("b", 2.0);
    sketch.update("c", 3.0);
    sketch.update("d", 4.0);

    assertEquals(sketch.getN(), 4);
    assertEquals(sketch.getHRegionCount(), 4);
    assertEquals(sketch.getRRegionCount(), 0);
    assertEquals(sketch.getMark(0), false);

    sketch.reset();
    assertEquals(sketch.getN(), 0);
    assertEquals(sketch.getHRegionCount(), 0);
    assertEquals(sketch.getRRegionCount(), 0);
    try {
      sketch.getMark(0);
      fail();
    } catch (final IndexOutOfBoundsException e) {
      // expected
    }

    // strip marks and try again
    sketch.stripMarks();
    for (int i = 0; i < (2 * k); ++i) {
      sketch.update("a", 100.0 + i);
    }
    assertEquals(sketch.getN(), 2 * k);
    assertEquals(sketch.getHRegionCount(), 0);
    assertEquals(sketch.getRRegionCount(), k);
    sketch.reset();
    assertEquals(sketch.getN(), 0);
    assertEquals(sketch.getHRegionCount(), 0);
    assertEquals(sketch.getRRegionCount(), 0);
  }

  @Test
  public void checkEstimateSubsetSum() {
    final int k = 10;
    final VarOptItemsSketch<Long> sketch = VarOptItemsSketch.newInstance(k);

    // empty sketch -- all zeros
    SampleSubsetSummary ss = sketch.estimateSubsetSum(item -> true);
    assertEquals(ss.getEstimate(), 0.0);
    assertEquals(ss.getTotalSketchWeight(), 0.0);

    // add items, keeping in exact mode
    double totalWeight = 0.0;
    for (long i = 1; i <= (k - 1); ++i) {
      sketch.update(i, 1.0 * i);
      totalWeight += 1.0 * i;
    }

    ss = sketch.estimateSubsetSum(item -> true);
    assertEquals(ss.getEstimate(), totalWeight);
    assertEquals(ss.getLowerBound(), totalWeight);
    assertEquals(ss.getUpperBound(), totalWeight);
    assertEquals(ss.getTotalSketchWeight(), totalWeight);

    // add a few more items, pushing to sampling mode
    for (long i = k; i <= (k + 1); ++i) {
      sketch.update(i, 1.0 * i);
      totalWeight += 1.0 * i;
    }

    // predicate always true so estimate == upper bound
    ss = sketch.estimateSubsetSum(item -> true);
    assertEquals(ss.getEstimate(), totalWeight);
    assertEquals(ss.getUpperBound(), totalWeight);
    assertTrue(ss.getLowerBound() < totalWeight);
    assertEquals(ss.getTotalSketchWeight(), totalWeight);

    // predicate always false so estimate == lower bound == 0.0
    ss = sketch.estimateSubsetSum(item -> false);
    assertEquals(ss.getEstimate(), 0.0);
    assertEquals(ss.getLowerBound(), 0.0);
    assertTrue(ss.getUpperBound() > 0.0);
    assertEquals(ss.getTotalSketchWeight(), totalWeight);

    // finally, a non-degenerate predicate
    // insert negative items with identical weights, filter for negative weights only
    for (long i = 1; i <= (k + 1); ++i) {
      sketch.update(-i, 1.0 * i);
      totalWeight += 1.0 * i;
    }

    ss = sketch.estimateSubsetSum(item -> item < 0);
    assertTrue(ss.getEstimate() >= ss.getLowerBound());
    assertTrue(ss.getEstimate() <= ss.getUpperBound());

    // allow pretty generous bounds when testing
    assertTrue(ss.getLowerBound() < (totalWeight / 1.4));
    assertTrue(ss.getUpperBound() > (totalWeight / 2.6));
    assertEquals(ss.getTotalSketchWeight(), totalWeight);

    // for good measure, test a different type
    final VarOptItemsSketch<Boolean> boolSketch = VarOptItemsSketch.newInstance(k);
    totalWeight = 0.0;
    for (int i = 1; i <= (k - 1); ++i) {
      boolSketch.update((i % 2) == 0, 1.0 * i);
      totalWeight += i;
    }

    ss = boolSketch.estimateSubsetSum(item -> !item);
    assertTrue(ss.getEstimate() == ss.getLowerBound());
    assertTrue(ss.getEstimate() == ss.getUpperBound());
    assertTrue(ss.getEstimate() < totalWeight); // exact mode, so know it must be strictly less
  }


  /* Returns a sketch of size k that has been presented with n items. Use n = k+1 to obtain a
     sketch that has just reached the sampling phase, so that the next update() is handled by
     one of the non-warmup routes.
   */
  static VarOptItemsSketch<Long> getUnweightedLongsVIS(final int k, final int n) {
    final VarOptItemsSketch<Long> sketch = VarOptItemsSketch.newInstance(k);
    for (long i = 0; i < n; ++i) {
      sketch.update(i, 1.0);
    }

    return sketch;
  }

  private static <T> void checkIfEqual(final VarOptItemsSketch<T> s1,
                                       final VarOptItemsSketch<T> s2) {
    assertEquals(s1.getK(), s2.getK(), "Sketches have different values of k");
    assertEquals(s1.getNumSamples(), s2.getNumSamples(), "Sketches have different sample counts");

    final int len = s1.getNumSamples();
    final VarOptItemsSamples<T> r1 = s1.getSketchSamples();
    final VarOptItemsSamples<T> r2 = s2.getSketchSamples();

    // next 2 lines also trigger copying results
    assertEquals(len, r1.getNumSamples());
    assertEquals(r1.getNumSamples(), r2.getNumSamples());

    for (int i = 0; i < len; ++i) {
      assertEquals(r1.items(i), r2.items(i), "Data values differ at sample " + i);
      assertEquals(r1.weights(i), r2.weights(i), "Weights differ at sample " + i);
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
