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

package org.apache.datasketches.theta;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.Arrays;

import org.apache.datasketches.common.Family;
import org.testng.annotations.Test;

/**
 * Tests the optional trim flag on UpdatableThetaSketch.compact().
 */
public class CompactTrimTest {
  private static final int K = 4096; //default nominal entries

  private static UpdatableThetaSketch sketchOf(final int n) {
    final UpdatableThetaSketch sk = UpdatableThetaSketch.builder().setNominalEntries(K).build();
    for (int i = 0; i < n; i++) { sk.update(i); }
    return sk;
  }

  private static long[] hashesOf(final CompactThetaSketch sk) {
    final long[] arr = new long[sk.getRetainedEntries(true)];
    final HashIterator it = sk.iterator();
    int i = 0;
    while (it.next()) { arr[i++] = it.get(); }
    Arrays.sort(arr);
    return arr;
  }

  @Test
  public void allFourOrderedTrimCases() {
    final UpdatableThetaSketch sk = sketchOf(8000);
    assertTrue(sk.getRetainedEntries(true) > K); //over-provisioned before trimming
    final int retainedBefore = sk.getRetainedEntries(true);
    final long thetaBefore = sk.getThetaLong();

    //the trimmed result is what rebuild() + compact() would produce
    final UpdatableThetaSketch copy = UpdatableThetaSketch.heapify(
        java.lang.foreign.MemorySegment.ofArray(sk.toByteArray()));
    final CompactThetaSketch expected = copy.rebuild().compact(true, null);
    final long[] expectedHashes = hashesOf(expected);

    //case 1: ordered, not trimmed (the default) keeps every retained entry
    final CompactThetaSketch c1 = sk.compact(true, null);
    assertTrue(c1.isOrdered());
    assertEquals(c1.getRetainedEntries(true), retainedBefore);
    assertEquals(c1.getThetaLong(), thetaBefore);

    //case 2: unordered, not trimmed
    final CompactThetaSketch c2 = sk.compact(false, null);
    assertEquals(c2.getRetainedEntries(true), retainedBefore);
    assertEquals(c2.getThetaLong(), thetaBefore);

    //case 3: ordered and trimmed
    final CompactThetaSketch c3 = sk.compactTrimmed(true, null);
    assertTrue(c3.isOrdered());
    assertEquals(c3.getRetainedEntries(true), K);
    assertTrue(c3.getThetaLong() < thetaBefore); //new theta is the (k+1)th smallest hash
    assertEquals(c3.getThetaLong(), expected.getThetaLong());
    assertEquals(hashesOf(c3), expectedHashes);
    for (final long h : hashesOf(c3)) { assertTrue(h < c3.getThetaLong()); }

    //case 4: unordered and trimmed: same set and theta
    final CompactThetaSketch c4 = sk.compactTrimmed(false, null);
    assertEquals(c4.getRetainedEntries(true), K);
    assertEquals(c4.getThetaLong(), expected.getThetaLong());
    assertEquals(hashesOf(c4), expectedHashes);

    //the source sketch must be untouched by any of the four
    assertEquals(sk.getRetainedEntries(true), retainedBefore);
    assertEquals(sk.getThetaLong(), thetaBefore);
  }

  @Test
  public void trimConvertsExactModeToEstimation() {
    //an update sketch can retain more than k entries while still in exact mode:
    //nothing has been evicted yet, so theta is still 1.0 and the count is exact
    final int n = 5000;
    final UpdatableThetaSketch sk = sketchOf(n);
    assertTrue(sk.getRetainedEntries(true) > K);
    assertFalse(sk.isEstimationMode());
    assertEquals(sk.getTheta(), 1.0);

    //not trimming keeps every entry and the exact count
    final CompactThetaSketch exact = sk.compact(true, null);
    assertFalse(exact.isEstimationMode());
    assertEquals(exact.getRetainedEntries(true), n);
    assertEquals(exact.getEstimate(), (double)n);

    //trimming is lossy: it discards real data, lowers theta below 1.0 and the
    //result is an estimate carrying error where the source held an exact count
    final CompactThetaSketch trimmed = sk.compactTrimmed(true, null);
    assertTrue(trimmed.isEstimationMode());
    assertEquals(trimmed.getRetainedEntries(true), K);
    assertTrue(trimmed.getTheta() < 1.0);
    assertTrue(trimmed.getEstimate() != (double)n);
    //the estimate is still sound: n must lie inside the 3-sigma bounds
    assertTrue(trimmed.getLowerBound(3) <= n);
    assertTrue(trimmed.getUpperBound(3) >= n);
  }

  @Test
  public void trimWidensBoundsInEstimationMode() {
    //trimming is lossy even when the source is already estimating: it discards
    //retained entries, and the relative error scales with 1 / sqrt(retained)
    final UpdatableThetaSketch sk = sketchOf(40000);
    assertTrue(sk.isEstimationMode());
    assertTrue(sk.getRetainedEntries(true) > K);

    final CompactThetaSketch plain = sk.compact(true, null);
    final CompactThetaSketch trimmed = sk.compactTrimmed(true, null);
    assertEquals(trimmed.getRetainedEntries(true), K);
    assertTrue(plain.getRetainedEntries(true) > trimmed.getRetainedEntries(true));

    //fewer retained entries -> strictly wider confidence interval
    final double plainWidth = plain.getUpperBound(2) - plain.getLowerBound(2);
    final double trimmedWidth = trimmed.getUpperBound(2) - trimmed.getLowerBound(2);
    assertTrue(trimmedWidth > plainWidth);
  }

  @Test
  public void trimOnEmptyAndBelowK() {
    //empty: trimming changes nothing
    final UpdatableThetaSketch empty = sketchOf(0);
    final CompactThetaSketch emptyResult = empty.compactTrimmed(true, null);
    assertTrue(emptyResult.isEmpty());
    assertEquals(emptyResult.getRetainedEntries(true), 0);

    //below k: nothing to trim, theta and entries are preserved
    final UpdatableThetaSketch small = sketchOf(100);
    assertFalse(small.isEstimationMode());
    final CompactThetaSketch smallResult = small.compactTrimmed(true, null);
    assertFalse(smallResult.isEstimationMode());
    assertEquals(smallResult.getRetainedEntries(true), 100);
    assertEquals(smallResult.getThetaLong(), small.getThetaLong());
    assertEquals(smallResult.getEstimate(), 100.0);
  }

  @Test
  public void serializesIdenticallyToRebuildThenCompact() {
    //binary compatibility is the first priority: the trimmed result must serialize to exactly
    //the same bytes as the rebuild() + compact() path it replaces
    final UpdatableThetaSketch sk = sketchOf(8000);
    assertTrue(sk.getRetainedEntries(true) > K);

    final UpdatableThetaSketch copy = UpdatableThetaSketch.heapify(
        java.lang.foreign.MemorySegment.ofArray(sk.toByteArray()));
    final byte[] viaRebuild = copy.rebuild().compact(true, null).toByteArray();
    final byte[] viaTrim = sk.compactTrimmed(true, null).toByteArray();
    assertEquals(viaTrim, viaRebuild);

    //the no-arg convenience must match the explicit ordered/heap form
    assertEquals(sk.compactTrimmed().toByteArray(), viaTrim);
  }

  @Test
  public void alphaDoesNotSupportTrimming() {
    //Alpha maintains theta by its own discipline and never needs reducing to k
    final UpdatableThetaSketch alpha = UpdatableThetaSketch.builder()
        .setFamily(Family.ALPHA).setNominalEntries(512).build();
    for (int i = 0; i < 8000; i++) { alpha.update(i); }
    //plain compact still works
    final CompactThetaSketch plain = alpha.compact(true, null);
    assertTrue(plain.getRetainedEntries(true) > 0);
    try {
      alpha.compactTrimmed(true, null);
      fail("Alpha must reject trimming");
    } catch (final UnsupportedOperationException expected) { }
  }

}
