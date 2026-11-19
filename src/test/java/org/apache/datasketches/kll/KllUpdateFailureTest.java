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

package org.apache.datasketches.kll;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;

import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.common.MemorySegmentRequest;
import org.testng.annotations.Test;

/**
 * Checks that an update whose compaction fails leaves a KLL sketch consistent:
 * the item is not added, and min and max are unchanged.
 */
public class KllUpdateFailureTest {
  private static final int K = 20;
  private static final long SEED = 12345;

  /** A MemorySegmentRequest that refuses every request. */
  private static final MemorySegmentRequest REFUSE = new MemorySegmentRequest() {
    @Override
    public MemorySegment request(final long newByteSize) {
      throw new IllegalStateException("no more space");
    }
  };

  //A direct sketch whose segment cannot grow: the update that needs a new level fails,
  //and the segment must still hold the sketch as it was before that update.
  @Test
  public void directDoublesRefusedGrowthLeavesSegmentUnchanged() {
    final MemorySegment seg = MemorySegment.ofArray(KllHelper.toByteArray(KllDoublesSketch.newHeapInstance(K), true));
    final KllDoublesSketch sk = KllDoublesSketch.newDirectInstance(K, seg, REFUSE);
    for (int i = 0; i < K; i++) { sk.update(i); }
    final byte[] before = seg.toArray(JAVA_BYTE);
    try {
      sk.update(1000.0); //a new maximum
      fail("the segment request should have been refused");
    } catch (final IllegalStateException e) {
      //expected
    }
    assertEquals(seg.toArray(JAVA_BYTE), before);
    assertEquals(sk.getN(), K);
    assertEquals(sk.getMaxItem(), K - 1.0);
    final KllDoublesSketch copy = KllDoublesSketch.heapify(seg);
    assertEquals(copy.getN(), K);
    assertEquals(copy.getNumLevels(), 1);
    assertEquals(copy.getMaxItem(), K - 1.0);
  }

  @Test
  public void directFloatsRefusedGrowthLeavesSegmentUnchanged() {
    final MemorySegment seg = MemorySegment.ofArray(KllHelper.toByteArray(KllFloatsSketch.newHeapInstance(K), true));
    final KllFloatsSketch sk = KllFloatsSketch.newDirectInstance(K, seg, REFUSE);
    for (int i = 0; i < K; i++) { sk.update(i); }
    final byte[] before = seg.toArray(JAVA_BYTE);
    try {
      sk.update(-1000.0f); //a new minimum
      fail("the segment request should have been refused");
    } catch (final IllegalStateException e) {
      //expected
    }
    assertEquals(seg.toArray(JAVA_BYTE), before);
    assertEquals(sk.getN(), K);
    assertEquals(sk.getMinItem(), 0.0f);
    assertEquals(KllFloatsSketch.heapify(seg).getNumLevels(), 1);
  }

  @Test
  public void directLongsRefusedGrowthLeavesSegmentUnchanged() {
    final MemorySegment seg = MemorySegment.ofArray(KllHelper.toByteArray(KllLongsSketch.newHeapInstance(K), true));
    final KllLongsSketch sk = KllLongsSketch.newDirectInstance(K, seg, REFUSE);
    for (int i = 0; i < K; i++) { sk.update(i); }
    final byte[] before = seg.toArray(JAVA_BYTE);
    try {
      sk.update(1000L); //a new maximum
      fail("the segment request should have been refused");
    } catch (final IllegalStateException e) {
      //expected
    }
    assertEquals(seg.toArray(JAVA_BYTE), before);
    assertEquals(sk.getN(), K);
    assertEquals(sk.getMaxItem(), K - 1L);
    assertEquals(KllLongsSketch.heapify(seg).getNumLevels(), 1);
  }

  /** A comparator that throws once a given number of further comparisons have been made. */
  private static final class FailingComparator implements Comparator<String> {
    private int allowed = -1; //negative: never throws

    void failAfter(final int comparisons) { allowed = comparisons; }

    @Override
    public int compare(final String a, final String b) {
      if (allowed == 0) {
        allowed = -1;
        throw new IllegalStateException("comparator failure");
      }
      if (allowed > 0) { allowed--; }
      return a.compareTo(b);
    }
  }

  private static String item(final int i) { return String.format("%06d", (i * 7919) % 100_003); }

  //The comparator throws while level 0 is sorted for compaction. The failed item is not added,
  //min and max are unchanged, and after more updates the sketch matches one that never saw that item.
  @Test
  public void itemsComparatorFailureLeavesSketchConsistent() {
    final ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();
    final int failAt = K; //the update that finds level 0 full
    final int total = 50 * K;

    KllSketch.random.setSeed(SEED);
    final FailingComparator comparator = new FailingComparator();
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(K, comparator, serDe);
    for (int i = 0; i < total; i++) {
      if (i == failAt) {
        final String min = sk.getMinItem();
        final String max = sk.getMaxItem();
        comparator.failAfter(2); //the min and max checks pass, the sort fails
        try {
          sk.update("~"); //sorts after every item, so a new maximum
          fail("the comparator should have thrown");
        } catch (final IllegalStateException e) {
          //expected
        }
        assertEquals(sk.getN(), failAt);
        assertEquals(sk.getMinItem(), min);
        assertEquals(sk.getMaxItem(), max);
      }
      sk.update(item(i));
    }

    KllSketch.random.setSeed(SEED);
    final KllItemsSketch<String> ref = KllItemsSketch.newHeapInstance(K, Comparator.naturalOrder(), serDe);
    for (int i = 0; i < total; i++) { ref.update(item(i)); }

    assertEquals(sk.getN(), total);
    assertEquals(sk.getMinItem(), ref.getMinItem());
    assertEquals(sk.getMaxItem(), ref.getMaxItem());
    assertEquals(sk.toByteArray(), ref.toByteArray());
  }
}
