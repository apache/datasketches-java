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

package org.apache.datasketches.quantiles;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.Comparator;

import org.apache.datasketches.common.ArrayOfLongsSerDe;
import org.testng.annotations.Test;

/**
 * Checks that an update whose processing fails leaves a QuantilesItemsSketch as it was.
 * The comparator is made to throw while the full base buffer is sorted. The failed item is not added,
 * and the sketch must then match, byte for byte, a sketch that never saw that item.
 */
public class ItemsUpdateFailureTest {
  private static final int K = 16;
  private static final long SEED = 12345;
  private static final ArrayOfLongsSerDe SERDE = new ArrayOfLongsSerDe();

  /** A comparator that throws once a given number of further comparisons have been made. */
  private static final class FailingComparator implements Comparator<Long> {
    private int allowed = -1; //negative: never throws

    void failAfter(final int comparisons) { allowed = comparisons; }

    @Override
    public int compare(final Long a, final Long b) {
      if (allowed == 0) {
        allowed = -1;
        throw new IllegalStateException("comparator failure");
      }
      if (allowed > 0) { allowed--; }
      return Long.compare(a, b);
    }
  }

  //Item i of the stream; the order is mixed so the sort does real work.
  private static long item(final int i) { return ((i * 7919L) % 100_003L); }

  //The failing update is the one that fills the base buffer, so processing runs.
  //At n = 2k the levels are grown before the sort fails; at n = 6k no growth is needed.
  @Test
  public void sortFailureLeavesSketchUnchanged() {
    for (final int failAt : new int[] {(2 * K) - 1, (6 * K) - 1}) {
      checkSortFailure(failAt, 20 * K);
    }
  }

  private static void checkSortFailure(final int failAt, final int total) {
    QuantilesItemsSketch.rand.setSeed(SEED);
    final FailingComparator comparator = new FailingComparator();
    final QuantilesItemsSketch<Long> sk = QuantilesItemsSketch.getInstance(Long.class, K, comparator);
    for (int i = 0; i < total; i++) {
      if (i == failAt) {
        comparator.failAfter(2); //the max and min checks pass, the sort fails
        try {
          sk.update(item(i));
          fail("the comparator should have thrown");
        } catch (final IllegalStateException e) {
          //expected
        }
        assertEquals(sk.getN(), failAt);
        assertEquals(sk.getBaseBufferCount(), failAt % (2 * K));
        continue;
      }
      sk.update(item(i));
    }

    QuantilesItemsSketch.rand.setSeed(SEED);
    final QuantilesItemsSketch<Long> ref = QuantilesItemsSketch.getInstance(Long.class, K, Comparator.naturalOrder());
    for (int i = 0; i < total; i++) {
      if (i != failAt) { ref.update(item(i)); }
    }

    assertEquals(sk.getN(), total - 1);
    assertEquals(sk.getBitPattern(), ref.getBitPattern());
    assertEquals(sk.getBaseBufferCount(), ref.getBaseBufferCount());
    assertEquals(sk.getMinItem(), ref.getMinItem());
    assertEquals(sk.getMaxItem(), ref.getMaxItem());
    assertEquals(sk.toByteArray(SERDE), ref.toByteArray(SERDE));
    assertEquals(sk.getQuantile(0.5), ref.getQuantile(0.5));
  }

  //A comparator failure in the max or min check also leaves the sketch unchanged,
  //including the max when only the min check fails.
  @Test
  public void minMaxFailureLeavesSketchUnchanged() {
    final FailingComparator comparator = new FailingComparator();
    final QuantilesItemsSketch<Long> sk = QuantilesItemsSketch.getInstance(Long.class, K, comparator);
    sk.update(10L);
    sk.update(20L);
    comparator.failAfter(1); //the max check passes, the min check fails
    try {
      sk.update(30L);
      fail("the comparator should have thrown");
    } catch (final IllegalStateException e) {
      //expected
    }
    assertEquals(sk.getN(), 2);
    assertEquals(sk.getBaseBufferCount(), 2);
    assertEquals(sk.getMaxItem(), Long.valueOf(20L));
    assertEquals(sk.getMinItem(), Long.valueOf(10L));
    sk.update(30L);
    assertEquals(sk.getMaxItem(), Long.valueOf(30L));
    assertTrue(sk.getN() == 3);
  }
}
