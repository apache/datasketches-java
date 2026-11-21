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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.Comparator;

import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.quantilescommon.QuantilesGenericSketchIteratorAPI;
import org.testng.annotations.Test;

/**
 * Checks that a KLL items sketch stays consistent when its comparator throws during a merge.
 */
public class KllMergeFailureTest {
  private static final int K = 20;
  private static final long SEED = 12345;

  /** Counts comparisons, and throws once a given number of further comparisons have been made. */
  private static final class CountingComparator implements Comparator<String> {
    int count = 0;
    private int allowed = -1; //negative: never throws

    void failAfter(final int comparisons) { allowed = comparisons; }

    @Override
    public int compare(final String a, final String b) {
      if (allowed == 0) {
        allowed = -1;
        throw new IllegalStateException("comparator failure");
      }
      if (allowed > 0) { allowed--; }
      count++;
      return a.compareTo(b);
    }
  }

  private static String item(final int i) { return String.format("%06d", i); }

  private static KllItemsSketch<String>[] newPair(final CountingComparator comparator) {
    final ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();
    KllSketch.random.setSeed(SEED);
    final KllItemsSketch<String> mine = KllItemsSketch.newHeapInstance(K, comparator, serDe);
    for (int i = 0; i < 10 * K; i++) { mine.update(item(500_000 + i)); }
    //ascending, so the other sketch's higher levels hold its smallest items
    final KllItemsSketch<String> other = KllItemsSketch.newHeapInstance(K, comparator, serDe);
    for (int i = 0; i < 50 * K; i++) { other.update(item(i)); }
    @SuppressWarnings("unchecked")
    final KllItemsSketch<String>[] pair = new KllItemsSketch[] {mine, other};
    return pair;
  }

  //The last two comparisons of a merge find the new min and max. The comparator throws at the first
  //of them. The min and max must still bound every retained item, and N must equal the total weight.
  @Test
  public void comparatorFailureAtMinMaxLeavesSketchConsistent() {
    final CountingComparator counter = new CountingComparator();
    final KllItemsSketch<String>[] dry = newPair(counter);
    counter.count = 0;
    dry[0].merge(dry[1]);
    final int mergeComparisons = counter.count;

    final CountingComparator comparator = new CountingComparator();
    final KllItemsSketch<String>[] pair = newPair(comparator);
    final KllItemsSketch<String> sk = pair[0];
    comparator.failAfter(mergeComparisons - 2);
    try {
      sk.merge(pair[1]);
      fail("the comparator should have thrown");
    } catch (final IllegalStateException e) {
      //expected
    }

    final QuantilesGenericSketchIteratorAPI<String> it = sk.iterator();
    long weight = 0;
    while (it.next()) {
      final String q = it.getQuantile();
      assertTrue(q.compareTo(sk.getMinItem()) >= 0, q + " < min " + sk.getMinItem());
      assertTrue(q.compareTo(sk.getMaxItem()) <= 0, q + " > max " + sk.getMaxItem());
      weight += it.getWeight();
    }
    assertEquals(weight, sk.getN());
    sk.merge(pair[1]); //the sketch keeps working
  }
}
