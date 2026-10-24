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

package org.apache.datasketches.tdigest;

import static org.testng.Assert.assertEquals;

import java.util.Arrays;
import java.util.Random;

import org.apache.datasketches.common.Shuffle;
import org.testng.annotations.Test;

public class SortTest {

  @Test
  public void smallWithRepetition() {
    final double[] keys = {3, 1, 4, 2, 1};
    final long[] values = {4, 1, 5, 3, 2};
    Sort.stableSort(keys, values, keys.length);
    assertEquals(keys[0], 1);
    assertEquals(keys[1], 1);
    assertEquals(keys[2], 2);
    assertEquals(keys[3], 3);
    assertEquals(keys[4], 4);
    assertEquals(values[0], 1);
    assertEquals(values[1], 2);
    assertEquals(values[2], 3);
    assertEquals(values[3], 4);
    assertEquals(values[4], 5);
  }

  @Test
  public void large() {
    final int n = 1000;
    final double[] keys = new double[n];
    final long[] values = new long[n];
    for (int i = 0; i < n; i++) {
      values[i] = i;
    }
    Shuffle.shuffle(values);
    for (int i = 0; i < n; i++) {
      keys[i] = values[i];
    }
    Sort.stableSort(keys, values, n);
    for (int i = 0; i < n; i++) {
      assertEquals(keys[i], i);
      assertEquals(values[i], i);
    }
  }

  @Test
  public void equalKeysAreOrderedByWeight() {
    // the example from the issue: (200,1) (100,3) (300,2) (100,1)
    final double[] keys = {200, 100, 300, 100};
    final long[] values = {1, 3, 2, 1};
    Sort.stableSort(keys, values, keys.length);
    assertEquals(keys, new double[] {100, 100, 200, 300});
    assertEquals(values, new long[] {1, 3, 1, 2});
  }

  @Test
  public void resultDoesNotDependOnInputOrder() {
    // many duplicate keys with different weights, more elements than the insertion sort limit,
    // so that both the quicksort and the insertion sort are exercised
    final int n = 1000;
    final Random rnd = new Random(1);
    final double[] origKeys = new double[n];
    final long[] origValues = new long[n];
    for (int i = 0; i < n; i++) {
      origKeys[i] = rnd.nextInt(20);
      origValues[i] = 1 + rnd.nextInt(1000);
    }
    double[] expectedKeys = null;
    long[] expectedValues = null;
    for (int iteration = 0; iteration < 20; iteration++) {
      final double[] keys = origKeys.clone();
      final long[] values = origValues.clone();
      // shuffle both arrays with the same permutation
      for (int i = n - 1; i > 0; i--) {
        final int j = rnd.nextInt(i + 1);
        final double tmpKey = keys[i];
        keys[i] = keys[j];
        keys[j] = tmpKey;
        final long tmpValue = values[i];
        values[i] = values[j];
        values[j] = tmpValue;
      }
      Sort.stableSort(keys, values, n);
      if (expectedKeys == null) {
        expectedKeys = keys;
        expectedValues = values;
      } else {
        assertEquals(keys, expectedKeys);
        assertEquals(values, expectedValues);
      }
    }
  }

  @Test
  public void matchesReferenceSort() {
    final Random rnd = new Random(2);
    final int[] sizes = {0, 1, 2, 10, 63, 64, 65, 66, 200, 5000};
    final int[] distinctKeys = {1, 3, 100, 100000};
    for (final int n : sizes) {
      for (final int d : distinctKeys) {
        final double[] keys = new double[n];
        final long[] values = new long[n];
        final long[][] pairs = new long[n][];
        for (int i = 0; i < n; i++) {
          keys[i] = rnd.nextInt(d) - d / 2;
          values[i] = 1 + rnd.nextInt(7);
          pairs[i] = new long[] {(long) keys[i], values[i]};
        }
        // reference: order by key, then by weight
        Arrays.sort(pairs, (x, y) -> x[0] != y[0] ? Long.compare(x[0], y[0]) : Long.compare(x[1], y[1]));
        Sort.stableSort(keys, values, n);
        for (int i = 0; i < n; i++) {
          assertEquals((long) keys[i], pairs[i][0]);
          assertEquals(values[i], pairs[i][1]);
        }
      }
    }
  }

  @Test
  public void sortsOnlyRequestedPrefix() {
    final double[] keys = {5, 3, 4, 1, 99, 98};
    final long[] values = {1, 1, 1, 1, 7, 7};
    Sort.stableSort(keys, values, 4);
    assertEquals(keys, new double[] {1, 3, 4, 5, 99, 98});
    assertEquals(values, new long[] {1, 1, 1, 1, 7, 7});
  }

}
