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

}
