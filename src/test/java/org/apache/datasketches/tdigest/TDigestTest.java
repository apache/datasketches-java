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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class TDigestTest {

  @Test
  public void empty() {
    final TDigest td = new TDigest(100);
    assertTrue(td.isEmpty());
    assertEquals(td.getK(), 100);
    assertEquals(td.getTotalWeight(), 0);
    assertThrows(IllegalArgumentException.class, () -> td.getMinValue());
    assertThrows(IllegalArgumentException.class, () -> td.getMaxValue());
    assertThrows(IllegalArgumentException.class, () -> td.getRank(0));
    assertThrows(IllegalArgumentException.class, () -> td.getQuantile(0.5));
  }

  @Test
  public void oneValue() {
    final TDigest td = new TDigest(100);
    td.update(1);
    assertFalse(td.isEmpty());
    assertEquals(td.getK(), 100);
    assertEquals(td.getTotalWeight(), 1);
    assertEquals(td.getMinValue(), 1);
    assertEquals(td.getMaxValue(), 1);
    assertEquals(td.getRank(0.99), 0);
    assertEquals(td.getRank(1), 0.5);
    assertEquals(td.getRank(1.01), 1);
    assertEquals(td.getQuantile(0), 1);
    assertEquals(td.getQuantile(0.5), 1);
    assertEquals(td.getQuantile(1), 1);
  }

  @Test
  public void manyValues() {
    final TDigest td = new TDigest(100);
    final int n = 10000;
    for (int i = 0; i < n; i++) td.update(i);
//    System.out.println(td.toString(true));
//    td.compress();
//    System.out.println(td.toString(true));
    assertFalse(td.isEmpty());
    assertEquals(td.getTotalWeight(), n);
    assertEquals(td.getMinValue(), 0);
    assertEquals(td.getMaxValue(), n - 1);
    assertEquals(td.getRank(0), 0, 0.0001);
    assertEquals(td.getRank(n / 4), 0.25, 0.0001);
    assertEquals(td.getRank(n / 2), 0.5, 0.0001);
    assertEquals(td.getRank(n * 3 / 4), 0.75, 0.0001);
    assertEquals(td.getRank(n), 1);
    assertEquals(td.getQuantile(0), 0);
    assertEquals(td.getQuantile(0.5), n / 2, n / 2 * 0.01);
    assertEquals(td.getQuantile(0.9), n * 0.9, n * 0.9 * 0.01);
    assertEquals(td.getQuantile(0.95), n * 0.95, n * 0.95 * 0.01);
    assertEquals(td.getQuantile(1), n - 1);
  }

  @Test
  public void mergeSmall() {
    final TDigest td1 = new TDigest(100);
    td1.update(1);
    td1.update(2);
    final TDigest td2 = new TDigest(100);
    td2.update(2);
    td2.update(3);
    td1.merge(td2);
    assertEquals(td1.getTotalWeight(), 4);
    assertEquals(td1.getMinValue(), 1);
    assertEquals(td1.getMaxValue(), 3);
  }

  @Test
  public void mergeLarge() {
    final int n = 10000;
    final TDigest td1 = new TDigest(100);
    final TDigest td2 = new TDigest(100);
    for (int i = 0; i < n / 2; i++) {
      td1.update(i);
      td2.update(n / 2 + i);
    }
    td1.merge(td2);
    assertEquals(td1.getTotalWeight(), n);
    assertEquals(td1.getMinValue(), 0);
    assertEquals(td1.getMaxValue(), n - 1);
//    System.out.println(td1.toString(true));
  }

}
