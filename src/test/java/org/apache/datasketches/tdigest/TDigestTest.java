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
  }

  @Test
  public void manyValues() {
    final TDigest td = new TDigest(100);
    final int n = 10000;
    for (int i = 0; i < n; i++) td.update(i);
    System.out.println(td.toString(true));
    td.compress();
    System.out.println(td.toString(true));
    assertFalse(td.isEmpty());
    assertEquals(td.getTotalWeight(), n);
    assertEquals(td.getMinValue(), 0);
    assertEquals(td.getMaxValue(), n - 1);
  }

}
