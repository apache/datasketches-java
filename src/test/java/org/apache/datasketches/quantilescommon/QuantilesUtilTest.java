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

package org.apache.datasketches.quantilescommon;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import org.apache.datasketches.common.SketchesArgumentException;
import org.testng.annotations.Test;

public class QuantilesUtilTest {

  @Test
  public void checkEvenlySpacedRanks() {
    double[] arr = QuantilesUtil.equallyWeightedRanks(2);
    assertEquals(arr[1], 0.5);
    arr = QuantilesUtil.equallyWeightedRanks(4);
    assertEquals(arr[0], 0.0);
    assertEquals(arr[1], 0.25);
    assertEquals(arr[2], 0.5);
    assertEquals(arr[3], 0.75);
    assertEquals(arr[4], 1.0);
  }

  @Test
  public void checkEvenlySpacedFloats() {
    float[] arr = QuantilesUtil.evenlySpacedFloats(0, 1, 3);
    assertEquals(arr[0], 0.0f);
    assertEquals(arr[1], 0.5f);
    assertEquals(arr[2], 1.0f);
    arr = QuantilesUtil.evenlySpacedFloats(3, 7, 3);
    assertEquals(arr[0], 3.0f);
    assertEquals(arr[1], 5.0f);
    assertEquals(arr[2], 7.0f);
    arr = QuantilesUtil.evenlySpacedFloats(0f, 1.0f, 2);
    assertEquals(arr[0], 0f);
    assertEquals(arr[1], 1f);
    try { QuantilesUtil.evenlySpacedFloats(0f, 1f, 1); fail(); } catch (SketchesArgumentException e) {}
  }

  @Test
  public void checkEvenlySpacedDoubles() {
    double[] arr = QuantilesUtil.evenlySpacedDoubles(0, 1, 3);
    assertEquals(arr[0], 0.0);
    assertEquals(arr[1], 0.5);
    assertEquals(arr[2], 1.0);
    arr = QuantilesUtil.evenlySpacedDoubles(3, 7, 3);
    assertEquals(arr[0], 3.0);
    assertEquals(arr[1], 5.0);
    assertEquals(arr[2], 7.0);
    arr = QuantilesUtil.evenlySpacedDoubles(0, 1.0, 2);
    assertEquals(arr[0], 0);
    assertEquals(arr[1], 1.0);
    try { QuantilesUtil.evenlySpacedDoubles(0, 1.0, 1); fail(); } catch (SketchesArgumentException e) {}
  }

  @Test
  public void checkEvenlyLogSpaced() {
    final double[] arr = QuantilesUtil.evenlyLogSpaced(1, 8, 4);
    assertEquals(arr[0], 1.0);
    assertEquals(arr[1], 2.0);
    assertEquals(arr[2], 4.0);
    assertEquals(arr[3], 8.0);
    try { QuantilesUtil.evenlyLogSpaced(-1.0, 1.0, 2); fail(); } catch (SketchesArgumentException e) {}
    try { QuantilesUtil.evenlyLogSpaced(1.0, -1.0, 2); fail(); } catch (SketchesArgumentException e) {}
    try { QuantilesUtil.evenlyLogSpaced(-1.0, -1.0, 2); fail(); } catch (SketchesArgumentException e) {}
    try { QuantilesUtil.evenlyLogSpaced(1.0, 1.0, 1); fail(); } catch (SketchesArgumentException e) {}
  }

}

