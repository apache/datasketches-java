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

import static org.apache.datasketches.kll.KllHelper.getAllLevelStatsGivenN;
import static org.apache.datasketches.kll.KllHelper.getLevelStats;
import static org.testng.Assert.assertEquals;

import org.apache.datasketches.kll.KllHelper.LevelStats;
import org.testng.annotations.Test;

public class KllHelperTest {

  @Test //convert two false below to true for visual checking
  public void testGetAllLevelStats() {
    long n = 1L << 30;
    int k = 200;
    int m = 8;
    LevelStats lvlStats = getAllLevelStatsGivenN(k, m, n, false, false, true);
    assertEquals(lvlStats.getCompactBytes(), 5708);
  }

  @Test //convert two false below to true for visual checking
  public void getStatsAtNumLevels() {
    int k = 200;
    int m = 8;
    int numLevels = 23;
    LevelStats lvlStats = getLevelStats(k, m, numLevels, false, false, true);
    assertEquals(lvlStats.getCompactBytes(), 5708);
  }
}
