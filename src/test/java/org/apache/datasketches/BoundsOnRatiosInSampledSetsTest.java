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

package org.apache.datasketches;

import static org.apache.datasketches.BoundsOnRatiosInSampledSets.checkInputs;
import static org.apache.datasketches.BoundsOnRatiosInSampledSets.getEstimateOfA;
import static org.apache.datasketches.BoundsOnRatiosInSampledSets.getEstimateOfB;
import static org.apache.datasketches.BoundsOnRatiosInSampledSets.getEstimateOfBoverA;
import static org.apache.datasketches.BoundsOnRatiosInSampledSets.getLowerBoundForBoverA;
import static org.apache.datasketches.BoundsOnRatiosInSampledSets.getUpperBoundForBoverA;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

@SuppressWarnings("javadoc")
public class BoundsOnRatiosInSampledSetsTest {

  @Test
  public void checkNormalReturns() {
    getLowerBoundForBoverA(500, 100, .1);
    getLowerBoundForBoverA(500, 100, 0.75);
    getLowerBoundForBoverA(500, 100, 1.0);
    assertEquals(getLowerBoundForBoverA(0, 0, .1), 0.0, 0.0);

    getUpperBoundForBoverA(500, 100, .1);
    getUpperBoundForBoverA(500, 100, 0.75);
    getUpperBoundForBoverA(500, 100, 1.0);
    assertEquals(getUpperBoundForBoverA(0, 0, .1), 1.0, 0.0);

    getEstimateOfBoverA(500,100);
    getEstimateOfA(500, .1);
    getEstimateOfB(100, .1);
    assertEquals(getEstimateOfBoverA(0, 0), .5, 0.0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInputA() {
    checkInputs(-1, 0, .3);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInputB() {
    checkInputs(500, -1, .3);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInputF() {
    checkInputs(500, 100, -1);
  }

}
