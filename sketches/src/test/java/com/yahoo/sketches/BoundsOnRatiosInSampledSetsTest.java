/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import static com.yahoo.sketches.BoundsOnRatiosInSampledSets.checkInputs;
import static com.yahoo.sketches.BoundsOnRatiosInSampledSets.getEstimateOfA;
import static com.yahoo.sketches.BoundsOnRatiosInSampledSets.getEstimateOfB;
import static com.yahoo.sketches.BoundsOnRatiosInSampledSets.getEstimateOfBoverA;
import static com.yahoo.sketches.BoundsOnRatiosInSampledSets.getLowerBoundForBoverA;
import static com.yahoo.sketches.BoundsOnRatiosInSampledSets.getUpperBoundForBoverA;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

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
