/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import static com.yahoo.sketches.BoundsOnRatiosInSampledSets.*;

import org.testng.annotations.Test;

public class BoundsOnRatiosInSampledSetsTest {

  @Test
  public void checkNormalReturns() {
    getLowerBoundForBoverA(500, 100, 1.0, .1);
    getLowerBoundForBoverA(500, 100, 0, 1.0);
    getUpperBoundForBoverA(500, 100, 1.0, .1);
    getUpperBoundForBoverA(500, 100, 0, 1.0);
    getEstimateOfBoverA(500,100);
    getEstimateOfA(500, .1);
    getEstimateOfB(100, .1);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkInputA() {
    checkInputs(-1, 0, 1.0, .3);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkInputB() {
    checkInputs(500, -1, 1.0, .3);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkInputStdDev() {
    checkInputs(500, 100, -1, .3);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkInputStdDev2() {
    checkInputs(500, 100, 4, .3);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkInputF() {
    checkInputs(500, 100, 2, -1);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkInputF2() {
    checkInputs(500, 100, 2, .75);
  }
}
