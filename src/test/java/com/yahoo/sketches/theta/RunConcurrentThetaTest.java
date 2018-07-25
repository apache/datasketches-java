/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import org.testng.annotations.Test;

import com.yahoo.sketches.theta.ConcurrentThetaTest.CONCURRENCY_TYPE;

/**
 * @author Lee Rhodes
 */
public class RunConcurrentThetaTest {

  @SuppressWarnings("unused")
  @Test
  public void checkBaseLine() throws Exception {
    new ConcurrentThetaTest(CONCURRENCY_TYPE.BASELINE, 1, 0, 3);
  }

  @SuppressWarnings("unused")
  @Test
  public void checkLockBased() throws Exception {
    new ConcurrentThetaTest(CONCURRENCY_TYPE.LOCK_BASED, 4, 4, 30);
  }

  @SuppressWarnings("unused")
  @Test
  public void checkConcurrent() throws Exception {
    new ConcurrentThetaTest(CONCURRENCY_TYPE.CONCURRENT, 4, 4, 30);
  }

}
