/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import org.testng.annotations.Test;

public class SketchesExceptionTest {

  @Test(expectedExceptions = SketchesException.class)
  public void checkSketchesException() {
    throw new SketchesException("This is a test.");
  }
  
  @Test(expectedExceptions = SketchesIllegalArgumentException.class)
  public void checkSketchesIllegalArgumentException() {
    throw new SketchesIllegalArgumentException("This is a test.");
  }
  
  @Test(expectedExceptions = SketchesIllegalStateException.class)
  public void checkSketchesIllegalStateException() {
    throw new SketchesIllegalStateException("This is a test.");
  }
}
