/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.frequencies;

import static org.testng.Assert.assertNull;

import org.testng.annotations.Test;

import com.yahoo.sketches.SketchesArgumentException;

public class ReversePurgeLongHashMapTest {

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkgetInstanceString() {
    ReversePurgeLongHashMap.getInstance("");
  }

  @Test
  public void checkActiveNull() {
    ReversePurgeLongHashMap map = new ReversePurgeLongHashMap(4);
    assertNull(map.getActiveKeys());
    assertNull(map.getActiveValues());
  }

}
