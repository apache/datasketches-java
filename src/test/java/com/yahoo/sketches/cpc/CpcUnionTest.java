/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static org.testng.Assert.fail;

import org.testng.annotations.Test;

import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesStateException;

/**
 * @author Lee Rhodes
 */
public class CpcUnionTest {

  @Test
  public void checkExceptions() {
    CpcSketch sk = new CpcSketch(10, 1);
    CpcUnion union = new CpcUnion(10);
    try {
      union.update(sk);
      fail();
    } catch (SketchesArgumentException e) {}
    sk = null;
    union.update(sk);
    union = null;
    try {
      CpcUnion.getBitMatrix(union);
      fail();
    } catch (SketchesStateException e) {}
    union = new CpcUnion(10);

  }

}
