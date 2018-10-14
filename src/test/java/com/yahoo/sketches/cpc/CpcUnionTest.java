/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
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

  @Test
  public void checkGetters() {
    int lgK = 10;
    CpcUnion union = new CpcUnion(lgK);
    assertEquals(union.getLgK(), lgK);
    assertEquals(union.getNumCoupons(), 0L);
    CpcSketch sk = new CpcSketch(lgK);
    for (int i = 0; i <= (4 << lgK); i++) { sk.update(i); }
    union.update(sk);
    assertTrue(union.getNumCoupons() > 0);
    assertTrue(CpcUnion.getBitMatrix(union) != null);

  }

}
