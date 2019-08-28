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

package org.apache.datasketches.cpc;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

import org.apache.datasketches.Family;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.SketchesStateException;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class CpcUnionTest {

  @Test
  public void checkExceptions() {
    CpcSketch sk = new CpcSketch(10, 1);
    CpcUnion union = new CpcUnion();
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
    assertEquals(Family.CPC, CpcUnion.getFamily());

  }

  @Test
  public void checkReduceK() {
    CpcUnion union = new CpcUnion(12);
    CpcSketch sk = new CpcSketch(11);
    int u = 1;
    sk.update(u);
    union.update(sk);
    CpcUnion.getBitMatrix(union);
    CpcSketch sk2 = new CpcSketch(10);
    int shTrans = ((3 * 512) / 32); //sparse-hybrid transition for lgK=9
    while (sk2.numCoupons < shTrans) { sk2.update(++u); }
    union.update(sk2);
    CpcSketch sk3 = new CpcSketch(9);
    sk3.update(++u);
    union.update(sk3);
    CpcSketch sk4 = new CpcSketch(8);
    sk4.update(++u);
    union.update(sk4);
  }

}
