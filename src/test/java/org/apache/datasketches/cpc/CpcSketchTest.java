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

import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;
import static org.apache.datasketches.cpc.TestUtil.specialEquals;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.PrintStream;

import org.apache.datasketches.Family;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class CpcSketchTest {
  static PrintStream ps = System.out;

  @Test
  public void checkUpdatesEstimate() {
    final CpcSketch sk = new CpcSketch(10, 0);
    println(sk.toString(true));
    assertEquals(sk.getFormat(), Format.EMPTY_HIP);
    sk.update(1L);
    sk.update(2.0);
    sk.update("3");
    sk.update(new byte[] { 4 });
    sk.update(new char[] { 5 });
    sk.update(new int[] { 6 });
    sk.update(new long[] { 7 });
    final double est = sk.getEstimate();
    final double lb = sk.getLowerBound(2);
    final double ub = sk.getUpperBound(2);
    assertTrue(lb >= 0);
    assertTrue(lb <= est);
    assertTrue(est <= ub);
    assertEquals(sk.getFlavor(), Flavor.SPARSE);
    assertEquals(sk.getFormat(), Format.SPARSE_HYBRID_HIP);
    println(sk.toString());
    println(sk.toString(true));
  }

  @Test
  public void checkEstimatesWithMerge() {
    final int lgK = 4;
    final CpcSketch sk1 = new CpcSketch(lgK);
    final CpcSketch sk2 = new CpcSketch(lgK);
    final int n = 1 << lgK;
    for (int i = 0; i < n; i++ ) {
      sk1.update(i);
      sk2.update(i + n);
    }
    final CpcUnion union = new CpcUnion(lgK);
    union.update(sk1);
    union.update(sk2);
    final CpcSketch result = union.getResult();
    final double est = result.getEstimate();
    final double lb = result.getLowerBound(2);
    final double ub = result.getUpperBound(2);
    assertTrue(lb >= 0);
    assertTrue(lb <= est);
    assertTrue(est <= ub);
    assertTrue(result.validate());
    println(result.toString(true));
  }

  @Test
  public void checkCornerCaseUpdates() {
    final int lgK = 4;
    final CpcSketch sk = new CpcSketch(lgK);
    sk.update(0.0);
    sk.update(-0.0);
    int est = (int) Math.round(sk.getEstimate());
    assertEquals(est, 1);
    String s = null;
    sk.update(s);
    s = "";
    sk.update(s);
    est = (int) Math.round(sk.getEstimate());
    assertEquals(est, 1);

    byte[] barr = null;
    sk.update(barr);
    est = (int) Math.round(sk.getEstimate());
    assertEquals(est, 1);
    barr = new byte[0];
    est = (int) Math.round(sk.getEstimate());
    assertEquals(est, 1);

    char[] carr = null;
    sk.update(carr);
    est = (int) Math.round(sk.getEstimate());
    assertEquals(est, 1);
    carr = new char[0];
    est = (int) Math.round(sk.getEstimate());
    assertEquals(est, 1);

    int[] iarr = null;
    sk.update(iarr);
    est = (int) Math.round(sk.getEstimate());
    assertEquals(est, 1);
    iarr = new int[0];
    est = (int) Math.round(sk.getEstimate());
    assertEquals(est, 1);

    long[] larr = null;
    sk.update(larr);
    est = (int) Math.round(sk.getEstimate());
    assertEquals(est, 1);
    larr = new long[0];
    est = (int) Math.round(sk.getEstimate());
    assertEquals(est, 1);
  }

  @Test
  public void checkCornerHashUpdates() {
    final CpcSketch sk = new CpcSketch(26);
    final long hash1 = 0;
    final long hash0 = -1L;
    sk.hashUpdate(hash0, hash1);
    final PairTable table = sk.pairTable;
    println(table.toString(true));
  }

  @SuppressWarnings("unused")
  @Test
  public void checkCopyWithWindow() {
    final int lgK = 4;
    final CpcSketch sk = new CpcSketch(lgK);
    CpcSketch sk2 = sk.copy();
    for (int i = 0; i < (1 << lgK); i++) { //pinned
      sk.update(i);
    }
    sk2 = sk.copy();
    final long[] bitMatrix = CpcUtil.bitMatrixOfSketch(sk);
    CpcSketch.refreshKXP(sk, bitMatrix);
  }

  @Test
  public void checkFamily() {
    assertEquals(CpcSketch.getFamily(), Family.CPC);
  }

  @Test
  public void checkLgK() {
    CpcSketch sk = new CpcSketch(10);
    assertEquals(sk.getLgK(), 10);
    try {
      sk = new CpcSketch(3);
      fail();
    } catch (final SketchesArgumentException e) { }
  }

  @Test
  public void checkIconHipUBLBLg15() {
    CpcConfidence.getIconConfidenceUB(15, 1, 2);
    CpcConfidence.getIconConfidenceLB(15, 1, 2);
    CpcConfidence.getHipConfidenceUB(15, 1, 1.0, 2);
    CpcConfidence.getHipConfidenceLB(15, 1, 1.0, 2);
  }

  @Test
  public void checkHeapify() {
    int lgK = 10;
    CpcSketch sk = new CpcSketch(lgK, DEFAULT_UPDATE_SEED);
    assertTrue(sk.isEmpty());
    byte[] byteArray = sk.toByteArray();
    CpcSketch sk2 = CpcSketch.heapify(byteArray, DEFAULT_UPDATE_SEED);
    assertTrue(specialEquals(sk2, sk, false, false));
  }

  @Test
  public void checkHeapify2() {
    int lgK = 10;
    CpcSketch sk = new CpcSketch(lgK);
    assertTrue(sk.isEmpty());
    byte[] byteArray = sk.toByteArray();
    Memory mem = Memory.wrap(byteArray);
    CpcSketch sk2 = CpcSketch.heapify(mem);
    assertTrue(specialEquals(sk2, sk, false, false));
  }

  @Test
  public void checkRowColUpdate() {
    int lgK = 10;
    CpcSketch sk = new CpcSketch(lgK, DEFAULT_UPDATE_SEED);
    sk.rowColUpdate(0);
    assertEquals(sk.getFlavor(), Flavor.SPARSE);
  }

  @Test
  public void checkGetMaxSize() {
    final int size4  = CpcSketch.getMaxSerializedBytes(4);
    final int size26 = CpcSketch.getMaxSerializedBytes(26);
    assertEquals(size4, 24 + 40);
    assertEquals(size26, (int) ((0.6 * (1 << 26)) + 40));
  }

  @Test
  void negative_int_equivalence() throws Exception {
    CpcSketch sketch = new CpcSketch();
    byte v1 = (byte) -1;
    sketch.update(v1);
    short v2 = -1;
    sketch.update(v2);
    int v3 = -1;
    sketch.update(v3);
    long v4 = -1;
    sketch.update(v4);
    assertEquals(sketch.getEstimate(), 1, 0.01);
    // to compare with C++
    //org.apache.datasketches.tuple.TestUtil.writeBytesToFile(sketch.toByteArray(), "cpc-negative-one.sk");
  }

  /**
   * @param s the string to print
   */
  private static void println(String s) {
    //ps.println(s);  //disable here
  }

}
