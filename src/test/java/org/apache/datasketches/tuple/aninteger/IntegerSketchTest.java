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

package org.apache.datasketches.tuple.aninteger;

import static org.testng.Assert.assertEquals;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.tuple.AnotB;
import org.apache.datasketches.tuple.CompactSketch;
import org.apache.datasketches.tuple.Intersection;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class IntegerSketchTest {

  @SuppressWarnings("deprecation")
  @Test
  public void serDeTest() {
    final int lgK = 12;
    final int K = 1 << lgK;
    final IntegerSummary.Mode a1Mode = IntegerSummary.Mode.AlwaysOne;
    final IntegerSketch a1Sk = new IntegerSketch(lgK, a1Mode);
    final int m = 2 * K;
    for (int i = 0; i < m; i++) {
      a1Sk.update(i, 1);
    }
    final double est1 = a1Sk.getEstimate();
    final Memory mem = Memory.wrap(a1Sk.toByteArray());
    final IntegerSketch a1Sk2 = new IntegerSketch(mem, a1Mode);
    final double est2 = a1Sk2.getEstimate();
    assertEquals(est1, est2);
  }

  @Test
  public void intersectTest() {
    final int lgK = 12;
    final int K = 1 << lgK;
    final IntegerSummary.Mode a1Mode = IntegerSummary.Mode.AlwaysOne;
    final IntegerSketch a1Sk1 = new IntegerSketch(lgK, a1Mode);
    final IntegerSketch a1Sk2 = new IntegerSketch(lgK, a1Mode);
    final int m = 2 * K;
    for (int i = 0; i < m; i++) {
      a1Sk1.update(i, 1);
      a1Sk2.update(i + m/2, 1);
    }
    final Intersection<IntegerSummary> inter =
        new Intersection<>(new IntegerSummarySetOperations(a1Mode, a1Mode));
    inter.intersect(a1Sk1);
    inter.intersect(a1Sk2);
    final CompactSketch<IntegerSummary> csk = inter.getResult();
    assertEquals(csk.getEstimate(), K * 1.0, K * .03);
  }

  @Test
  public void aNotBTest() {
    final int lgK = 4;
    final int u = 5;
    final IntegerSummary.Mode a1Mode = IntegerSummary.Mode.AlwaysOne;
    final IntegerSketch a1Sk1 = new IntegerSketch(lgK, a1Mode);
    final IntegerSketch a1Sk2 = null;//new IntegerSketch(lgK, a1Mode);
    final AnotB<IntegerSummary> anotb = new AnotB<>();
    for (int i = 0; i < u; i++) {
      a1Sk1.update(i, 1);
    }
    anotb.setA(a1Sk1);
    anotb.notB(a1Sk2);
    final CompactSketch<IntegerSummary> cSk = anotb.getResult(true);
    assertEquals((int)cSk.getEstimate(), u);
  }

  @Test
  public void checkMinMaxMode() {
    final int lgK = 12;
    final int K = 1 << lgK;
    final IntegerSummary.Mode minMode = IntegerSummary.Mode.Min;
    final IntegerSummary.Mode maxMode = IntegerSummary.Mode.Max;
    final IntegerSketch a1Sk1 = new IntegerSketch(lgK, minMode);
    final IntegerSketch a1Sk2 = new IntegerSketch(lgK, maxMode);
    final int m = K / 2;
    for (int key = 0; key < m; key++) {
      a1Sk1.update(key, 1);
      a1Sk1.update(key, 0);
      a1Sk1.update(key, 2);
      a1Sk2.update(key + m/2, 1);
      a1Sk2.update(key + m/2, 0);
      a1Sk2.update(key + m/2, 2);
    }
    final double est1 = a1Sk1.getEstimate();
    final double est2 = a1Sk2.getEstimate();
    assertEquals(est1, est2);
  }

  @Test
  public void checkStringKey() {
    final int lgK = 12;
    final int K = 1 << lgK;
    final IntegerSummary.Mode a1Mode = IntegerSummary.Mode.AlwaysOne;
    final IntegerSketch a1Sk1 = new IntegerSketch(lgK, a1Mode);
    final int m = K / 2;
    for (int key = 0; key < m; key++) {
      a1Sk1.update(Integer.toHexString(key), 1);
    }
    assertEquals(a1Sk1.getEstimate(), K / 2.0);
  }

  /**
   * @param o object to print
   */
  static void println(final Object o) {
    //System.out.println(o.toString()); //Disable
  }

  /**
   * @param fmt format
   * @param args arguments
   */
  static void printf(final String fmt, final Object ... args) {
    //System.out.printf(fmt, args); //Disable
  }
}
