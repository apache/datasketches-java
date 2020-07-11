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

package org.apache.datasketches.tuple.adouble;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;
import org.apache.datasketches.tuple.AnotB;
import org.apache.datasketches.tuple.CompactSketch;
import org.apache.datasketches.tuple.Sketch;
import org.apache.datasketches.tuple.SketchIterator;
import org.apache.datasketches.tuple.UpdatableSketch;
import org.apache.datasketches.tuple.UpdatableSketchBuilder;
import org.apache.datasketches.tuple.adouble.DoubleSummary.Mode;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class AdoubleAnotBTest {
  private static final DoubleSummary.Mode mode = Mode.Sum;
  private final Results results = new Results();

  @SuppressWarnings("deprecation")
  private static void threeMethodsWithTheta(
      final AnotB<DoubleSummary> aNotB,
      final Sketch<DoubleSummary> skA,
      final Sketch<DoubleSummary> skB,
      final org.apache.datasketches.theta.Sketch skThetaB,
      final Results results)
  {
    CompactSketch<DoubleSummary> result;

    //Deprecated, Stateless, A = Tuple, B = Tuple
    //Old behavior is tolerant of nulls
    aNotB.update(skA, skB);
    result = aNotB.getResult();
    results.check(result);

    //Stateless A = Tuple, B = Tuple
    if ((skA == null) || (skB == null)) {
      try {
        result = AnotB.aNotB(skA, skB);
        fail();
      }
      catch (final SketchesArgumentException e) { }
    } else {
      result = AnotB.aNotB(skA, skB);
      results.check(result);
    }

    //Stateless A = Tuple, B = Theta
    if ((skA == null) || (skThetaB == null)) {
      try { result = AnotB.aNotB(skA, skThetaB); fail(); }
      catch (final SketchesArgumentException e) { }
    } else {
      result = AnotB.aNotB(skA, skThetaB);
      results.check(result);
    }

    //Stateful A = Tuple, B = Tuple
    if (skA == null) {
      try { aNotB.setA(skA); fail(); }
      catch (final SketchesArgumentException e) { }
    } else {
      aNotB.setA(skA);
      aNotB.notB(skB);
      result = aNotB.getResult(true);
      results.check(result);
    }

    //Stateful A = Tuple, B = Theta
    if (skA == null) {
      try { aNotB.setA(skA); fail(); }
      catch (final SketchesArgumentException e) { }
    } else {
      aNotB.setA(skA);
      aNotB.notB(skThetaB);
      result = aNotB.getResult(false);
      results.check(result);
      result = aNotB.getResult(true);
      results.check(result);
    }
  }

  private static class Results {
    private int retEnt = 0;
    private boolean empty = true;
    private double expect = 0.0;
    private double tol = 0.0;
    private double sum = 0.0;

    Results() {}

    Results set(final int retEnt, final boolean empty,
        final double expect, final double tol, final double sum) {
      this.retEnt = retEnt; //retained Entries
      this.empty = empty;
      this.expect = expect; //expected estimate
      this.tol = tol;       //tolerance
      this.sum = sum;
      return this;
    }

    void check(final CompactSketch<DoubleSummary> result) {
      assertEquals(result.getRetainedEntries(), retEnt);
      assertEquals(result.isEmpty(), empty);
      if (result.getTheta() < 1.0) {
        double est = result.getEstimate();
        assertEquals(est, expect, expect * tol);
        assertTrue(result.getUpperBound(1) > est);
        assertTrue(result.getLowerBound(1) <= est);
      } else {
        assertEquals(result.getEstimate(), expect, 0.0);
        assertEquals(result.getUpperBound(1), expect, 0.0);
        assertEquals(result.getLowerBound(1), expect, 0.0);
      }
      SketchIterator<DoubleSummary> it = result.iterator();
      while (it.next()) {
        Assert.assertEquals(it.getSummary().getValue(), sum);
      }
    }
  } //End class Results

  private static UpdatableSketch<Double, DoubleSummary> buildUpdatableTuple() {
    return new UpdatableSketchBuilder<>(new DoubleSummaryFactory(mode)).build();
  }

  private static UpdateSketch buildUpdateTheta() {
    return new UpdateSketchBuilder().build();
  }

  /*****************************************/

  @SuppressWarnings("deprecation")
  @Test
  public void aNotBNullEmptyCombinations() {
    AnotB<DoubleSummary> aNotB = new AnotB<>();
    // calling getResult() before calling update() should yield an empty set
    CompactSketch<DoubleSummary> result = aNotB.getResult();
    results.set(0, true, 0.0, 0.0, 0.0).check(result);

    UpdatableSketch<Double, DoubleSummary> sketch = buildUpdatableTuple();
    UpdateSketch skTheta = buildUpdateTheta();

    threeMethodsWithTheta(aNotB, null, null, null, results);
    threeMethodsWithTheta(aNotB, sketch, null, null, results);
    threeMethodsWithTheta(aNotB, null, sketch, null, results);
    threeMethodsWithTheta(aNotB, sketch, sketch, null, results);
    threeMethodsWithTheta(aNotB, null, null, skTheta, results);
    threeMethodsWithTheta(aNotB, sketch, null, skTheta, results);
    threeMethodsWithTheta(aNotB, null, sketch, skTheta, results);
    threeMethodsWithTheta(aNotB, sketch, sketch, skTheta, results);
  }

  @Test
  public void aNotBCheckDoubleSetAs() {
    UpdatableSketch<Double, DoubleSummary> skA = buildUpdatableTuple();
    skA.update(1, 1.0);
    skA.update(2, 1.0);
    UpdatableSketch<Double, DoubleSummary> skA2 = buildUpdatableTuple();
    AnotB<DoubleSummary> aNotB = new AnotB<>();
    aNotB.setA(skA);
    assertEquals(aNotB.getResult(false).isEmpty(), false);
    aNotB.setA(skA2);
    assertEquals(aNotB.getResult(false).isEmpty(), true);
  }

  @Test
  public void aNotBEmptyExact() {
    UpdatableSketch<Double, DoubleSummary> sketchA = buildUpdatableTuple();
    UpdatableSketch<Double, DoubleSummary> sketchB = buildUpdatableTuple();
    sketchB.update(1, 1.0);
    sketchB.update(2, 1.0);
    UpdateSketch skThetaB = buildUpdateTheta();
    skThetaB.update(1);
    skThetaB.update(2);

    AnotB<DoubleSummary> aNotB = new AnotB<>();
    results.set(0, true, 0.0, 0.0, 0.0);
    threeMethodsWithTheta(aNotB, sketchA, sketchB, skThetaB, results);
  }

  @Test
  public void aNotBExactEmpty() {
    UpdatableSketch<Double, DoubleSummary> sketchA = buildUpdatableTuple();
    sketchA.update(1, 1.0);
    sketchA.update(2, 1.0);
    UpdatableSketch<Double, DoubleSummary> sketchB = buildUpdatableTuple();
    UpdateSketch skThetaB = buildUpdateTheta();

    AnotB<DoubleSummary> aNotB = new AnotB<>();
    results.set(2, false, 2.0, 0.0, 1.0);
    threeMethodsWithTheta(aNotB, sketchA, sketchB, skThetaB, results);

    // same thing, but compact sketches
    threeMethodsWithTheta(aNotB, sketchA.compact(), sketchB.compact(), skThetaB.compact(), results);
  }

  @Test
  public void aNotBExactOverlap() {
    UpdatableSketch<Double, DoubleSummary> sketchA = buildUpdatableTuple();
    sketchA.update(1, 1.0);
    sketchA.update(1, 1.0);
    sketchA.update(2, 1.0);
    sketchA.update(2, 1.0);

    UpdatableSketch<Double, DoubleSummary> sketchB = buildUpdatableTuple();
    sketchB.update(2, 1.0);
    sketchB.update(2, 1.0);
    sketchB.update(3, 1.0);
    sketchB.update(3, 1.0);

    UpdateSketch skThetaB = buildUpdateTheta();
    skThetaB.update(2);
    skThetaB.update(3);

    AnotB<DoubleSummary> aNotB = new AnotB<>();
    results.set(1, false, 1.0, 0.0, 2.0);
    threeMethodsWithTheta(aNotB, sketchA, sketchB, skThetaB, results);
  }

  @Test
  public void aNotBEstimationOverlap() {
    UpdatableSketch<Double, DoubleSummary> sketchA = buildUpdatableTuple();
    for (int i = 0; i < 8192; i++) {
      sketchA.update(i, 1.0);
    }

    UpdatableSketch<Double, DoubleSummary> sketchB = buildUpdatableTuple();
    for (int i = 0; i < 4096; i++) {
      sketchB.update(i, 1.0);
    }

    UpdateSketch skThetaB = buildUpdateTheta();
    for (int i = 0; i < 4096; i++) {
      skThetaB.update(i);
    }

    AnotB<DoubleSummary> aNotB = new AnotB<>();
    results.set(2123, false, 4096.0, 0.03, 1.0);
    threeMethodsWithTheta(aNotB, sketchA, sketchB, skThetaB, results);

    // same thing, but compact sketches
    threeMethodsWithTheta(aNotB, sketchA.compact(), sketchB.compact(), skThetaB.compact(), results);
  }

  @Test
  public void aNotBEstimationOverlapLargeB() {
    UpdatableSketch<Double, DoubleSummary> sketchA = buildUpdatableTuple();
    for (int i = 0; i < 10_000; i++) {
      sketchA.update(i, 1.0);
    }

    UpdatableSketch<Double, DoubleSummary> sketchB = buildUpdatableTuple();
    for (int i = 0; i < 100_000; i++) {
      sketchB.update(i + 8000, 1.0);
    }

    UpdateSketch skThetaB = buildUpdateTheta();
    for (int i = 0; i < 100_000; i++) {
      skThetaB.update(i + 8000);
    }

    final int expected = 8_000;
    AnotB<DoubleSummary> aNotB = new AnotB<>();
    results.set(376, false, expected, 0.1, 1.0);
    threeMethodsWithTheta(aNotB, sketchA, sketchB, skThetaB, results);

    // same thing, but compact sketches
    threeMethodsWithTheta(aNotB, sketchA.compact(), sketchB.compact(), skThetaB.compact(), results);
  }

}
