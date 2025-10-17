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

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.theta.ThetaSketch;
import org.apache.datasketches.theta.UpdatableThetaSketch;
import org.apache.datasketches.theta.UpdatableThetaSketchBuilder;
import org.apache.datasketches.tuple.TupleAnotB;
import org.apache.datasketches.tuple.CompactTupleSketch;
import org.apache.datasketches.tuple.TupleSketch;
import org.apache.datasketches.tuple.TupleSketchIterator;
import org.apache.datasketches.tuple.UpdatableTupleSketch;
import org.apache.datasketches.tuple.UpdatableTupleSketchBuilder;
import org.apache.datasketches.tuple.adouble.DoubleSummary.Mode;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class AdoubleAnotBTest {
  private static final DoubleSummary.Mode mode = Mode.Sum;
  private final Results results = new Results();

  private static void threeMethodsWithTheta(
      final TupleAnotB<DoubleSummary> aNotB,
      final TupleSketch<DoubleSummary> skA,
      final TupleSketch<DoubleSummary> skB,
      final ThetaSketch skThetaB,
      final Results results)
  {
    CompactTupleSketch<DoubleSummary> result;

    //Stateful, A = Tuple, B = Tuple
    if (skA != null) {
      try {
        aNotB.setA(skA);
        aNotB.notB(skB);
        result = aNotB.getResult(true);
        results.check(result);
      }
      catch (final SketchesArgumentException e) { }
    }

    //Stateless A = Tuple, B = Tuple
    if (skA == null || skB == null) {
      try {
        result = TupleAnotB.aNotB(skA, skB);
        fail();
      }
      catch (final SketchesArgumentException e) { }
    } else {
      result = TupleAnotB.aNotB(skA, skB);
      results.check(result);
    }

    //Stateless A = Tuple, B = Theta
    if (skA == null || skThetaB == null) {
      try { result = TupleAnotB.aNotB(skA, skThetaB); fail(); }
      catch (final SketchesArgumentException e) { }
    } else {
      result = TupleAnotB.aNotB(skA, skThetaB);
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

    void check(final CompactTupleSketch<DoubleSummary> result) {
      assertEquals(result.getRetainedEntries(), retEnt);
      assertEquals(result.isEmpty(), empty);
      if (result.getTheta() < 1.0) {
        final double est = result.getEstimate();
        assertEquals(est, expect, expect * tol);
        assertTrue(result.getUpperBound(1) > est);
        assertTrue(result.getLowerBound(1) <= est);
      } else {
        assertEquals(result.getEstimate(), expect, 0.0);
        assertEquals(result.getUpperBound(1), expect, 0.0);
        assertEquals(result.getLowerBound(1), expect, 0.0);
      }
      final TupleSketchIterator<DoubleSummary> it = result.iterator();
      while (it.next()) {
        Assert.assertEquals(it.getSummary().getValue(), sum);
      }
    }
  } //End class Results

  private static UpdatableTupleSketch<Double, DoubleSummary> buildUpdatableTuple() {
    return new UpdatableTupleSketchBuilder<>(new DoubleSummaryFactory(mode)).build();
  }

  private static UpdatableThetaSketch buildUpdateTheta() {
    return new UpdatableThetaSketchBuilder().build();
  }

  /*****************************************/

  @Test
  public void aNotBNullEmptyCombinations() {
    final TupleAnotB<DoubleSummary> aNotB = new TupleAnotB<>();
    // calling getResult() before calling update() should yield an empty set
    final CompactTupleSketch<DoubleSummary> result = aNotB.getResult(true);
    results.set(0, true, 0.0, 0.0, 0.0).check(result);

    final UpdatableTupleSketch<Double, DoubleSummary> sketch = buildUpdatableTuple();
    final UpdatableThetaSketch skTheta = buildUpdateTheta();

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
    final UpdatableTupleSketch<Double, DoubleSummary> skA = buildUpdatableTuple();
    skA.update(1, 1.0);
    skA.update(2, 1.0);
    final UpdatableTupleSketch<Double, DoubleSummary> skA2 = buildUpdatableTuple();
    final TupleAnotB<DoubleSummary> aNotB = new TupleAnotB<>();
    aNotB.setA(skA);
    assertEquals(aNotB.getResult(false).isEmpty(), false);
    aNotB.setA(skA2);
    assertEquals(aNotB.getResult(false).isEmpty(), true);
  }

  @Test
  public void aNotBEmptyExact() {
    final UpdatableTupleSketch<Double, DoubleSummary> sketchA = buildUpdatableTuple();
    final UpdatableTupleSketch<Double, DoubleSummary> sketchB = buildUpdatableTuple();
    sketchB.update(1, 1.0);
    sketchB.update(2, 1.0);
    final UpdatableThetaSketch skThetaB = buildUpdateTheta();
    skThetaB.update(1);
    skThetaB.update(2);

    final TupleAnotB<DoubleSummary> aNotB = new TupleAnotB<>();
    results.set(0, true, 0.0, 0.0, 0.0);
    threeMethodsWithTheta(aNotB, sketchA, sketchB, skThetaB, results);
  }

  @Test
  public void aNotBExactEmpty() {
    final UpdatableTupleSketch<Double, DoubleSummary> sketchA = buildUpdatableTuple();
    sketchA.update(1, 1.0);
    sketchA.update(2, 1.0);
    final UpdatableTupleSketch<Double, DoubleSummary> sketchB = buildUpdatableTuple();
    final UpdatableThetaSketch skThetaB = buildUpdateTheta();

    final TupleAnotB<DoubleSummary> aNotB = new TupleAnotB<>();
    results.set(2, false, 2.0, 0.0, 1.0);
    threeMethodsWithTheta(aNotB, sketchA, sketchB, skThetaB, results);

    // same thing, but compact sketches
    threeMethodsWithTheta(aNotB, sketchA.compact(), sketchB.compact(), skThetaB.compact(), results);
  }

  @Test
  public void aNotBExactOverlap() {
    final UpdatableTupleSketch<Double, DoubleSummary> sketchA = buildUpdatableTuple();
    sketchA.update(1, 1.0);
    sketchA.update(1, 1.0);
    sketchA.update(2, 1.0);
    sketchA.update(2, 1.0);

    final UpdatableTupleSketch<Double, DoubleSummary> sketchB = buildUpdatableTuple();
    sketchB.update(2, 1.0);
    sketchB.update(2, 1.0);
    sketchB.update(3, 1.0);
    sketchB.update(3, 1.0);

    final UpdatableThetaSketch skThetaB = buildUpdateTheta();
    skThetaB.update(2);
    skThetaB.update(3);

    final TupleAnotB<DoubleSummary> aNotB = new TupleAnotB<>();
    results.set(1, false, 1.0, 0.0, 2.0);
    threeMethodsWithTheta(aNotB, sketchA, sketchB, skThetaB, results);
  }

  @Test
  public void aNotBEstimationOverlap() {
    final UpdatableTupleSketch<Double, DoubleSummary> sketchA = buildUpdatableTuple();
    for (int i = 0; i < 8192; i++) {
      sketchA.update(i, 1.0);
    }

    final UpdatableTupleSketch<Double, DoubleSummary> sketchB = buildUpdatableTuple();
    for (int i = 0; i < 4096; i++) {
      sketchB.update(i, 1.0);
    }

    final UpdatableThetaSketch skThetaB = buildUpdateTheta();
    for (int i = 0; i < 4096; i++) {
      skThetaB.update(i);
    }

    final TupleAnotB<DoubleSummary> aNotB = new TupleAnotB<>();
    results.set(2123, false, 4096.0, 0.03, 1.0);
    threeMethodsWithTheta(aNotB, sketchA, sketchB, skThetaB, results);

    // same thing, but compact sketches
    threeMethodsWithTheta(aNotB, sketchA.compact(), sketchB.compact(), skThetaB.compact(), results);
  }

  @Test
  public void aNotBEstimationOverlapLargeB() {
    final UpdatableTupleSketch<Double, DoubleSummary> sketchA = buildUpdatableTuple();
    for (int i = 0; i < 10_000; i++) {
      sketchA.update(i, 1.0);
    }

    final UpdatableTupleSketch<Double, DoubleSummary> sketchB = buildUpdatableTuple();
    for (int i = 0; i < 100_000; i++) {
      sketchB.update(i + 8000, 1.0);
    }

    final UpdatableThetaSketch skThetaB = buildUpdateTheta();
    for (int i = 0; i < 100_000; i++) {
      skThetaB.update(i + 8000);
    }

    final int expected = 8_000;
    final TupleAnotB<DoubleSummary> aNotB = new TupleAnotB<>();
    results.set(376, false, expected, 0.1, 1.0);
    threeMethodsWithTheta(aNotB, sketchA, sketchB, skThetaB, results);

    // same thing, but compact sketches
    threeMethodsWithTheta(aNotB, sketchA.compact(), sketchB.compact(), skThetaB.compact(), results);
  }

}
