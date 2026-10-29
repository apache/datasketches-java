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

package org.apache.datasketches.tuple;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.theta.UpdatableThetaSketch;
import org.apache.datasketches.tuple.adouble.DoubleSummary;
import org.apache.datasketches.tuple.adouble.DoubleSummaryFactory;
import org.apache.datasketches.tuple.adouble.DoubleSummarySetOperations;
import org.testng.annotations.Test;

/**
 * Checks that the TupleSketch set operations reject a ThetaSketch created with a non-default seed.
 */
public class ThetaSeedHashTest {
  private final DoubleSummary.Mode umode = DoubleSummary.Mode.Sum;
  private final DoubleSummarySetOperations dsso = new DoubleSummarySetOperations();
  private final DoubleSummaryFactory factory = new DoubleSummaryFactory(umode);

  private UpdatableTupleSketch<Double, DoubleSummary> tupleSketch() {
    final UpdatableTupleSketch<Double, DoubleSummary> sk =
        new UpdatableTupleSketchBuilder<>(factory).build();
    for (int i = 0; i < 10; i++) { sk.update(i, 1.0); }
    return sk;
  }

  private static UpdatableThetaSketch thetaSketch(final long seed, final int n) {
    final UpdatableThetaSketch sk = UpdatableThetaSketch.builder().setSeed(seed).build();
    for (int i = 0; i < n; i++) { sk.update(i); }
    return sk;
  }

  @Test
  public void checkUnion() {
    final TupleUnion<DoubleSummary> union = new TupleUnion<>(dsso);
    try { union.union(thetaSketch(123L, 10), factory.newSummary()); fail(); }
    catch (final SketchesArgumentException e) { }
    union.union(thetaSketch(123L, 0), factory.newSummary()); //empty is ignored
    union.union(thetaSketch(9001L, 10), factory.newSummary()); //default seed
    assertEquals(union.getResult().getRetainedEntries(), 10);
  }

  @Test
  public void checkIntersection() {
    final TupleIntersection<DoubleSummary> inter = new TupleIntersection<>(dsso);
    inter.intersect(tupleSketch());
    try { inter.intersect(thetaSketch(123L, 10), factory.newSummary()); fail(); }
    catch (final SketchesArgumentException e) { }
    inter.intersect(thetaSketch(9001L, 10), factory.newSummary()); //default seed
    assertEquals(inter.getResult().getRetainedEntries(), 10);
  }

  @Test
  public void checkAnotB() {
    final TupleAnotB<DoubleSummary> anotb = new TupleAnotB<>();
    anotb.setA(tupleSketch());
    try { anotb.notB(thetaSketch(123L, 10)); fail(); }
    catch (final SketchesArgumentException e) { }
    anotb.notB(thetaSketch(123L, 0)); //empty is ignored
    anotb.notB(thetaSketch(9001L, 5)); //default seed
    assertEquals(anotb.getResult(true).getRetainedEntries(), 5);

    try { TupleAnotB.aNotB(tupleSketch(), thetaSketch(123L, 10)); fail(); }
    catch (final SketchesArgumentException e) { }
    assertEquals(TupleAnotB.aNotB(tupleSketch(), thetaSketch(9001L, 5)).getRetainedEntries(), 5);
  }

}
