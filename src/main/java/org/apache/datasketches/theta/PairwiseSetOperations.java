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

package org.apache.datasketches.theta;

import static org.apache.datasketches.Util.DEFAULT_NOMINAL_ENTRIES;

/**
 * Set Operations where the arguments are presented in pairs as in <i>C = Op(A,B)</i>. These are
 * stateless operations and the result is returned immediately.
 *
 * <p>These operations are designed for convenience and accept Sketches that may be either
 * Heap-based or Direct.
 *
 * @author Lee Rhodes
 * @deprecated  This class has been deprecated as equivalent functionality has been added to the
 * SetOperation classes: {@link Union}, {@link Intersection} and {@link AnotB}.
 */
@Deprecated
public class PairwiseSetOperations {

  /**
   * This implements a stateless, pair-wise <i>Intersect</i> operation on sketches
   * that are either Heap-based or Direct.
   * If either inputs are null or empty an EmptyCompactSketch is returned.
   *
   * @param skA The first Sketch argument.
   * @param skB The second Sketch argument.
   * @return the result as an ordered CompactSketch on the heap.
   * @deprecated Use {@link Intersection#intersect(Sketch, Sketch)} instead, which has more
   * complete seed handling.
   */
  @Deprecated
  public static CompactSketch intersect(final Sketch skA, final Sketch skB) {
    final Intersection inter = new SetOperationBuilder().buildIntersection();
    return inter.intersect(skA, skB);
  }

  /**
   * This implements a stateless, pair-wise <i>A AND NOT B</i> operation on Sketches
   * that are either Heap-based or Direct.
   * If both inputs are null an EmptyCompactSketch is returned.
   *
   * @param skA The first Sketch argument.
   * @param skB The second Sketch argument.
   * @return the result as an ordered CompactSketch on the heap.
   * @deprecated Use {@link AnotB#aNotB(Sketch, Sketch)} instead, which has more
   * complete seed handling.
   */
  @Deprecated
  public static CompactSketch aNotB(final Sketch skA, final Sketch skB) {
    final AnotB anotb = new SetOperationBuilder().buildANotB();
    return anotb.aNotB(skA, skB);
  }

  /**
   * This implements a stateless, pair-wise union operation on ordered,
   * CompactSketches that are either Heap-based or Direct.
   * Having the input sketches be compact and ordered enables extremely fast union operation.
   * If both inputs are null an EmptyCompactSketch is returned.
   * If one is null the other is returned, which can be either Heap-based or Direct.
   * This is equivalent to union(skA, skB, k) where k is the default of 4096.
   *
   * @param skA The first ordered, CompactSketch argument.
   * @param skB The second ordered, CompactSketch argument
   * @return the result as an ordered CompactSketch.
   * @deprecated Please use {@link Union#union(Sketch, Sketch)} instead, which has more
   * complete seed handling.
   */
  @Deprecated
  public static CompactSketch union(final CompactSketch skA, final CompactSketch skB) {
    return union(skA, skB, DEFAULT_NOMINAL_ENTRIES);
  }

  /**
   * This implements a stateless, pair-wise union operation on ordered,
   * CompactSketches that are either Heap-based or Direct. The returned sketch will be cutback to
   * k if required, similar to the regular Union operation. If a cutback is required, the returned
   * sketch will always be on the heap.
   * If both inputs are null a null is returned. If either sketch is empty its Theta is ignored.
   * If one is null the other is returned, which may be either Direct or heap-based if a cutback
   * is required.
   *
   * @param skA The first ordered, CompactSketch argument.
   * @param skB The second ordered, CompactSketch argument
   * @param k The upper bound of the number of entries to be retained by the sketch
   * @return the result as an ordered CompactSketch.
   * @deprecated Please use {@link Union#union(Sketch, Sketch)} instead, which has more
   * complete seed handling.
   */
  @Deprecated
  public static CompactSketch union(final CompactSketch skA, final CompactSketch skB, final int k) {
    final Union un = new SetOperationBuilder().setNominalEntries(k).buildUnion();
    return un.union(skA, skB);
  }

}
