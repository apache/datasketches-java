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

package org.apache.datasketches;

import static org.apache.datasketches.Util.LONG_MAX_VALUE_AS_DOUBLE;

import org.apache.datasketches.theta.Sketch;

/**
 * This class is used to compute the bounds on the estimate of the ratio <i>B / A</i>, where:
 * <ul>
 * <li><i>A</i> is a Theta Sketch of population <i>PopA</i>.</li>
 * <li><i>B</i> is a Theta Sketch of population <i>PopB</i> that is a subset of <i>A</i>,
 * obtained by an intersection of <i>A</i> with some other Theta Sketch <i>C</i>,
 * which acts like a predicate or selection clause.</li>
 * <li>The estimate of the ratio <i>PopB/PopA</i> is
 * BoundsOnRatiosInThetaSketchedSets.getEstimateOfBoverA(<i>A, B</i>).</li>
 * <li>The Upper Bound estimate on the ratio PopB/PopA is
 * BoundsOnRatiosInThetaSketchedSets.getUpperBoundForBoverA(<i>A, B</i>).</li>
 * <li>The Lower Bound estimate on the ratio PopB/PopA is
 * BoundsOnRatiosInThetaSketchedSets.getLowerBoundForBoverA(<i>A, B</i>).</li>
 * </ul>
 * Note: The theta of <i>A</i> cannot be greater than the theta of <i>B</i>.
 * If <i>B</i> is formed as an intersection of <i>A</i> and some other set <i>C</i>,
 * then the theta of <i>B</i> is guaranteed to be less than or equal to the theta of <i>B</i>.
 *
 * @author Kevin Lang
 * @author Lee Rhodes
 */
public final class BoundsOnRatiosInThetaSketchedSets {

  private BoundsOnRatiosInThetaSketchedSets() {}

  /**
   * Gets the approximate lower bound for B over A based on a 95% confidence interval
   * @param sketchA the sketch A
   * @param sketchB the sketch B
   * @return the approximate lower bound for B over A
   */
  public static double getLowerBoundForBoverA(final Sketch sketchA, final Sketch sketchB) {
    final long thetaLongA = sketchA.getThetaLong();
    final long thetaLongB = sketchB.getThetaLong();
    checkThetas(thetaLongA, thetaLongB);

    final int countB = sketchB.getRetainedEntries(true);
    final int countA = (thetaLongB == thetaLongA)
        ? sketchA.getRetainedEntries(true)
        : sketchA.getCountLessThanThetaLong(thetaLongB);

    if (countA <= 0) { return 0; }
    final double f = thetaLongB / LONG_MAX_VALUE_AS_DOUBLE;
    return BoundsOnRatiosInSampledSets.getLowerBoundForBoverA(countA, countB, f);
  }

  /**
   * Gets the approximate upper bound for B over A based on a 95% confidence interval
   * @param sketchA the sketch A
   * @param sketchB the sketch B
   * @return the approximate upper bound for B over A
   */
  public static double getUpperBoundForBoverA(final Sketch sketchA, final Sketch sketchB) {
    final long thetaLongA = sketchA.getThetaLong();
    final long thetaLongB = sketchB.getThetaLong();
    checkThetas(thetaLongA, thetaLongB);

    final int countB = sketchB.getRetainedEntries(true);
    final int countA = (thetaLongB == thetaLongA)
        ? sketchA.getRetainedEntries(true)
        : sketchA.getCountLessThanThetaLong(thetaLongB);

    if (countA <= 0) { return 1.0; }
    final double f = thetaLongB / LONG_MAX_VALUE_AS_DOUBLE;
    return BoundsOnRatiosInSampledSets.getUpperBoundForBoverA(countA, countB, f);
  }

  /**
   * Gets the estimate for B over A
   * @param sketchA the sketch A
   * @param sketchB the sketch B
   * @return the estimate for B over A
   */
  public static double getEstimateOfBoverA(final Sketch sketchA, final Sketch sketchB) {
    final long thetaLongA = sketchA.getThetaLong();
    final long thetaLongB = sketchB.getThetaLong();
    checkThetas(thetaLongA, thetaLongB);

    final int countB = sketchB.getRetainedEntries(true);
    final int countA = (thetaLongB == thetaLongA)
        ? sketchA.getRetainedEntries(true)
        : sketchA.getCountLessThanThetaLong(thetaLongB);

    if (countA <= 0) { return 0.5; }

    return (double) countB / (double) countA;
  }

  static void checkThetas(final long thetaLongA, final long thetaLongB) {
    if (thetaLongB > thetaLongA) {
      throw new SketchesArgumentException("ThetaLongB cannot be > ThetaLongA.");
    }
  }
}
