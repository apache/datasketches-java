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

package org.apache.datasketches.hll;

import static org.apache.datasketches.common.Util.pwr2SeriesNext;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

/**
 * This test demonstrates that DataSketch HLL merging is not order-dependent if you use the Composite estimator.
 */
public class HllSketchMergeOrderTest {

    private static final int LgK = 11;

    @Test
    public void testDataSketchHLLMergeOrderDependency() {
        final int ppo = 1 << 17; //= 131,072 unique Points Per Octave.

        // Create 3 sketches with fractional powers of 2 series
        final HllSketch sketchA = createUniquePowerSeriesSketch(1L << 59, ppo, 1L << 60);
        final HllSketch sketchB = createUniquePowerSeriesSketch(1L << 60, ppo, 1L << 61);
        final HllSketch sketchC = createUniquePowerSeriesSketch(1L << 61, ppo, 1L << 62);

        final double skAEst = sketchA.getCompositeEstimate();
        final double skBEst = sketchB.getCompositeEstimate();
        final double skCEst = sketchC.getCompositeEstimate();

        final double skA_RE = (skAEst/ppo) - 1.0;
        final double skB_RE = (skBEst/ppo) - 1.0;
        final double skC_RE = (skCEst/ppo) - 1.0;

        //Print individual composite estimates and Relative Error:
        println("");
        println("SketchA estimate: " + skAEst + ", RE%: " + (skA_RE * 100));
        println("SketchB estimate: " + skBEst + ", RE%: " + (skB_RE * 100));
        println("SketchC estimate: " + skCEst + ", RE%: " + (skC_RE * 100));

        println("\nNOTE: Sketch Relative Error for Composite Estimator for LgK = 11 is +/- 4.6% at 95% confidence.\n");

        // Test six different merge orders:
        final double estABC = mergeThreeSketches(sketchA, sketchB, sketchC);
        final double estACB = mergeThreeSketches(sketchA, sketchC, sketchB);
        final double estBAC = mergeThreeSketches(sketchB, sketchA, sketchC);
        final double estBCA = mergeThreeSketches(sketchB, sketchC, sketchA);
        final double estCAB = mergeThreeSketches(sketchC, sketchA, sketchB);
        final double estCBA = mergeThreeSketches(sketchC, sketchB, sketchA);

        println("Merge order ABC: " + estABC);
        println("Merge order ACB: " + estACB);
        println("Merge order BAC: " + estBAC);
        println("Merge order BCA: " + estBCA);
        println("Merge order CAB: " + estCAB);
        println("Merge order CBA: " + estCBA);

        assertTrue((estABC == estACB) && (estABC == estBAC) && (estABC == estBCA) && (estABC == estCAB) && (estABC == estCBA));
    }

    /**
     * Generates a power series based on fractional powers of 2 where the separation between successive values is 2^(1/ppo).
     * @param baseValue starting value, inclusive
     * @param ppo number of unique points per octave
     * @param limit the upper limit, exclusive
     * @return the loaded sketch
     */
    private HllSketch createUniquePowerSeriesSketch(final long baseValue, final int ppo, final long limit) {
      final HllSketch sketch = new HllSketch(LgK);
      int count = 0;
      long lastp = 0;
      for (long p = baseValue; p < limit; p = pwr2SeriesNext(ppo, p)) {
        sketch.update(p);
        count++;
        lastp = p;
      }
      println("BaseValue: " + baseValue + ", limit: " + limit + ", Count: " + count + ", lastPt: " + lastp);
      return sketch;
    }

    /**
     * Merges three sketches in the specified order and returns the composite estimate
     */
    private double mergeThreeSketches(final HllSketch s1, final HllSketch s2, final HllSketch s3) {
        final HllUnion union = new HllUnion(LgK);

        union.update(s1);
        union.update(s2);
        union.update(s3);

        return union.getCompositeEstimate();
    }

    //@Test
    /**
     * Used for tweaking the generator algorithm
     */
    public void checkNewGenerator() {
      final long baseValue = 1L << 59;
      final int ppo = 1 << 10;
      final long limit = 1L << 60;
      int count = 0;
      long lastp = 0;
      for (long p = baseValue; p < limit; p = pwr2SeriesNext(ppo, p)) {
        count++;
        println(count + ", " + p);
        lastp = p;
      }
      println("\nPPO:       " + ppo);
      println("Count:     " + count);
      println("baseValue: " + baseValue);
      println("last p:    " + lastp);
      println("limit:     " + limit);
    }

    private static void println(final Object o) {
      //System.out.println(o.toString());
    }

}
