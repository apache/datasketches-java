package org.apache.datasketches.hll;

import static java.lang.Math.abs;
import static java.lang.Math.max;
import static org.apache.datasketches.common.Util.pwr2SeriesNext;

import java.util.Random;

import org.testng.annotations.Test;

/**
 * This test demonstrates that DataSketch HLL merging is not order-dependent if you use the correct estimator,
 * which is the Composite Estimator.
 */
public class HllSketchMergeOrderBTest {

    private static final int LOG2M = 11;

    @Test
    public void testDataSketchHLLMergeOrderDependency() {
        final int ppo = 1 << 17; //Points per octave.
        // Create 3 sketches with powers-of-2 pattern that triggers order dependency
        final HllSketch sketchA = createUniquePowerSeriesSketch(1L << 59, ppo, 1L << 60);
        final HllSketch sketchB = createUniquePowerSeriesSketch(1L << 60, ppo, 1L << 61);
        final HllSketch sketchC = createUniquePowerSeriesSketch(1L << 61, ppo, 1L << 62);

        println("");

        final double skAEst = sketchA.getCompositeEstimate();
        final double skBEst = sketchB.getCompositeEstimate();
        final double skCEst = sketchC.getCompositeEstimate();

        final double skA_RE = (skAEst/ppo) - 1.0;
        final double skB_RE = (skBEst/ppo) - 1.0;
        final double skC_RE = (skCEst/ppo) - 1.0;

        //Print individual estimates:
        println("SketchA estimate: " + skAEst + ", RE%: " + (skA_RE * 100));
        println("SketchB estimate: " + skBEst + ", RE%: " + (skB_RE * 100));
        println("SketchD estimate: " + skCEst + ", RE%: " + (skC_RE * 100));

        println("\nNOTE: Sketch Error for Composite Estimator for LgK = 11 is +/- 4.6% at 95% confidence.\n");

        // Test six different merge orders:
        final double estimateABC = mergeThreeSketches(sketchA, sketchB, sketchC);
        final double estimateACB = mergeThreeSketches(sketchA, sketchC, sketchB);
        final double estimateBAC = mergeThreeSketches(sketchB, sketchA, sketchC);
        final double estimateBCA = mergeThreeSketches(sketchB, sketchC, sketchA);
        final double estimateCAB = mergeThreeSketches(sketchC, sketchA, sketchB);
        final double estimateCBA = mergeThreeSketches(sketchC, sketchB, sketchA);

        println("Merge order ABC: " + estimateABC);
        println("Merge order ACB: " + estimateACB);
        println("Merge order BAC: " + estimateBAC);
        println("Merge order BCA: " + estimateBCA);
        println("Merge order CAB: " + estimateCAB);
        println("Merge order CBA: " + estimateCBA);
    }

    @Test
    public void checkNewGenerator() {
      final long baseValue = 1L << 59;
      final int ppo = 1 << 13;
      final long limit = (1L << 60);
      int count = 0;
      long lastp = 0;
      for (long p = baseValue; p < limit; p = pwr2SeriesNext(ppo, p)) {
        count++;
        println(count + ", " + p);
        lastp = p;
      }
      println("Count : " + count);
      println("last p: " + lastp);
      println(1L << 60);
    }


    private HllSketch createUniquePowerSeriesSketch(final long baseValue, final int ppo, final long limit) {
      final HllSketch sketch = new HllSketch(LOG2M);
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
        final Union union = new Union(LOG2M);

        union.update(s1);
        union.update(s2);
        union.update(s3);

        return union.getCompositeEstimate();
    }


    private static void println(final Object o) { System.out.println(o.toString()); }

}

