package com.yahoo.sketches.kll;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.SketchesArgumentException;

public class KllFloatsSketchTest {

  private static final double PMF_EPS_FOR_K_8 = 0.35; // PMF rank error (epsilon) for k=8
  private static final double PMF_EPS_FOR_K_128 = 0.025; // PMF rank error (epsilon) for k=128
  private static final double PMF_EPS_FOR_K_256 = 0.013; // PMF rank error (epsilon) for k=256
  private static final double NUMERIC_NOISE_TOLERANCE = 1E-6;

  @Test
  public void empty() {
    final KllFloatsSketch sketch = new KllFloatsSketch();
    sketch.update(Float.NaN); // this must not change anything
    assertTrue(sketch.isEmpty());
    assertEquals(sketch.getN(), 0);
    assertEquals(sketch.getNumRetained(), 0);
    assertTrue(Double.isNaN(sketch.getRank(0)));
    assertTrue(Float.isNaN(sketch.getMinValue()));
    assertTrue(Float.isNaN(sketch.getMaxValue()));
    assertTrue(Float.isNaN(sketch.getQuantile(0.5)));
    assertNull(sketch.getQuantiles(new double[] {0}));
    assertNull(sketch.getPMF(new float[] {0}));
    assertNotNull(sketch.toString(true, true));
    assertNotNull(sketch.toString());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadGetQuantiles() {
    final KllFloatsSketch sketch = new KllFloatsSketch();
    sketch.update(1);
    sketch.getQuantile(-1.0);
  }

  @Test
  public void oneItem() {
    final KllFloatsSketch sketch = new KllFloatsSketch();
    sketch.update(1);
    assertFalse(sketch.isEmpty());
    assertEquals(sketch.getN(), 1);
    assertEquals(sketch.getNumRetained(), 1);
    assertEquals(sketch.getRank(1), 0.0);
    assertEquals(sketch.getRank(2), 1.0);
    assertEquals(sketch.getMinValue(), 1f);
    assertEquals(sketch.getMaxValue(), 1f);
    assertEquals(sketch.getQuantile(0.5), 1f);
  }

  @Test
  public void manyItemsEstimationMode() {
    final KllFloatsSketch sketch = new KllFloatsSketch();
    final int n = 1000000;
    for (int i = 0; i < n; i++) {
      sketch.update(i);
      assertEquals(sketch.getN(), i + 1);
    }

    // test getRank
    for (int i = 0; i < n; i++) {
      final double trueRank = (double) i / (n - 1);
      assertEquals(sketch.getRank(i), trueRank, PMF_EPS_FOR_K_256, "for value " + i);
    }

    // test getPMF
    final double[] pmf = sketch.getPMF(new float[] {n / 2}); // split at median
    assertEquals(pmf.length, 2);
    assertEquals(pmf[0], 0.5, PMF_EPS_FOR_K_256);
    assertEquals(pmf[1], 0.5, PMF_EPS_FOR_K_256);

    assertEquals(sketch.getMinValue(), 0f); // min value is exact
    assertEquals(sketch.getQuantile(0), 0f); // min value is exact
    assertEquals(sketch.getMaxValue(), (float) (n - 1)); // max value is exact
    assertEquals(sketch.getQuantile(1), (float) (n - 1)); // max value is exact

    // check at every 0.1 percentage point
    final double[] fractions = new double[1001];
    final double[] reverseFractions = new double[1001]; // check that ordering doesn't matter
    for (int i = 0; i <= 1000; i++) {
      fractions[i] = (double) i / 1000;
      reverseFractions[1000 - i] = fractions[i];
    }
    final float[] quantiles = sketch.getQuantiles(fractions);
    final float[] reverseQuantiles = sketch.getQuantiles(reverseFractions);
    float previousQuantile = 0;
    for (int i = 0; i <= 1000; i++) {
      final float quantile = sketch.getQuantile(fractions[i]);
      assertEquals(quantile, quantiles[i]);
      assertEquals(quantile, reverseQuantiles[1000 - i]);
      assertTrue(previousQuantile <= quantile);
      previousQuantile = quantile;
    }
}

  @Test
  public void getRankGetCdfGetPmfConsistency() {
    final KllFloatsSketch sketch = new KllFloatsSketch();
    final int n = 1000;
    final float[] values = new float[n];
    for (int i = 0; i < n; i++) {
      sketch.update(i);
      values[i] = i;
    }
    final double[] ranks = sketch.getCDF(values);
    final double[] pmf = sketch.getPMF(values);
    double sumPmf = 0;
    for (int i = 0; i < n; i++) {
      assertEquals(ranks[i], sketch.getRank(values[i]), NUMERIC_NOISE_TOLERANCE,
          "rank vs CDF for value " + i);
      sumPmf += pmf[i];
      assertEquals(ranks[i], sumPmf, NUMERIC_NOISE_TOLERANCE, "CDF vs PMF for value " + i);
    }
    sumPmf += pmf[n];
    assertEquals(sumPmf, 1.0, NUMERIC_NOISE_TOLERANCE);
    assertEquals(ranks[n], 1.0, NUMERIC_NOISE_TOLERANCE);
  }

  @Test
  public void floorLog2() {
    assertEquals(KllHelper.floorOfLog2OfFraction(0, 1), 0);
    assertEquals(KllHelper.floorOfLog2OfFraction(1, 2), 0);
    assertEquals(KllHelper.floorOfLog2OfFraction(2, 2), 0);
    assertEquals(KllHelper.floorOfLog2OfFraction(3, 2), 0);
    assertEquals(KllHelper.floorOfLog2OfFraction(4, 2), 1);
    assertEquals(KllHelper.floorOfLog2OfFraction(5, 2), 1);
    assertEquals(KllHelper.floorOfLog2OfFraction(6, 2), 1);
    assertEquals(KllHelper.floorOfLog2OfFraction(7, 2), 1);
    assertEquals(KllHelper.floorOfLog2OfFraction(8, 2), 2);
  }

  @Test
  public void merge() {
    final KllFloatsSketch sketch1 = new KllFloatsSketch();
    final KllFloatsSketch sketch2 = new KllFloatsSketch();
    final int n = 10000;
    for (int i = 0; i < n; i++) {
      sketch1.update(i);
      sketch2.update((2 * n) - i - 1);
    }

    assertEquals(sketch2.getMinValue(), (float) n);
    assertEquals(sketch2.getMaxValue(), (float) ((2 * n) - 1));

    assertEquals(sketch2.getMinValue(), (float) n);
    assertEquals(sketch2.getMaxValue(), (float) ((2 * n) - 1));

    sketch1.merge(sketch2);

    assertFalse(sketch1.isEmpty());
    assertEquals(sketch1.getN(), 2 * n);
    assertEquals(sketch1.getMinValue(), 0f);
    assertEquals(sketch1.getMaxValue(), (float) ((2 * n) - 1));
    assertEquals(sketch1.getQuantile(0.5), n, n * PMF_EPS_FOR_K_256);
  }

  @Test
  public void mergeLowerK() {
    final KllFloatsSketch sketch1 = new KllFloatsSketch(256);
    final KllFloatsSketch sketch2 = new KllFloatsSketch(128);
    final int n = 10000;
    for (int i = 0; i < n; i++) {
      sketch1.update(i);
      sketch2.update((2 * n) - i - 1);
    }

    assertEquals(sketch2.getMinValue(), (float) n);
    assertEquals(sketch2.getMaxValue(), (float) ((2 * n) - 1));

    assertEquals(sketch2.getMinValue(), (float) n);
    assertEquals(sketch2.getMaxValue(), (float) ((2 * n) - 1));

    assertTrue(sketch1.getNormalizedRankError(false) < sketch2.getNormalizedRankError(false));
    assertTrue(sketch1.getNormalizedRankError(true) < sketch2.getNormalizedRankError(true));
    sketch1.merge(sketch2);

    // sketch1 must get "contaminated" by the lower K in sketch2
    assertEquals(sketch1.getNormalizedRankError(false), sketch2.getNormalizedRankError(false));
    assertEquals(sketch1.getNormalizedRankError(true), sketch2.getNormalizedRankError(true));

    assertFalse(sketch1.isEmpty());
    assertEquals(sketch1.getN(), 2 * n);
    assertEquals(sketch1.getMinValue(), 0f);
    assertEquals(sketch1.getMaxValue(), (float) ((2 * n) - 1));
    assertEquals(sketch1.getQuantile(0.5), n, n * PMF_EPS_FOR_K_128);
  }

  @Test
  public void mergeEmptyLowerK() {
    final KllFloatsSketch sketch1 = new KllFloatsSketch(256);
    final KllFloatsSketch sketch2 = new KllFloatsSketch(128);
    final int n = 10000;
    for (int i = 0; i < n; i++) {
      sketch1.update(i);
    }

    // rank error should not be affected by a merge with an empty sketch
    final double rankErrorBeforeMerge = sketch1.getNormalizedRankError(true);
    sketch1.merge(sketch2);
    assertEquals(sketch1.getNormalizedRankError(true), rankErrorBeforeMerge);

    assertFalse(sketch1.isEmpty());
    assertEquals(sketch1.getN(), n);
    assertEquals(sketch1.getMinValue(), 0f);
    assertEquals(sketch1.getMaxValue(), (float) (n - 1));
    assertEquals(sketch1.getQuantile(0.5), n / 2, (n / 2) * PMF_EPS_FOR_K_256);

    //merge the other way
    sketch2.merge(sketch1);
    assertFalse(sketch1.isEmpty());
    assertEquals(sketch1.getN(), n);
    assertEquals(sketch1.getMinValue(), 0f);
    assertEquals(sketch1.getMaxValue(), (float) (n - 1));
    assertEquals(sketch1.getQuantile(0.5), n / 2, (n / 2) * PMF_EPS_FOR_K_256);
  }

  @Test
  public void checkMergeUseOtherMinValue() {
    final KllFloatsSketch sketch1 = new KllFloatsSketch();
    final KllFloatsSketch sketch2 = new KllFloatsSketch();
    sketch1.update(1);
    sketch2.update(2);
    sketch2.merge(sketch1);
    assertEquals(sketch2.getMinValue(), 1.0F);
  }

  @SuppressWarnings("unused")
  @Test(expectedExceptions = SketchesArgumentException.class)
  public void kTooSmall() {
    new KllFloatsSketch(KllFloatsSketch.MIN_K - 1);
  }

  @SuppressWarnings("unused")
  @Test(expectedExceptions = SketchesArgumentException.class)
  public void kTooLarge() {
    new KllFloatsSketch(KllFloatsSketch.MAX_K + 1);
  }

  @Test
  public void minK() {
    final KllFloatsSketch sketch = new KllFloatsSketch(KllFloatsSketch.MIN_K);
    for (int i = 0; i < 1000; i++) {
      sketch.update(i);
    }
    assertEquals(sketch.getQuantile(0.5), 500, 500 * PMF_EPS_FOR_K_8);
  }

  @Test
  public void maxK() {
    final KllFloatsSketch sketch = new KllFloatsSketch(KllFloatsSketch.MAX_K);
    for (int i = 0; i < 1000; i++) {
      sketch.update(i);
    }
    assertEquals(sketch.getQuantile(0.5), 500, 500 * PMF_EPS_FOR_K_256);
  }

  @Test
  public void serializeDeserializeEmpty() {
    final KllFloatsSketch sketch1 = new KllFloatsSketch();
    final byte[] bytes = sketch1.toByteArray();
    KllFloatsSketch sketch2 = KllFloatsSketch.heapify(Memory.wrap(bytes));
    assertEquals(bytes.length, sketch1.getSerializedSizeBytes());
    assertTrue(sketch2.isEmpty());
    assertEquals(sketch2.getNumRetained(), sketch1.getNumRetained());
    assertEquals(sketch2.getN(), sketch1.getN());
    assertEquals(sketch2.getNormalizedRankError(false), sketch1.getNormalizedRankError(false));
    assertTrue(Float.isNaN(sketch2.getMinValue()));
    assertTrue(Float.isNaN(sketch2.getMaxValue()));
    assertEquals(sketch2.getSerializedSizeBytes(), sketch1.getSerializedSizeBytes());
  }

  @Test
  public void serializeDeserialize() {
    final KllFloatsSketch sketch1 = new KllFloatsSketch();
    final int n = 1000;
    for (int i = 0; i < n; i++) {
      sketch1.update(i);
    }
    final byte[] bytes = sketch1.toByteArray();
    KllFloatsSketch sketch2 = KllFloatsSketch.heapify(Memory.wrap(bytes));
    assertEquals(bytes.length, sketch1.getSerializedSizeBytes());
    assertFalse(sketch2.isEmpty());
    assertEquals(sketch2.getNumRetained(), sketch1.getNumRetained());
    assertEquals(sketch2.getN(), sketch1.getN());
    assertEquals(sketch2.getNormalizedRankError(false), sketch1.getNormalizedRankError(false));
    assertEquals(sketch2.getMinValue(), sketch1.getMinValue());
    assertEquals(sketch2.getMaxValue(), sketch1.getMaxValue());
    assertEquals(sketch2.getSerializedSizeBytes(), sketch1.getSerializedSizeBytes());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void outOfOrderSplitPoints() {
    final KllFloatsSketch sketch = new KllFloatsSketch();
    sketch.update(0);
    sketch.getCDF(new float[] {1, 0});
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void nanSplitPoint() {
    final KllFloatsSketch sketch = new KllFloatsSketch();
    sketch.update(0);
    sketch.getCDF(new float[] {Float.NaN});
  }

  @Test
  public void getMaxSerializedSizeBytes() {
    final int sizeBytes =
        KllFloatsSketch.getMaxSerializedSizeBytes(KllFloatsSketch.DEFAULT_K, 1_000_000_000);
    assertEquals(sizeBytes, 3160);
  }

  @Test
  public void checkUbOnNumLevels() {
    assertEquals(KllHelper.ubOnNumLevels(0), 1);
  }

  @Test
  public void checkIntCapAux() {
    final int lvlCap = KllHelper.levelCapacity(10, 100, 50, 8);
    assertEquals(lvlCap, 8);
  }

}
