package com.yahoo.sketches.quantiles;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.SketchesArgumentException;

public class KllFloatsSketchTest {

  private static final double EPS_FOR_K_8 = 0.35; // rank error (epsilon) for k=8
  private static final double EPS_FOR_K_128 = 0.025; // rank error (epsilon) for k=128
  private static final double EPS_FOR_K_256 = 0.013; // rank error (epsilon) for k=256
  private static final double NUMERIC_NOISE_TOLERANCE = 0.000001;

  @Test
  public void empty() {
    final KllFloatsSketch sketch = new KllFloatsSketch();
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getN(), 0);
    Assert.assertEquals(sketch.getNumRetained(), 0);
    Assert.assertTrue(Double.isNaN(sketch.getRank(0)));
    Assert.assertTrue(Float.isNaN(sketch.getMinValue()));
    Assert.assertTrue(Float.isNaN(sketch.getMaxValue()));
    Assert.assertTrue(Float.isNaN(sketch.getQuantile(0.5)));
  }

  @Test
  public void oneItem() {
    final KllFloatsSketch sketch = new KllFloatsSketch();
    sketch.update(1);
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getN(), 1);
    Assert.assertEquals(sketch.getNumRetained(), 1);
    Assert.assertEquals(sketch.getRank(1), 0.0);
    Assert.assertEquals(sketch.getRank(2), 1.0);
    Assert.assertEquals(sketch.getMinValue(), 1f);
    Assert.assertEquals(sketch.getMaxValue(), 1f);
    Assert.assertEquals(sketch.getQuantile(0.5), 1f);
  }

  @Test
  public void manyItemsEstimationMode() {
    final KllFloatsSketch sketch = new KllFloatsSketch();
    final int n = 1000000;
    for (int i = 0; i < n; i++) {
      sketch.update(i);
      Assert.assertEquals(sketch.getN(), i + 1);
    }

    // test getRank
    for (int i = 0; i < n; i++) {
      final double trueRank = (double) i / (n - 1);
      Assert.assertEquals(sketch.getRank(i), trueRank, EPS_FOR_K_256, "for value " + i);
    }

    // test getPMF
    final double[] pmf = sketch.getPMF(new float[] {n / 2}); // split at median
    Assert.assertEquals(pmf.length, 2);
    Assert.assertEquals(pmf[0], 0.5, EPS_FOR_K_256);
    Assert.assertEquals(pmf[1], 0.5, EPS_FOR_K_256);

    Assert.assertEquals(sketch.getMinValue(), 0f); // min value is exact
    Assert.assertEquals(sketch.getQuantile(0), 0f); // min value is exact
    Assert.assertEquals(sketch.getMaxValue(), (float) (n - 1)); // max value is exact
    Assert.assertEquals(sketch.getQuantile(1), (float) (n - 1)); // max value is exact

    // check at every 0.1 percentage point
    final double[] fractions = new double[1001];
    for (int i = 0; i <= 1000; i++) {
      fractions[i] = (double) i / 1000;
    }
    final float[] quantiles = sketch.getQuantiles(fractions);
    float previousQuantile = 0;
    for (int i = 0; i <= 1000; i++) {
      final float quantile = sketch.getQuantile(fractions[i]);
      Assert.assertEquals(quantile, quantiles[i]);
      Assert.assertTrue(previousQuantile <= quantile);
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
      Assert.assertEquals(ranks[i], sketch.getRank(values[i]), NUMERIC_NOISE_TOLERANCE, "rank vs CDF for value " + i);
      sumPmf += pmf[i];
      Assert.assertEquals(ranks[i], sumPmf, NUMERIC_NOISE_TOLERANCE, "CDF vs PMF for value " + i);
    }
    sumPmf += pmf[n];
    Assert.assertEquals(sumPmf, 1.0, NUMERIC_NOISE_TOLERANCE);
    Assert.assertEquals(ranks[n], 1.0, NUMERIC_NOISE_TOLERANCE);
  }

  @Test
  public void floorLog2() {
    Assert.assertEquals(KllFloatsSketch.floor_of_log2_of_fraction(0, 1), 0);
    Assert.assertEquals(KllFloatsSketch.floor_of_log2_of_fraction(1, 2), 0);
    Assert.assertEquals(KllFloatsSketch.floor_of_log2_of_fraction(2, 2), 0);
    Assert.assertEquals(KllFloatsSketch.floor_of_log2_of_fraction(3, 2), 0);
    Assert.assertEquals(KllFloatsSketch.floor_of_log2_of_fraction(4, 2), 1);
    Assert.assertEquals(KllFloatsSketch.floor_of_log2_of_fraction(5, 2), 1);
    Assert.assertEquals(KllFloatsSketch.floor_of_log2_of_fraction(6, 2), 1);
    Assert.assertEquals(KllFloatsSketch.floor_of_log2_of_fraction(7, 2), 1);
    Assert.assertEquals(KllFloatsSketch.floor_of_log2_of_fraction(8, 2), 2);
  }

  @Test
  public void merge() {
    final KllFloatsSketch sketch1 = new KllFloatsSketch();
    final KllFloatsSketch sketch2 = new KllFloatsSketch();
    final int n = 10000;
    for (int i = 0; i < n; i++) {
      sketch1.update(i);
      sketch2.update(2 * n - i - 1);
    }

    Assert.assertEquals(sketch2.getMinValue(), (float) n);
    Assert.assertEquals(sketch2.getMaxValue(), (float) (2 * n - 1));

    Assert.assertEquals(sketch2.getMinValue(), (float) n);
    Assert.assertEquals(sketch2.getMaxValue(), (float) (2 * n - 1));

    sketch1.merge(sketch2);

    Assert.assertFalse(sketch1.isEmpty());
    Assert.assertEquals(sketch1.getN(), 2 * n);
    Assert.assertEquals(sketch1.getMinValue(), 0f);
    Assert.assertEquals(sketch1.getMaxValue(), (float) (2 * n - 1));
    Assert.assertEquals(sketch1.getQuantile(0.5), n, n * EPS_FOR_K_256);
  }

  @Test
  public void mergeLowerK() {
    final KllFloatsSketch sketch1 = new KllFloatsSketch(256);
    final KllFloatsSketch sketch2 = new KllFloatsSketch(128);
    final int n = 10000;
    for (int i = 0; i < n; i++) {
      sketch1.update(i);
      sketch2.update(2 * n - i - 1);
    }

    Assert.assertEquals(sketch2.getMinValue(), (float) n);
    Assert.assertEquals(sketch2.getMaxValue(), (float) (2 * n - 1));

    Assert.assertEquals(sketch2.getMinValue(), (float) n);
    Assert.assertEquals(sketch2.getMaxValue(), (float) (2 * n - 1));

    Assert.assertTrue(sketch1.getNormalizedRankError() < sketch2.getNormalizedRankError());

    sketch1.merge(sketch2);

    // sketch1 must get "contaminated" by the lower K in sketch2
    Assert.assertEquals(sketch1.getNormalizedRankError(), sketch2.getNormalizedRankError());

    Assert.assertFalse(sketch1.isEmpty());
    Assert.assertEquals(sketch1.getN(), 2 * n);
    Assert.assertEquals(sketch1.getMinValue(), 0f);
    Assert.assertEquals(sketch1.getMaxValue(), (float) (2 * n - 1));
    Assert.assertEquals(sketch1.getQuantile(0.5), n, n * EPS_FOR_K_128);
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
    final double rankErrorBeforeMerge = sketch1.getNormalizedRankError();
    sketch1.merge(sketch2);
    Assert.assertEquals(sketch1.getNormalizedRankError(), rankErrorBeforeMerge);

    Assert.assertFalse(sketch1.isEmpty());
    Assert.assertEquals(sketch1.getN(), n);
    Assert.assertEquals(sketch1.getMinValue(), 0f);
    Assert.assertEquals(sketch1.getMaxValue(), (float) (n - 1));
    Assert.assertEquals(sketch1.getQuantile(0.5), n / 2, n / 2 * EPS_FOR_K_256);
}

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void kTooSmall() {
    new KllFloatsSketch(KllFloatsSketch.MIN_K - 1);
  }

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
    Assert.assertEquals(sketch.getQuantile(0.5), 500, 500 * EPS_FOR_K_8);
  }

  @Test
  public void maxK() {
    final KllFloatsSketch sketch = new KllFloatsSketch(KllFloatsSketch.MAX_K);
    for (int i = 0; i < 1000; i++) {
      sketch.update(i);
    }
    Assert.assertEquals(sketch.getQuantile(0.5), 500, 500 * EPS_FOR_K_256);
  }

  @Test
  public void serializeDeserializeEmpty() {
    final KllFloatsSketch sketch1 = new KllFloatsSketch();
    final byte[] bytes = sketch1.toByteArray();
    KllFloatsSketch sketch2 = KllFloatsSketch.heapify(Memory.wrap(bytes));
    Assert.assertEquals(bytes.length, sketch1.getSerializedSizeBytes());
    Assert.assertTrue(sketch2.isEmpty());
    Assert.assertEquals(sketch2.getNumRetained(), sketch1.getNumRetained());
    Assert.assertEquals(sketch2.getN(), sketch1.getN());
    Assert.assertEquals(sketch2.getNormalizedRankError(), sketch1.getNormalizedRankError());
    Assert.assertTrue(Float.isNaN(sketch2.getMinValue()));
    Assert.assertTrue(Float.isNaN(sketch2.getMaxValue()));
    Assert.assertEquals(sketch2.getSerializedSizeBytes(), sketch1.getSerializedSizeBytes());
    System.out.println(sketch2.toString(true, true));
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
    Assert.assertEquals(bytes.length, sketch1.getSerializedSizeBytes());
    Assert.assertFalse(sketch2.isEmpty());
    Assert.assertEquals(sketch2.getNumRetained(), sketch1.getNumRetained());
    Assert.assertEquals(sketch2.getN(), sketch1.getN());
    Assert.assertEquals(sketch2.getNormalizedRankError(), sketch1.getNormalizedRankError());
    Assert.assertEquals(sketch2.getMinValue(), sketch1.getMinValue());
    Assert.assertEquals(sketch2.getMaxValue(), sketch1.getMaxValue());
    Assert.assertEquals(sketch2.getSerializedSizeBytes(), sketch1.getSerializedSizeBytes());
  }

}
