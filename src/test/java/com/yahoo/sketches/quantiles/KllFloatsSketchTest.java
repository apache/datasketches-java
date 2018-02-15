package com.yahoo.sketches.quantiles;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

public class KllFloatsSketchTest {

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
    System.out.println(sketch.toString(true, true));

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

    System.out.println(sketch1.toString(true, true));
    System.out.println(sketch2.toString(true, true));

    Assert.assertEquals(sketch2.getMinValue(), (float) n);
    Assert.assertEquals(sketch2.getMaxValue(), (float) (2 * n - 1));

    Assert.assertEquals(sketch2.getMinValue(), (float) n);
    Assert.assertEquals(sketch2.getMaxValue(), (float) (2 * n - 1));

    sketch1.merge(sketch2);

    System.out.println(sketch1.toString(true, true));

    Assert.assertFalse(sketch1.isEmpty());
    Assert.assertEquals(sketch1.getN(), 2 * n);
    Assert.assertEquals(sketch1.getMinValue(), 0f);
    Assert.assertEquals(sketch1.getMaxValue(), (float) (2 * n - 1));
    Assert.assertEquals(sketch1.getQuantile(0.5), n, n * EPS_FOR_K_256);
  }

  // for animation
  //@Test
  public void printLevels() throws Exception {
    final int n = 1_000_000;
    Random rnd = new Random();
    final KllFloatsSketch sketch = new KllFloatsSketch(128);
    OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream("levels.tsv"));
    for (int i = 0; i < n; i++) {
      sketch.update(rnd.nextFloat());
      out.write(joinInts(sketch.getLevelSizes(), "\t"));
      out.write("\t");
      out.write(joinInts(sketch.getNominalLevelCapacities(), "\t"));
      out.write("\t");
      out.write(Integer.toString(sketch.getNumRetained()));
      out.write(Util.LS);
    }
    out.close();
    System.out.println(sketch.toString(true, true));
  }

  private static String joinInts(final int[] array, final String delimiter) {
    final int numFields = 13;
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < numFields; i++) {
      if (i > 0) {
        sb.append(delimiter);
      }
      if (i < array.length) {
        sb.append(array[i]);
      } else {
        sb.append(0);
      }
    }
    return sb.toString();
  }

}
