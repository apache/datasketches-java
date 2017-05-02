package com.yahoo.sketches.performance;

import java.util.Comparator;
import java.util.Random;

import org.testng.annotations.Test;

import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.UpdateDoublesSketch;
import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.quantiles.ItemsSketch;
import com.yahoo.sketches.ArrayOfDoublesSerDe;
import com.yahoo.memory.Memory;
//import com.yahoo.sketches.ArrayOfLongsSerDe;
//import com.yahoo.sketches.ArrayOfStringsSerDe;
import com.yahoo.memory.WritableMemory;

@SuppressWarnings("unused")
public class QuantilesPerformanceTest {
  private static final Random random = new Random(0);

  @Test
  public void sketchOps() {
    final int sketchSize = 128;
    final double distrParam = 0.005;
    final int firstPowerOfTwo = 0;
    final int lastPowerOfTwo = 23;
    final int minIterations = 30;
    final int stepsPerPowerOfTwo = 16;
    final double step = 1.0 / stepsPerPowerOfTwo;
    warmUp();
    System.out.println("items, iterations, items * iterations, build, update, serialize, deserialize, get quantiles, get pmf, size bytes");
    //ArrayOfItemsSerDe<Long> serDe = new ArrayOfLongsSerDe();
    //ArrayOfItemsSerDe<Double> serDe = new ArrayOfDoublesSerDe();
    //ArrayOfItemsSerDe<String> serDe = new ArrayOfStringsSerDe();
    //ArrayOfItemsSerDe<String> serDe = new ArrayOfUtf16StringsSerDe();

    // prepare input
    final int maxItems = (int) Math.pow(2, lastPowerOfTwo);
    final double[] values = new double[maxItems];
    for (int j = 0; j < maxItems; j++) values[j] = randomGeometricDist(distrParam);
    //final String[] valuesStr = new String[maxItems];
    //for (int j = 0; j < maxItems; j++) { final String s = Long.toString(Util.hash(randomGeometricDist(distrParam))); valuesStr[j] = s; }

    for (double powerOfTwo = firstPowerOfTwo; powerOfTwo <= lastPowerOfTwo; powerOfTwo += step) {
      int numItems = (int) Math.pow(2, powerOfTwo);
      // this is to have exponentially more iterations at low numbers of items to improve signal to noise ratio
      int numIterations = (int)(minIterations * Math.pow(1.8, lastPowerOfTwo - powerOfTwo));

      long buildTimeNanoSecs = 0;
      long updateTimeNanoSecs = 0;
      long serializeTimeNanoSecs = 0;
      long deserializeTimeNanoSecs = 0;
      long serializedSize = 0;
      long getQuantilesTimeNanoSecs = 0;
      long getPmfTimeNanoSecs = 0;

      for (int i = 0; i < numIterations; i++) {
        final long buildStart = System.nanoTime();
        final UpdateDoublesSketch sketch = DoublesSketch.builder().build(sketchSize);
        //final ItemsSketch<Double> sketch = ItemsSketch.getInstance(sketchSize, Comparator.naturalOrder());
        //final ItemsSketch<Long> sketch = ItemsSketch.getInstance(sketchSize, Comparator.naturalOrder());
        final long buildStop1 = System.nanoTime();
        final long buildStop2 = System.nanoTime();
        buildTimeNanoSecs += (buildStop1 - buildStart - (buildStop2 - buildStop1));

        final long updateStart = System.nanoTime();
        for (int j = 0; j < numItems; j++) sketch.update(values[j]);
        //for (int j = 0; j < numItems; j++) sketch.update(valuesStr[j]);
        final long updateStop1 = System.nanoTime();
        final long updateStop2 = System.nanoTime();
        updateTimeNanoSecs += (updateStop1 - updateStart - (updateStop2 - updateStop1));

        final long serializeStart = System.nanoTime();
        final byte[] bytes = sketch.toByteArray();
        //final byte[] bytes = sketch.toByteArray(serDe);
        final long serializeStop1 = System.nanoTime();
        final long serializeStop2 = System.nanoTime();
        serializeTimeNanoSecs += (serializeStop1 - serializeStart - (serializeStop2 - serializeStop1));
        serializedSize += bytes.length;

        final long deserializeStart = System.nanoTime();
        final DoublesSketch deserializedSketch = DoublesSketch.heapify(Memory.wrap(bytes));
        //final ItemsSketch<Double> deserializedSketch = ItemsSketch.getInstance(new NativeMemory(bytes), Comparator.naturalOrder(), serDe);
        final long deserializeStop1 = System.nanoTime();
        final long deserializeStop2 = System.nanoTime();
        deserializeTimeNanoSecs += (deserializeStop1 - deserializeStart - (deserializeStop2 - deserializeStop1));

        final long getQuantilesStart = System.nanoTime();
        final double[] splitPoints = deserializedSketch.getQuantiles((int) Math.min(10, deserializedSketch.getN()));
        //final Double[] splitPoints = deserializedSketch.getQuantiles((int) Math.min(10, deserializedSketch.getN()));
        final long getQuantilesStop1 = System.nanoTime();
        final long getQuantilesStop2 = System.nanoTime();
        getQuantilesTimeNanoSecs += (getQuantilesStop1 - getQuantilesStart  - (getQuantilesStop2 - getQuantilesStop1));

        //System.out.println("Split Points: " + Arrays.asList(splitPoints));
        final long getPmfStart = System.nanoTime();
        final double[] fractions = deserializedSketch.getPMF(splitPoints);
        final long getPmfStop1 = System.nanoTime();
        final long getPmfStop2 = System.nanoTime();
        getPmfTimeNanoSecs += (getPmfStop1 - getPmfStart  - (getPmfStop2 - getPmfStop1));
      }
      System.out.println(
          numItems + " " + numIterations + " " + numIterations * numItems
        + " " + String.format("%.1f", buildTimeNanoSecs / (double) numIterations)
        + " " + String.format("%.1f", updateTimeNanoSecs / (double) numIterations / numItems)
        + " " + String.format("%.1f", serializeTimeNanoSecs / (double) numIterations)
        + " " + String.format("%.1f", deserializeTimeNanoSecs / (double) numIterations)
        + " " + String.format("%.1f", getQuantilesTimeNanoSecs / (double) numIterations)
        + " " + String.format("%.1f", getPmfTimeNanoSecs / (double) numIterations)
        + " " + String.format("%d", serializedSize / numIterations)
      );
    }
  }

  private static void warmUp() {
    // warm up
    for (int i = 0; i < 200; i++) {
      ItemsSketch<Integer> sketch = ItemsSketch.getInstance(1024, Comparator.naturalOrder());
      for (int j = 0; j < 1000000; j++) sketch.update(j);
    }
  }

  /**
   * @param prob the probability of success for the geometric distribution.
   * @return a random number (1,inf) generated from the geometric distribution.
   */
  static private long randomGeometricDist(double prob) {
    //assert(prob > 0.0 && prob < 1.0);
    return 1 + (long) (Math.log(random.nextDouble()) / Math.log(1.0 - prob));
  }

}
