package com.yahoo.sketches.performance;

import java.util.Random;

import org.testng.annotations.Test;

import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.ArrayOfLongsSerDe;
import com.yahoo.sketches.ArrayOfStringsSerDe;
import com.yahoo.sketches.frequencies.ItemsSketch;
import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.memory.WritableMemory;

import static com.yahoo.sketches.frequencies.DistTest.zipf;
import static com.yahoo.sketches.frequencies.DistTest.zeta;

@SuppressWarnings("unused")
public class FrequentItemsPerformanceTest {
  private static final Random random = new Random(0);

  //@Test
  @SuppressWarnings({ "rawtypes", "unused" })
  public void frequentItems() {
    final int iterations = 2000;
    final int items = 1000000;

    long buildTimeNanoSecs = 0;
    long updateTimeNanoSecs = 0;
    long serializeTimeNanoSecs = 0;
    long deserializeTimeNanoSecs = 0;
    int numItems = 0;

    warmUp();

    ArrayOfItemsSerDe<Long> serDe = new ArrayOfLongsSerDe();
    //ArrayOfItemsSerDe<String> serDe = new ArrayOfStringsSerDe();

    for (int i = 0; i < iterations; ++i) {
      final long buildStart = System.nanoTime();
      //SpaceSavingStringTopK top = new SpaceSavingStringTopK(128);
      //SpaceSavingLongTopK top = new SpaceSavingLongTopK(1024);
      //FrequentLongsSketch sketch = new FrequentLongsSketch(1024);
      ItemsSketch<Long> sketch = new ItemsSketch<Long>(1024);
      //FrequentItemsSketch<String> sketch = new FrequentItemsSketch<String>(1024);
      buildTimeNanoSecs += (System.nanoTime() - buildStart);

      final long[] values = new long[items];
      for (int j = 0; j < items; j++) values[j] = randomGeometricDist(0.01);
      //final String[] valuesStr = new String[items];
      //for (int j = 0; j < items; j++) valuesStr[j] = Long.toString(randomGeometricDist(0.01));

      final long updateStart = System.nanoTime();
      for (int j = 0; j < items; j++) sketch.update(values[j]);
      //for (int j = 0; j < items; j++) sketch.update(valuesStr[j]);
      updateTimeNanoSecs += (System.nanoTime() - updateStart);

      //for (int j = 0; j < items; j++) {
      //  final long value = randomGeometricDist(0.01);
      //  final String valueStr = Long.toString(value);
      //  final long updateStart = System.nanoTime();
      //  //sketch.update(value);
      //  sketch.update(valueStr);
      //  updateTimeNanoSecs += (System.nanoTime() - updateStart);
      //}

      final long serializeStart = System.nanoTime();
      //byte[] bytes = sketch.serializeToByteArray();
      byte[] bytes = sketch.toByteArray(serDe);
      serializeTimeNanoSecs += (System.nanoTime() - serializeStart);

      final long deserializeStart = System.nanoTime();
      //FrequentLongsSketch deserializedSketch = FrequentLongsSketch.getInstance(new NativeMemory(bytes));
      ItemsSketch deserializedSketch = ItemsSketch.getInstance(WritableMemory.wrap(bytes), serDe);
      deserializeTimeNanoSecs += (System.nanoTime() - deserializeStart);

      //numItems += top.getNumElements();
      numItems += sketch.getNumActiveItems();
    }
    System.out.println(
        "build time: " + String.format("%.2f", buildTimeNanoSecs / (double) iterations)
      + ", update time: " + String.format("%.2f", updateTimeNanoSecs / (double) iterations / items)
      + ", serialize time: " + String.format("%.2f", serializeTimeNanoSecs / (double) iterations)
      + ", deserialize time: " + String.format("%.2f", deserializeTimeNanoSecs / (double) iterations)
      + ", items: " + numItems / iterations
    );
  }

  //@Test
  public void numActiveItems() {
    final int items = 500000;
    final ItemsSketch<Long> sketch = new ItemsSketch<Long>(1024);
    for (int j = 0; j < items; j++) {
      sketch.update(randomGeometricDist(0.005));
    }
    System.out.println("items: " + sketch.getNumActiveItems());
    //System.out.println("sketch: " + sketch.serializeToString());
    //long sl = 0;
    //long[] values = sketch.hashMap.getActiveValues();
    //for (long value: values) {
    //  sl += value;
    //}
    //System.out.println("total counts: " + sl + ", steam length: " + sketch.getStreamLength());
  }

  //@Test
  @SuppressWarnings("rawtypes")
  public void sketchOps() {
    final int sketchSize = 32768;
    final double distrParam = 0.0002;
    final int firstPowerOfTwo = 2;
    final int lastPowerOfTwo = 23;
    final int minIterations = 20;
    final int stepsPerPowerOfTwo = 16;
    final double step = 1.0 / stepsPerPowerOfTwo;
    warmUp();
    System.out.println("items, iterations, items * iterations, build, update, serialize, deserialize, active items");
    ArrayOfItemsSerDe<Long> serDe = new ArrayOfLongsSerDe();
    //ArrayOfItemsSerDe<String> serDe = new ArrayOfStringsSerDe();
    //ArrayOfItemsSerDe<String> serDe = new ArrayOfUtf16StringsSerDe();

    // prepare input
    final int maxItems = (int) Math.pow(2, lastPowerOfTwo);
    final long[] values = new long[maxItems];
    for (int j = 0; j < maxItems; j++) values[j] = randomGeometricDist(distrParam);
    //final String[] valuesStr = new String[maxItems];
    //for (int j = 0; j < maxItems; j++) { final String s = Long.toString(hash(randomGeometricDist(distrParam))); valuesStr[j] = s; }

    for (double powerOfTwo = firstPowerOfTwo; powerOfTwo <= lastPowerOfTwo; powerOfTwo += step) {
      int numItems = (int) Math.pow(2, powerOfTwo);
      // this is to have exponentially more iterations at low numbers of items to improve signal to noise ratio
      int numIterations = (int)(minIterations * Math.pow(1.8, lastPowerOfTwo - powerOfTwo));

      long buildTimeNanoSecs = 0;
      long updateTimeNanoSecs = 0;
      long serializeTimeNanoSecs = 0;
      long deserializeTimeNanoSecs = 0;
      int numActiveItems = 0;

      for (int i = 0; i < numIterations; i++) {
        final long buildStart = System.nanoTime();
        //FrequentLongsSketch sketch = new FrequentLongsSketch(sketchSize);
        ItemsSketch<Long> sketch = new ItemsSketch<Long>(sketchSize);
        //ItemsSketch<String> sketch = new ItemsSketch<String>(sketchSize);
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
        //byte[] bytes = sketch.serializeToByteArray();
        byte[] bytes = sketch.toByteArray(serDe);
        final long serializeStop1 = System.nanoTime();
        final long serializeStop2 = System.nanoTime();
        serializeTimeNanoSecs += (serializeStop1 - serializeStart - (serializeStop2 - serializeStop1));

        final long deserializeStart = System.nanoTime();
        //FrequentLongsSketch deserializedSketch = FrequentLongsSketch.getInstance(new NativeMemory(bytes));
        ItemsSketch deserializedSketch = ItemsSketch.getInstance(WritableMemory.wrap(bytes), serDe);
        final long deserializeStop1 = System.nanoTime();
        final long deserializeStop2 = System.nanoTime();
        deserializeTimeNanoSecs += (deserializeStop1 - deserializeStart - (deserializeStop2 - deserializeStop1));

        numActiveItems += deserializedSketch.getNumActiveItems();
      }
      System.out.println(
          numItems + " " + numIterations + " " + numIterations * numItems
        + " " + String.format("%.1f", buildTimeNanoSecs / (double) numIterations)
        + " " + String.format("%.1f", updateTimeNanoSecs / (double) numIterations / numItems)
        + " " + String.format("%.1f", serializeTimeNanoSecs / (double) numIterations)
        + " " + String.format("%.1f", deserializeTimeNanoSecs / (double) numIterations)
        + " " + String.format("%.1f", numActiveItems / (double) numIterations)
      );
    }
  }

  //@Test
  @SuppressWarnings({ "rawtypes", "unused" })
  public void merge() {
    final int sketchSize = 32768;
    final double distrParam = 0.0002;
    final int firstPowerOfTwo = 2;
    final int lastPowerOfTwo = 23;
    //final int minIterations = 60;
    final int minIterations = 2;
    final int stepsPerPowerOfTwo = 16;
    final double step = 1.0 / stepsPerPowerOfTwo;
    warmUp();
    System.out.println("items, iterations, items * iterations, build, update, merge, active items");
    //ArrayOfItemsSerDe<Long> serDe = new ArrayOfLongsSerDe();
    ArrayOfItemsSerDe<String> serDe = new ArrayOfStringsSerDe();
    //ArrayOfItemsSerDe<String> serDe = new ArrayOfUtf16StringsSerDe();

    // prepare input
    final int maxItems = (int) Math.pow(2, lastPowerOfTwo);
    //final long[] values = new long[maxItems];
    //for (int j = 0; j < maxItems; j++) values[j] = randomGeometricDist(distrParam);
    final String[] valuesStr1 = new String[maxItems];
    for (int j = 0; j < maxItems; j++) { final String s = Long.toString(hash(randomGeometricDist(distrParam))); valuesStr1[j] = s; }
    final String[] valuesStr2 = new String[maxItems];
    for (int j = 0; j < maxItems; j++) { final String s = Long.toString(hash(randomGeometricDist(distrParam))); valuesStr2[j] = s; }

    for (double powerOfTwo = firstPowerOfTwo; powerOfTwo <= lastPowerOfTwo; powerOfTwo += step) {
      int numItems = (int) Math.pow(2, powerOfTwo);
      // this is to have exponentially more iterations at low numbers of items to improve signal to noise ratio
      int numIterations = (int)(minIterations * Math.pow(1.8, lastPowerOfTwo - powerOfTwo));

      long buildTimeNanoSecs = 0;
      long updateTimeNanoSecs = 0;
      long mergeTimeNanoSecs = 0;
      int numActiveItems = 0;

      for (int i = 0; i < numIterations; i++) {
        final long buildStart = System.nanoTime();
        //FrequentLongsSketch sketch = new FrequentLongsSketch(sketchSize);
        //FrequentItemsSketch<Long> sketch = new FrequentItemsSketch<Long>(sketchSize);
        ItemsSketch<String> sketch1 = new ItemsSketch<String>(sketchSize);
        ItemsSketch<String> sketch2 = new ItemsSketch<String>(sketchSize);
        final long buildStop1 = System.nanoTime();
        final long buildStop2 = System.nanoTime();
        buildTimeNanoSecs += (buildStop1 - buildStart - (buildStop2 - buildStop1));

        final long updateStart = System.nanoTime();
        //for (int j = 0; j < numItems; j++) sketch.update(values[j]);
        for (int j = 0; j < numItems; j++) sketch1.update(valuesStr1[j]);
        for (int j = 0; j < numItems; j++) sketch2.update(valuesStr2[j]);
        final long updateStop1 = System.nanoTime();
        final long updateStop2 = System.nanoTime();
        updateTimeNanoSecs += (updateStop1 - updateStart - (updateStop2 - updateStop1));

        final long mergeStart = System.nanoTime();
        sketch1.merge(sketch2);
        final long mergeStop1 = System.nanoTime();
        final long mergeStop2 = System.nanoTime();
        mergeTimeNanoSecs += (mergeStop1 - mergeStart - (mergeStop2 - mergeStop1));

        numActiveItems += sketch1.getNumActiveItems();
      }
      System.out.println(
          numItems + " " + numIterations + " " + numIterations * numItems
        + " " + String.format("%.1f", buildTimeNanoSecs / (double) numIterations)
        + " " + String.format("%.1f", updateTimeNanoSecs / (double) numIterations / numItems)
        + " " + String.format("%.1f", mergeTimeNanoSecs / (double) numIterations)
        + " " + String.format("%.1f", numActiveItems / (double) numIterations)
      );
    }
  }

  //@Test
  public void _merge() {
    final int sketchSize = 32768;
    final double distrParam = 0.0002;
    final int zipfn = 100000;
    final double zetan = zeta(zipfn, 1.05);
    final int firstPowerOfTwo = 10;
    final int lastPowerOfTwo = 25;
    final int minIterations = 2;
    final int stepsPerPowerOfTwo = 16;
    final double step = 1.0 / stepsPerPowerOfTwo;
    warmUp();
    System.out.println("items, iterations, items * iterations, build, update, merge, active items");
    ArrayOfItemsSerDe<Long> serDe = new ArrayOfLongsSerDe();

    for (double powerOfTwo = firstPowerOfTwo; powerOfTwo <= lastPowerOfTwo; powerOfTwo += step) {
      int numItems = (int) Math.pow(2, powerOfTwo);
      // this is to have exponentially more iterations at low numbers of items to improve signal to noise ratio
      int numIterations = (int)(minIterations * Math.pow(1.8, lastPowerOfTwo - powerOfTwo));

      long buildTimeNanoSecs = 0;
      long updateTimeNanoSecs = 0;
      long mergeTimeNanoSecs = 0;
      int numActiveItems = 0;

      for (int i = 0; i < numIterations; i++) {
        final long buildStart = System.nanoTime();
        //FrequentLongsSketch sketch = new FrequentLongsSketch(sketchSize);
        ItemsSketch<Long> sketch1 = new ItemsSketch<Long>(sketchSize);
        ItemsSketch<Long> sketch2 = new ItemsSketch<Long>(sketchSize);
        final long buildStop1 = System.nanoTime();
        final long buildStop2 = System.nanoTime();
        buildTimeNanoSecs += (buildStop1 - buildStart - (buildStop2 - buildStop1));

        final long updateStart = System.nanoTime();
        for (int j = 0; j < numItems; j++) sketch1.update(zipf(1.05, zipfn, zetan));
        for (int j = 0; j < numItems; j++) sketch2.update(zipf(1.05, zipfn, zetan));
        final long updateStop1 = System.nanoTime();
        final long updateStop2 = System.nanoTime();
        updateTimeNanoSecs += (updateStop1 - updateStart - (updateStop2 - updateStop1));

        final long mergeStart = System.nanoTime();
        sketch1.merge(sketch2);
        final long mergeStop1 = System.nanoTime();
        final long mergeStop2 = System.nanoTime();
        mergeTimeNanoSecs += (mergeStop1 - mergeStart - (mergeStop2 - mergeStop1));

        numActiveItems += sketch1.getNumActiveItems();
      }
      System.out.println(
          numItems + " " + numIterations + " " + numIterations * numItems
        + " " + String.format("%.1f", buildTimeNanoSecs / (double) numIterations)
        + " " + String.format("%.1f", updateTimeNanoSecs / (double) numIterations / numItems)
        + " " + String.format("%.1f", mergeTimeNanoSecs / (double) numIterations)
        + " " + String.format("%.1f", numActiveItems / (double) numIterations)
      );
    }
  }

  @Test
  public void __merge() {
    final int sketchSize = 32768;
    final int numItems = 500_000;
    final double alpha = 1.05;
    final double zetan = zeta(numItems, alpha);
    final int numIterations = 1;

    int sketch1ActiveItems = 0;
    int sketch2ActiveItems = 0;
    int sketch3ActiveItems = 0;
    int mergedActiveItems = 0;
    long sketch1MaxError = 0;
    long sketch2MaxError = 0;
    long sketch3MaxError = 0;
    long mergedMaxError = 0;
    long mergeTimeNanoSecs = 0;

    for (int iteration = 0; iteration < numIterations; iteration++) {
      ItemsSketch<Long> sketch1 = new ItemsSketch<Long>(sketchSize);
      ItemsSketch<Long> sketch2 = new ItemsSketch<Long>(sketchSize);
      //ItemsSketch<Long> sketch3 = new ItemsSketch<Long>(sketchSize);
      for (int i = 0; i < numItems; i++) {
        sketch1.update(zipf(alpha, numItems, zetan), random.nextInt(10000));
        sketch2.update(zipf(alpha, numItems, zetan), random.nextInt(10000));
        //sketch3.update(zipf(alpha, numItems, zetan), random.nextInt(10000));
      }
      sketch1ActiveItems += sketch1.getNumActiveItems();
      sketch2ActiveItems += sketch2.getNumActiveItems();
      //sketch3ActiveItems += sketch3.getNumActiveItems();
      sketch1MaxError += sketch1.getMaximumError();
      sketch2MaxError += sketch2.getMaximumError();
      //sketch3MaxError += sketch3.getMaximumError();

      final long mergeStart = System.nanoTime();
      sketch1.merge(sketch2);
      //sketch1.merge(sketch3);
      final long mergeStop = System.nanoTime();
      mergeTimeNanoSecs += mergeStop - mergeStart;
      mergedActiveItems += sketch1.getNumActiveItems();
      mergedMaxError += sketch1.getMaximumError();
    }
    System.out.println("Sketch 1 active items: " + String.format("%d", (int) (sketch1ActiveItems / (double) numIterations)));
    System.out.println("Sketch2 active items: " + String.format("%d", (int) (sketch2ActiveItems / (double) numIterations)));
    //System.out.println("Sketch3 active items: " + String.format("%d", (int) (sketch3ActiveItems / (double) numIterations)));
    System.out.println("Merged active items: " + String.format("%d", (int) (mergedActiveItems / (double) numIterations)));
    System.out.println("Sketch 1 max error: " + String.format("%d", (int) (sketch1MaxError / (double) numIterations)));
    System.out.println("Sketch 2 max error: " + String.format("%d", (int) (sketch2MaxError / (double) numIterations)));
    //System.out.println("Sketch 3 max error: " + String.format("%d", (int) (sketch3MaxError / (double) numIterations)));
    System.out.println("Merged max error: " + String.format("%d", (int) (mergedMaxError / (double) numIterations)));
    System.out.println("Merge time, ns: " + String.format("%.1f", mergeTimeNanoSecs / (double) numIterations));
  }

  private static void warmUp() {
    // warm up
    for (int i = 0; i < 200; i++) {
      ItemsSketch<Integer> sketch = new ItemsSketch<Integer>(1024);
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

  /**
   * @param key to be hashed
   * @return an index into the hash table This hash function is taken from the internals of 
   * Austin Appleby's MurmurHash3 algorithm. It is also used by the Trove for Java libraries.
   */
  static long hash(long key) {
    key ^= key >>> 33;
    key *= 0xff51afd7ed558ccdL;
    key ^= key >>> 33;
    key *= 0xc4ceb9fe1a85ec53L;
    key ^= key >>> 33;
    return key;
  }

}
