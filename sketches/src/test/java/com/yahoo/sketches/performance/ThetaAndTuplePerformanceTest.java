package com.yahoo.sketches.performance;

import org.testng.annotations.Test;

import java.util.Random;

import org.testng.Assert;

import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.HllSketchBuilder;
import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.theta.UpdateSketch;
import com.yahoo.sketches.theta.UpdateSketchBuilder;
import com.yahoo.sketches.tuple.AnotB;
import com.yahoo.sketches.tuple.ArrayOfDoublesCombiner;
import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;
import com.yahoo.sketches.tuple.CompactSketch;
import com.yahoo.sketches.tuple.DoubleSummary;
import com.yahoo.sketches.tuple.DoubleSummaryFactory;
import com.yahoo.sketches.tuple.Sketch;
import com.yahoo.sketches.tuple.UpdatableSketch;
import com.yahoo.sketches.tuple.UpdatableSketchBuilder;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.QuickSelect;

@SuppressWarnings("unused")
public class ThetaAndTuplePerformanceTest {
  // measures update time for different number of unique values presented to a fixed-size sketch
  // prints nanoseconds per one update operation
  //@Test
  @SuppressWarnings("unused")
  public void sketchOperations() {
    final int sketchSize = 65536;
    final int firstPowerOfTwo = 2;
    final int lastPowerOfTwo = 23;
    final int minIterations = 4;
    final int stepsPerPowerOfTwo = 16;
    final double step = 1.0 / stepsPerPowerOfTwo;
    long key = 1; // key to present to a sketch, which is unique across all trials
    final double[] value = { 1 };

    warmUp();

    //final Memory mem = new AllocMemory(100000000);
    //final Memory mem2 = new AllocMemory(100000000);
    final UpdateSketchBuilder builder = new UpdateSketchBuilder().setNominalEntries(sketchSize);
    //final UpdateSketchBuilder builder = new UpdateSketchBuilder().setNominalEntries(sketchSize).initMemory(mem);
    //final ArrayOfDoublesUpdatableSketchBuilder builder = new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(sketchSize);
    //final ArrayOfDoublesUpdatableSketchBuilder builder = new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(sketchSize).setMemory(mem);
    //final UpdatableSketchBuilder<Double, DoubleSummary> builder = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).setNominalEntries(sketchSize);
    System.out.println("uniques, iterations, build, update, serialize updatable, deserialize updatable, compact, serilaize compact, deserialize compact");

    for (double powerOfTwo = firstPowerOfTwo; powerOfTwo <= lastPowerOfTwo; powerOfTwo += step) {
      int nUniques = (int) Math.pow(2, powerOfTwo);
      long buildTimeNanoSecs = 0;
      long updateTimeNanoSecs = 0;
      long serializeUpdatableTimeNanoSecs = 0;
      long deserializeUpdatableTimeNanoSecs = 0;
      long compactingTimeNanoSecs = 0;
      long serializeCompactTimeNanoSecs = 0;
      long deserializeCompactTimeNanoSecs = 0;
      // this is to have exponentially more iterations at low numbers of uniques to improve signal to noise ratio
      int numIterations = (int)(minIterations * Math.pow(2, lastPowerOfTwo - powerOfTwo));
      for (int i = 1; i <= numIterations; i++) {
        // build sketch
        final long startBuilder = System.nanoTime();
        final UpdateSketch sketch = builder.build();
        //final ArrayOfDoublesUpdatableSketch sketch = builder.build();
        //UpdatableSketch<Double, DoubleSummary> sketch = builder.build();
        final long stopBuilder1 = System.nanoTime();
        final long stopBuilder2 = System.nanoTime();
        buildTimeNanoSecs += (stopBuilder1 - startBuilder - (stopBuilder2 - stopBuilder1));

        // update sketch
        final long startUpdate = System.nanoTime();
        for (int j = 1; j <= nUniques; j++) sketch.update(key++);
        //for (int j = 1; j <= nUniques; j++) sketch.update(key++, value);
        //for (int j = 1; j <= nUniques; j++) sketch.update(key++, 1.0);
        final long stopUpdate1 = System.nanoTime();
        final long stopUpdate2 = System.nanoTime();
        updateTimeNanoSecs += (stopUpdate1 - startUpdate - (stopUpdate2 - stopUpdate1)) / (double) nUniques;

        final long startSerializeUpdatable = System.nanoTime();
        final byte[] bytesUpdatable = sketch.toByteArray();
        final long stopSerializeUpdatable1 = System.nanoTime();
        final long stopSerializeUpdatable2 = System.nanoTime();
        serializeUpdatableTimeNanoSecs += (stopSerializeUpdatable1 - startSerializeUpdatable - (stopSerializeUpdatable2 - stopSerializeUpdatable1));

        final long startDeserializeUpdatable = System.nanoTime();
        UpdateSketch deserializedUpdatableSketch = (UpdateSketch) com.yahoo.sketches.theta.Sketches.heapifySketch(Memory.wrap(bytesUpdatable));
        //UpdateSketch deserializedUpdatableSketch = (UpdateSketch) com.yahoo.sketches.theta.Sketches.wrapSketch(new NativeMemory(bytesUpdatable));
        //ArrayOfDoublesUpdatableSketch deserializedUpdatableSketch = (ArrayOfDoublesUpdatableSketch) ArrayOfDoublesSketches.heapifySketch(new NativeMemory(bytesUpdatable));
        //ArrayOfDoublesUpdatableSketch deserializedUpdatableSketch = (ArrayOfDoublesUpdatableSketch) ArrayOfDoublesSketches.wrapSketch(new NativeMemory(bytesUpdatable));
        //UpdatableSketch<Double, DoubleSummary> deserializedUpdatableSketch = Sketches.heapifyUpdatableSketch(new NativeMemory(bytesUpdatable));
        final long stopDeserializeUpdatable1 = System.nanoTime();
        final long stopDeserializeUpdatable2 = System.nanoTime();
        deserializeUpdatableTimeNanoSecs += (stopDeserializeUpdatable1 - startDeserializeUpdatable - (stopDeserializeUpdatable2 - stopDeserializeUpdatable1));
        deserializedUpdatableSketch.getEstimate();

        final long startCompacting = System.nanoTime();
        com.yahoo.sketches.theta.CompactSketch compactSketch = sketch.compact();
        //com.yahoo.sketches.theta.CompactSketch compactSketch = sketch.compact(true, mem2);
        //ArrayOfDoublesCompactSketch compactSketch = sketch.compact();
        //ArrayOfDoublesCompactSketch compactSketch = sketch.compact(mem2);
        //CompactSketch<DoubleSummary> compactSketch = sketch.compact();
        final long stopCompacting1 = System.nanoTime();
        final long stopCompacting2 = System.nanoTime();
        compactingTimeNanoSecs += (stopCompacting1 - startCompacting - (stopCompacting2 - stopCompacting1));

        final long startSerializeCompact = System.nanoTime();
        byte[] bytesCompact = compactSketch.toByteArray();
        final long stopSerializeCompact1 = System.nanoTime();
        final long stopSerializeCompact2 = System.nanoTime();
        serializeCompactTimeNanoSecs += (stopSerializeCompact1 - startSerializeCompact - (stopSerializeCompact2 - stopSerializeCompact1));

        final long startDeserializeCompact = System.nanoTime();
        com.yahoo.sketches.theta.Sketch deserializedCompactSketch = com.yahoo.sketches.theta.Sketches.heapifySketch(Memory.wrap(bytesCompact));
        //com.yahoo.sketches.theta.Sketch deserializedCompactSketch = com.yahoo.sketches.theta.Sketches.wrapSketch(new NativeMemory(bytesCompact));
        //ArrayOfDoublesSketch deserializedCompactSketch = ArrayOfDoublesSketches.heapifySketch(new NativeMemory(bytesCompact));
        //ArrayOfDoublesSketch deserializedCompactSketch = ArrayOfDoublesSketches.wrapSketch(new NativeMemory(bytesCompact));
        //Sketch<DoubleSummary> deserializedCompactSketch = Sketches.heapifySketch(new NativeMemory(bytesCompact));
        final long stopDeserializeCompact1 = System.nanoTime();
        final long stopDeserializeCompact2 = System.nanoTime();
        deserializeCompactTimeNanoSecs += (stopDeserializeCompact1 - startDeserializeCompact - (stopDeserializeCompact2 - stopDeserializeCompact1));
        deserializedCompactSketch.getEstimate();
      }
      System.out.println(
        nUniques + " " + numIterations
        + " " + String.format("%.1f", buildTimeNanoSecs / (double) numIterations)
        + " " + String.format("%.1f", updateTimeNanoSecs / (double) numIterations)
        + " " + String.format("%.1f", serializeUpdatableTimeNanoSecs / (double) numIterations)
        + " " + String.format("%.1f", deserializeUpdatableTimeNanoSecs / (double) numIterations)
        + " " + String.format("%.1f", compactingTimeNanoSecs / (double) numIterations)
        + " " + String.format("%.1f", serializeCompactTimeNanoSecs / (double) numIterations)
        + " " + String.format("%.1f", deserializeCompactTimeNanoSecs / (double) numIterations)
      );
      // calling garbage collection is super expensive
      // tests will take much longer, but, presumably, give cleaner results
      //System.gc();
    }
  }

  // measures union or intersection update time for different number of sketches
  // prints nanoseconds per one operation
  //@Test
  @SuppressWarnings("null")
  public void setOperations() {
    final int sketchSize = 16384;
    final int firstPowerOfTwo = 1;
    final int lastPowerOfTwo = 12;
    final int minIterations = 2;
    final int stepsPerPowerOfTwo = 8;
    final double step = 1.0 / stepsPerPowerOfTwo;
    long key = 1; // key to present to a sketch, which is unique across all trials
    final double[] value = { 1 };
    final WritableMemory mem = WritableMemory.wrap(new byte[100000000]);
    final WritableMemory mem2 = WritableMemory.wrap(new byte[100000000]);
    final WritableMemory mem3 = WritableMemory.wrap(new byte[100000000]);
    //ArrayOfDoublesSetOperationBuilder setOpBuilder = new ArrayOfDoublesSetOperationBuilder().setNominalEntries(sketchSize);
    //ArrayOfDoublesSetOperationBuilder setOpBuilder = new ArrayOfDoublesSetOperationBuilder().setNominalEntries(sketchSize).setMemory(mem);
    //com.yahoo.sketches.theta.SetOperationBuilder setOpBuilder = com.yahoo.sketches.theta.SetOperation.builder().setNominalEntries(sketchSize); 
    //com.yahoo.sketches.theta.SetOperationBuilder setOpBuilder = com.yahoo.sketches.theta.SetOperation.builder().setNominalEntries(sketchSize).initMemory(mem);

    //ArrayOfDoublesUpdatableSketchBuilder sketchBuilder = new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(sketchSize);
    //UpdateSketchBuilder sketchBuilder = new UpdateSketchBuilder().setNominalEntries(sketchSize);
    UpdatableSketchBuilder<Double, DoubleSummary> sketchBuilder = new UpdatableSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory()).setNominalEntries(sketchSize);
    for (double powerOfTwo = firstPowerOfTwo; powerOfTwo <= lastPowerOfTwo; powerOfTwo += step) {
      final int numOfSketches = (int) Math.pow(2, powerOfTwo);
      long buildTimeNanoSecs = 0;
      long setOpTimeNanoSecs = 0;
      long getResultTimeNanoSecs = 0;
      // this is to have exponentially more iterations at low numbers to improve signal to noise ratio
      final int numIterations = (int)(minIterations * Math.pow(2, lastPowerOfTwo - powerOfTwo));
      int setSize = 0;
      double setEstimate = 0;
      for (int i = 1; i <= numIterations; i++) {
        final long startBuilder = System.nanoTime();
        //ArrayOfDoublesUnion setOp = setOpBuilder.buildUnion();
        //ArrayOfDoublesIntersection setOp = setOpBuilder.buildIntersection();
        //ArrayOfDoublesAnotB setOp = setOpBuilder.buildAnotB();
        //com.yahoo.sketches.theta.Union setOp = setOpBuilder.buildUnion();
        //com.yahoo.sketches.theta.Intersection setOp = setOpBuilder.buildIntersection();
        //com.yahoo.sketches.theta.AnotB setOp = setOpBuilder.buildANotB();
        //Union<DoubleSummary> setOp = new Union<DoubleSummary>(sketchSize, new DoubleSummaryFactory());
        //Intersection<DoubleSummary> setOp = new Intersection<DoubleSummary>(new DoubleSummaryFactory());
        AnotB<DoubleSummary> setOp = new AnotB<DoubleSummary>();
        buildTimeNanoSecs += (System.nanoTime() - startBuilder);

        //ArrayOfDoublesSketch aNotBresult = null;
        //com.yahoo.sketches.theta.Sketch aNotBresult = null;
        Sketch<DoubleSummary> aNotBresult = null;
        for (int j = 0; j < numOfSketches; j++) {
          //ArrayOfDoublesUpdatableSketch us = sketchBuilder.build();
          //for (int k = 0; k < sketchSize; k++) us.update(key++, value);
          //UpdateSketch us = sketchBuilder.build();
          //for (int k = 0; k < sketchSize; k++) us.update(key++);
          UpdatableSketch<Double, DoubleSummary> us = sketchBuilder.build();
          for (int k = 0; k < sketchSize; k++) us.update(key++, 1.0);
          //key -= sketchSize / 2; // make half of the entries in the next sketch overlap with the current sketch
          //key -= sketchSize - 4; // shift slightly so that almost all entries overlap
          if (aNotBresult == null) {
            key -= 4; // make 2 entries overlap with previous sketch
          } else {
            key -= sketchSize + 4;
          }
          //ArrayOfDoublesSketch s = us.compact();
          //ArrayOfDoublesSketch s = us.compact(mem2);
          //com.yahoo.sketches.theta.Sketch s = us.compact();
          //com.yahoo.sketches.theta.Sketch s = us.compact(true, mem2);
          CompactSketch<DoubleSummary> s = us.compact();
          long startUpdate = System.nanoTime();
          //setOp.update(s);
          //setOp.update(s, combiner);
          if (aNotBresult == null) {
            aNotBresult = s;
          } else {
            setOp.update(aNotBresult, s);
            aNotBresult = setOp.getResult();
          }
          setOpTimeNanoSecs += System.nanoTime() - startUpdate;
        }
        final long startGetResult = System.nanoTime();
        //ArrayOfDoublesSketch result = setOp.getResult();
        //ArrayOfDoublesSketch result = setOp.getResult(mem3);
        //com.yahoo.sketches.theta.Sketch result = setOp.getResult();
        //com.yahoo.sketches.theta.Sketch result = setOp.getResult(true, mem3);
        //Sketch<DoubleSummary> result = setOp.getResult();
        //setSize = result.getRetainedEntries();
        //setSize = result.getRetainedEntries(true);
        //setEstimate = result.getEstimate();
        setSize = aNotBresult.getRetainedEntries();
        //setSize = aNotBresult.getRetainedEntries(true);
        setEstimate = aNotBresult.getEstimate();
        getResultTimeNanoSecs += System.nanoTime() - startGetResult;
      }
      System.out.println(numOfSketches + " " + numIterations + " " + setSize + String.format(" %.1f", setEstimate)
        + String.format(" %.1f", buildTimeNanoSecs / (double) numIterations)
        + String.format(" %.1f", setOpTimeNanoSecs / (double) numIterations / numOfSketches)
        + String.format(" %.1f", getResultTimeNanoSecs / (double) numIterations)
      );
    }
  }

  static final int iterations = 4000;
  static final int size = 65536;

  //@Test
  public void onHeapSelect() {
    final Random random = new Random(0);
    long[][] arrays = new long[iterations][size];
    for (int i = 0; i < iterations; i++) {
      for (int j = 0; j < size; j++) arrays[i][j] = random.nextLong();
    }
    final long startTimeNs = System.nanoTime();
    for (int i = 0; i < iterations; i++) {
      QuickSelect.select(arrays[i], 0, size - 1, size - 1);
    }
    final long stopTimeNs = System.nanoTime();
    System.out.println("On-heap quick select ns: " + String.format(" %.1f", (stopTimeNs - startTimeNs) / (double) iterations));
  }

//  //@Test
//  public void memOnHeapSelect() {
//    final Random random = new Random(0);
//    final NativeMemory[] mems = new NativeMemory[iterations];
//    for (int i = 0; i < iterations; i++) {
//      byte[] bytes = new byte[size * Long.BYTES];
//      final NativeMemory mem = new NativeMemory(bytes);
//      for (int j = 0; j < size; j++) mem.putLong(j * Long.BYTES, random.nextLong());
//      mems[i] = mem;
//    }
//    final long startTimeNs = System.nanoTime();
//    for (int i = 0; i < iterations; i++) {
//      QuickSelect2.select(mems[i], 0, size - 1, size - 1);
//    }
//    final long stopTimeNs = System.nanoTime();
//    System.out.println("Mem on-heap quick select ns: " + String.format(" %.1f", (stopTimeNs - startTimeNs) / (double) iterations));
//  }
//
//  //@Test
//  public void memOnHeapSingleClassSelect() {
//    final Random random = new Random(0);
//    final SingleClassNativeMemory[] mems = new SingleClassNativeMemory[iterations];
//    for (int i = 0; i < iterations; i++) {
//      byte[] bytes = new byte[size * Long.BYTES];
//      final SingleClassNativeMemory mem = new SingleClassNativeMemory(bytes);
//      for (int j = 0; j < size; j++) mem.putLong(j * Long.BYTES, random.nextLong());
//      mems[i] = mem;
//    }
//    final long startTimeNs = System.nanoTime();
//    for (int i = 0; i < iterations; i++) {
//      QuickSelect2.select(mems[i], 0, size - 1, size - 1);
//    }
//    final long stopTimeNs = System.nanoTime();
//    System.out.println("Single class Mem on-heap quick select ns: " + String.format(" %.1f", (stopTimeNs - startTimeNs) / (double) iterations));
//  }
//
//  //@Test
//  public void offHeapSelect() {
//    final Random random = new Random(0);
//    final NativeMemory[] mems = new NativeMemory[iterations];
//    for (int i = 0; i < iterations; i++) {
//      //byte[] bytes = new byte[size * Long.BYTES];
//      //final Memory mem = new NativeMemory(bytes);
//      final NativeMemory mem = new AllocMemory(size * Long.BYTES);
//      for (int j = 0; j < size; j++) mem.putLong(j * Long.BYTES, random.nextLong());
//      mems[i] = mem;
//    }
//    final long startTimeNs = System.nanoTime();
//    for (int i = 0; i < iterations; i++) {
//      QuickSelect2.select(mems[i], 0, size - 1, size - 1);
//    }
//    final long stopTimeNs = System.nanoTime();
//    System.out.println("Off-heap quick select ns: " + String.format(" %.1f", (stopTimeNs - startTimeNs) / (double) iterations));
//    for (int i = 0; i < iterations; i++) mems[i].freeMemory();
//  }

  private static ArrayOfDoublesCombiner combiner = new ArrayOfDoublesCombiner() {
    @Override
    public double[] combine(final double[] a, final double[] b) {
      final double[] result = new double[a.length];
      for (int i = 0; i < a.length; i++) result[i] = a[i] + b[i];
      return result;
    }
  };

  private static void warmUp() {
    for (int i = 0; i < 5000; i++) {
      ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().build();
      for (int j = 0; j < 1000000; j++) sketch.update(j, new double[] {1});
    }
  }

//  public static void main(String[] args) {
//    PerformanceTest test = new PerformanceTest();
//    test.onHeapSelect();
//    test.memOnHeapSelect();
//    test.memOnHeapSingleClassSelect();
//    test.offHeapSelect();
//  }

}
