package com.yahoo.sketches.benchmark;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Union;
import com.yahoo.sketches.theta.UpdateSketch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 */
public class ThetaMemoryBenchmark implements SketchBenchmark
{
  private final int nominalEntries;
  private final Random rand;
  private final byte[] bytes;

  private List<Memory> memories;

  public ThetaMemoryBenchmark(int lgK) {
    this.nominalEntries = 1 << lgK;
    this.rand = new Random(lgK);
    this.bytes = new byte[Sketch.getMaxUpdateSketchBytes(nominalEntries) + 8];
  }

  @Override
  public void setup(int numSketches, List<Spec> specs)
  {
    memories = new ArrayList<>(numSketches);

    for (Spec spec : specs) {
      for (int i = 0; i < spec.getNumSketches(); ++i) {
        UpdateSketch sketch = UpdateSketch.builder().build(nominalEntries);
        for (int j = 0; j < spec.getNumEntries(); ++j) {
          sketch.update(rand.nextLong());
        }
        memories.add(new NativeMemory(sketch.rebuild().compact(true, null).toByteArray()));
      }
    }
    Collections.shuffle(memories, rand);

    int numRetained = 0;
    int numEstimating = 0;
    for (Memory mem : memories) {
      Sketch sketch = Sketch.wrap(mem);
      numRetained += sketch.getRetainedEntries(true);
      if (sketch.isEstimationMode()) {
        ++numEstimating;
      }
    }
    System.out.printf(
        "%,d entries, %,d/sketch, %,d estimating (%.2f%%)%n",
        numRetained, numRetained / memories.size(), numEstimating, (100 * numEstimating) / (double) memories.size()
    );
  }

  @Override
  public void runNTimes(int n)
  {
    for (int i = 0; i < n; ++i) {
      Union combined = (Union) SetOperation
          .builder()
          .setMemory(new NativeMemory(bytes))
          .build(nominalEntries, Family.UNION);
      for (Memory toUnion : memories) {
        combined.update(toUnion);
      }
    }
  }

  @Override
  public void reset()
  {
    memories = null;
  }

  @Override
  public String toString()
  {
    return String.format("Theta Memory Benchmark(nominalEntries=%s)", nominalEntries);
  }
}
