package com.yahoo.sketches.benchmark;

import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.OnHeapFields;
import com.yahoo.sketches.hll.OnHeapHashFields;
import com.yahoo.sketches.hll.Preamble;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 */
public class HllSketchBenchmark implements SketchBenchmark
{
  private final Random rand;
  private final Preamble preamble;

  private List<HllSketch> sketches;

  public HllSketchBenchmark(int logK)
  {
    this.rand = new Random(logK);
    this.preamble = Preamble.createSharedPreamble(logK);
  }

  @Override
  public void setup(int numSketches, List<Spec> specs)
  {
    sketches = new ArrayList<>(numSketches);

    for (Spec spec : specs) {
      for (int i = 0; i < spec.getNumSketches(); ++i) {
        HllSketch sketch = HllSketch.builder().setPreamble(preamble).build();
        for (int j = 0; j < spec.getNumEntries(); ++j) {
          sketch.update(new long[]{rand.nextLong()});
        }
        sketches.add(sketch.asCompact());
      }
    }
    Collections.shuffle(sketches);
  }

  @Override
  public void runNTimes(int n)
  {
    for (int i = 0; i < n; ++i) {
      HllSketch combined = new HllSketch(new OnHeapFields(preamble));
      for (HllSketch toUnion : sketches) {
        combined.union(toUnion);
      }
    }
  }

  @Override
  public void reset()
  {
    sketches = null;
  }

  @Override
  public String toString()
  {
    return "HLL Benchmark";
  }
}
