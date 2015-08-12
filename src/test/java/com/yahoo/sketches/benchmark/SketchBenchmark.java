package com.yahoo.sketches.benchmark;

import java.util.List;

/**
 */
public interface SketchBenchmark
{
  void setup(int numSketches, List<Spec> specs);
  void runNTimes(int n);
  void reset();

  class Spec {
    private final int numSketches;
    private final long numEntries;

    public Spec(long numEntries, int numSketches) {

      this.numSketches = numSketches;
      this.numEntries = numEntries;
    }

    public int getNumSketches()
    {
      return numSketches;
    }

    public long getNumEntries()
    {
      return numEntries;
    }
  }
}
