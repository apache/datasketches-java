package com.yahoo.sketches.benchmark;

import java.util.List;

/**
 */
public interface SketchBenchmark
{
  public void setup(int numSketches, List<Spec> specs);
  public void runNTimes(int n);
  public void reset();

  public class Spec {
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
