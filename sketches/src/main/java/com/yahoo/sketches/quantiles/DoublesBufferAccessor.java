package com.yahoo.sketches.quantiles;

import java.util.AbstractList;

/**
 * @author Jon Malkin
 */
abstract class DoublesBufferAccessor extends AbstractList<Double> {
  public abstract double[] getArray(int fromIdx, int numItems);

  public abstract void putArray(double[] srcArray, int srcIndex,
                                int dstIndex, int numItems);


  // These next 3 come from AbstractList, just included here to be explicit
  public abstract Double get(final int index);

  public abstract Double set(final int index, final Double value);

  public abstract int size();
}
