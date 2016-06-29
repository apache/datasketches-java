/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

/**
 * Combines two arrays of double values for use with ArrayOfDoubles tuple sketches
 */
public interface ArrayOfDoublesCombiner {

  /**
   * Method of combining two arrays of double values
   * @param a Array A.
   * @param b Array B.
   * @return Result of combining A and B
   */
  public double[] combine(double[] a, double[] b);

}
