/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

/**
 * @author Kevin Lang
 */
final class HarmonicNumbers {
  private static final int NUM_EXACT_HARMONIC_NUMBERS = 25;
  private static final double EULER_MASCHERONI_CONSTANT = 0.577215664901532860606512090082;
  private static double[] tableOfExactHarmonicNumbers = {
    0.0, // 0
    1.0, // 1
    1.5, // 2
    11.0 / 6.0, // 3
    25.0 / 12.0, // 4
    137.0 / 60.0, // 5
    49.0 / 20.0, // 6
    363.0 / 140.0, // 7
    761.0 / 280.0, // 8
    7129.0 / 2520.0, // 9
    7381.0 / 2520.0, // 10
    83711.0 / 27720.0, // 11
    86021.0 / 27720.0, // 12
    1145993.0 / 360360.0, // 13
    1171733.0 / 360360.0, // 14
    1195757.0 / 360360.0, // 15
    2436559.0 / 720720.0, // 16
    42142223.0 / 12252240.0, // 17
    14274301.0 / 4084080.0, // 18
    275295799.0 / 77597520.0, // 19
    55835135.0 / 15519504.0, // 20
    18858053.0 / 5173168.0, // 21
    19093197.0 / 5173168.0, // 22
    444316699.0 / 118982864.0, // 23
    1347822955.0 / 356948592.0 // 24
  };

  private HarmonicNumbers() {}

  @SuppressWarnings("cast")
  public static double harmonicNumber(final long x_i) {
    if (x_i < NUM_EXACT_HARMONIC_NUMBERS) {
      return tableOfExactHarmonicNumbers[(int) x_i];
    } else {
      final double x = (double) x_i;
      final double invSq = 1.0 / (x * x);
      double sum = Math.log(x) + EULER_MASCHERONI_CONSTANT + (1.0 / (2.0 * x));
      /* note: the number of terms included from this series expansion is appropriate
         for the size of the exact table (25) and the precision of doubles */
      double pow = invSq; /* now n^-2 */
      sum -= pow * (1.0 / 12.0);
      pow *= invSq; /* now n^-4 */
      sum += pow * (1.0 / 120.0);
      pow *= invSq; /* now n^-6 */
      sum -= pow * (1.0 / 252.0);
      pow *= invSq; /* now n^-8 */
      sum += pow * (1.0 / 240.0);
      return sum;
    }
  }
}
