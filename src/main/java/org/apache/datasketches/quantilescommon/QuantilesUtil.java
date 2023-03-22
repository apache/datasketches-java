/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.quantilescommon;

import static java.lang.Math.log;
import static java.lang.Math.pow;

import java.util.Objects;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;

/**
 * Utilities for the quantiles sketches.
 *
 * @author Lee Rhodes
 */
public final class QuantilesUtil {

  private QuantilesUtil() {}

  public static final String THROWS_EMPTY = "The sketch must not be empty at this point.";

  /**
   * Checks that the given normalized rank: <i>0 &le; nRank &le; 1.0</i>.
   * @param nRank the given normalized rank.
   */
  public static final void checkNormalizedRankBounds(final double nRank) {
    if ((nRank < 0.0) || (nRank > 1.0)) {
      throw new SketchesArgumentException(
        "A normalized rank must be >= 0 and <= 1.0: " + nRank);
    }
  }

  /**
   * Checks the sequential validity of the given array of double values.
   * They must be unique, monotonically increasing and not NaN.
   * @param values the given array of double values
   */
  public static final void checkDoublesSplitPointsOrder(final double[] values) {
    Objects.requireNonNull(values);
    final int len = values.length - 1;
    for (int j = 0; j < len; j++) {
      if (values[j] < values[j + 1]) { continue; }
      throw new SketchesArgumentException(
          "Values must be unique, monotonically increasing and not NaN.");
    }
  }

  /**
   * Checks the sequential validity of the given array of float values.
   * They must be unique, monotonically increasing and not NaN.
   * @param values the given array of double values
   */
  public static final void checkFloatsSplitPointsOrder(final float[] values) {
    Objects.requireNonNull(values);
    final int len = values.length - 1;
    for (int j = 0; j < len; j++) {
      if (values[j] < values[j + 1]) { continue; }
      throw new SketchesArgumentException(
          "Values must be unique, monotonically increasing and not NaN.");
    }
  }

  /**
   * Returns a double array of evenly spaced values between 0.0, exclusive, and 1.0, inclusive.
   * If num == 1, 1.0 will be in index 0 of the returned array.
   * @param num the total number of values excluding 0 and including 1.0. Must be 1 or greater.
   * @return a double array of evenly spaced values between 0.0, exclusive, and 1.0, inclusive.
   * @throws IllegalArgumentException if <i>num</i> is less than 1.
   */
  public static double[] evenlySpacedRanks(final int num) {
    if (num < 1) { throw new IllegalArgumentException("num must be >= 1"); }
    final double[] out = new double[num];
    if (num == 1) { return new double[] { 1.0 }; }
    out[num - 1] = 1.0;

    final double delta = 1.0 / num;
    for (int i = 0; i < num - 1; i++) { out[i] = (i + 1) * delta; }
    return out;
  }

  /**
   * Returns a float array of evenly spaced values between value1 and value2 inclusive.
   * If value2 &gt; value1, the resulting sequence will be increasing.
   * If value2 &lt; value1, the resulting sequence will be decreasing.
   * @param value1 will be in index 0 of the returned array
   * @param value2 will be in the highest index of the returned array
   * @param num the total number of values including value1 and value2. Must be 2 or greater.
   * @return a float array of evenly spaced values between value1 and value2 inclusive.
   */
  public static float[] evenlySpacedFloats(final float value1, final float value2, final int num) {
    if (num < 2) {
      throw new SketchesArgumentException("num must be >= 2");
    }
    final float[] out = new float[num];
    out[0] = value1;
    out[num - 1] = value2;
    if (num == 2) { return out; }

    final float delta = (value2 - value1) / (num - 1);

    for (int i = 1; i < num - 1; i++) { out[i] = i * delta + value1; }
    return out;
  }

  /**
   * Returns a double array of evenly spaced values between value1, inclusive, and value2 inclusive.
   * If value2 &gt; value1, the resulting sequence will be increasing.
   * If value2 &lt; value1, the resulting sequence will be decreasing.
   * @param value1 will be in index 0 of the returned array
   * @param value2 will be in the highest index of the returned array
   * @param num the total number of values including value1 and value2. Must be 2 or greater.
   * @return a float array of evenly spaced values between value1 and value2 inclusive.
   */
  public static double[] evenlySpacedDoubles(final double value1, final double value2, final int num) {
    if (num < 2) {
      throw new SketchesArgumentException("num must be >= 2");
    }
    final double[] out = new double[num];
    out[0] = value1;
    out[num - 1] = value2;
    if (num == 2) { return out; }

    final double delta = (value2 - value1) / (num - 1);

    for (int i = 1; i < num - 1; i++) { out[i] = i * delta + value1; }
    return out;
  }
  
  /**
   * Returns a double array of values between min and max inclusive where the log of the
   * returned values are evenly spaced.
   * If value2 &gt; value1, the resulting sequence will be increasing.
   * If value2 &lt; value1, the resulting sequence will be decreasing.
   * @param value1 will be in index 0 of the returned array, and must be greater than zero.
   * @param value2 will be in the highest index of the returned array, and must be greater than zero.
   * @param num the total number of values including value1 and value2. Must be 2 or greater
   * @return a double array of exponentially spaced values between value1 and value2 inclusive.
   */
  public static double[] evenlyLogSpaced(final double value1, final double value2, final int num) {
    if (num < 2) {
      throw new SketchesArgumentException("num must be >= 2");
    }
    if (value1 <= 0 || value2 <= 0) {
      throw new SketchesArgumentException("value1 and value2 must be > 0.");
    }

    final double[] arr = evenlySpacedDoubles(log(value1) / Util.LOG2, log(value2) / Util.LOG2, num);
    for (int i = 0; i < arr.length; i++) { arr[i] = pow(2.0,arr[i]); }
    return arr;
  }

}

