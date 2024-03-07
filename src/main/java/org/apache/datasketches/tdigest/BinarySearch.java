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

package org.apache.datasketches.tdigest;

/**
 * Algorithms with logarithmic complexity for searching in an array.
 */
public final class BinarySearch {

  /**
   * Returns an index to the first element in the range [first, last) such that
   * element < value is false (i.e. that is greater than or equal to value),
   * or last if no such element is found.
   * The range [first, last) must be partitioned with respect to the expression element < value,
   * i.e., all elements for which the expression is true must precede all elements
   * for which the expression is false.
   * A fully-sorted range meets this criterion.
   * The number of comparisons performed is logarithmic in the distance between first and last.
   *
   * @param values array of values
   * @param first index to the first element in the range
   * @param last index to the element past the end of the range
   * @param value to look for
   * @return index to the element found or last if not found
   */
  static int lowerBound(final double[] values, int first, final int last, final double value) {
    int current;
    int step;
    int count = last - first; 
    while (count > 0) {
      step = count / 2;
      current = first + step;
      if (values[current] < value) {
        first = ++current;
        count -= step + 1;
      } else {
        count = step;
      }
    }
    return first;
  }

  /**
   * Returns an index to the first element in the range [first, last) such that
   * value < element is true (i.e. that is strictly greater than value),
   * or last if no such element is found.
   * The range [first, last) must be partitioned with respect to the expression !(value < element),
   * i.e., all elements for which the expression is true must precede all elements
   * for which the expression is false.
   * A fully-sorted range meets this criterion.
   * The number of comparisons performed is logarithmic in the distance between first and last.
   *
   * @param values array of values
   * @param first index to the first element in the range
   * @param last index to the element past the end of the range
   * @param value to look for
   * @return index to the element found or last if not found
   */
  static int upperBound(final double[] values, int first, final int last, final double value) {
    int current;
    int step;
    int count = last - first; 
    while (count > 0) {
      step = count / 2; 
      current = first + step;
      if (!(value < values[current])) {
        first = ++current;
        count -= step + 1;
      } else {
        count = step;
      }
    }
    return first;
  }

}
