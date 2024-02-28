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

package org.apache.datasketches.common;

import java.util.Random;

/**
 * @author Lee Rhodes
 */
public final class Shuffle {
  private static final Random rand = new Random();

  /**
   * Shuffle the given input float array
   * @param array input array
   */
  public static void shuffle(final float[] array) {
    final int arrLen = array.length;
    for (int i = 0; i < arrLen; i++) {
      final int r = rand.nextInt(i + 1);
      swap(array, i, r);
    }
  }

  private static void swap(final float[] array, final int i1, final int i2) {
    final float value = array[i1];
    array[i1] = array[i2];
    array[i2] = value;
  }

  /**
   * Shuffle the given input double array
   * @param array input array
   */
  public static void shuffle(final double[] array) {
    final int arrLen = array.length;
    for (int i = 0; i < arrLen; i++) {
      final int r = rand.nextInt(i + 1);
      swap(array, i, r);
    }
  }

  private static void swap(final double[] array, final int i1, final int i2) {
    final double value = array[i1];
    array[i1] = array[i2];
    array[i2] = value;
  }

  /**
   * Shuffle the given input long array
   * @param array input array
   */
  public static void shuffle(final long[] array) {
    final int arrLen = array.length;
    for (int i = 0; i < arrLen; i++) {
      final int r = rand.nextInt(i + 1);
      swap(array, i, r);
    }
  }

  private static void swap(final long[] array, final int i1, final int i2) {
    final long value = array[i1];
    array[i1] = array[i2];
    array[i2] = value;
  }

  /**
   * Shuffle the given input int array
   * @param array input array
   */
  public static void shuffle(final int[] array) {
    final int arrLen = array.length;
    for (int i = 0; i < arrLen; i++) {
      final int r = rand.nextInt(i + 1);
      swap(array, i, r);
    }
  }

  private static void swap(final int[] array, final int i1, final int i2) {
    final int value = array[i1];
    array[i1] = array[i2];
    array[i2] = value;
  }

  /**
   * Shuffle the given input array of type T
   * @param array input array
   */
  public static <T> void shuffle(final T[] array) {
    final int arrLen = array.length;
    for (int i = 0; i < arrLen; i++) {
      final int r = rand.nextInt(i + 1);
      swap(array, i, r);
    }
  }

  private static<T> void swap(final T[] array, final int i1, final int i2) {
    final T value = array[i1];
    array[i1] = array[i2];
    array[i2] = value;
  }

}
