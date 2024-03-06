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
   * Shuffle the given input array using a default source of randomness.
   * @param array the array to be shuffled.
   */
  public static void shuffle(final float[] array) {
    shuffle(array, rand);
  }

  /**
   * Shuffle the given input array using the given source of randomness.
   * @param array the array to be shuffled.
   * @param rand the source of randomness used to shuffle the list.
   */
  public static void shuffle(final float[] array, final Random rand) {
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
   * Shuffle the given input array using a default source of randomness.
   * @param array the array to be shuffled.
   */
  public static void shuffle(final double[] array) {
    shuffle(array, rand);
  }

  /**
   * Shuffle the given input array using the given source of randomness.
   * @param array the array to be shuffled.
   * @param rand the source of randomness used to shuffle the list.
   */
  public static void shuffle(final double[] array, final Random rand) {
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
   * Shuffle the given input array using a default source of randomness.
   * @param array the array to be shuffled.
   */
  public static void shuffle(final long[] array) {
    shuffle(array, rand);
  }

  /**
   * Shuffle the given input array using the given source of randomness.
   * @param array the array to be shuffled.
   * @param rand the source of randomness used to shuffle the list.
   */
  public static void shuffle(final long[] array, final Random rand) {
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
   * Shuffle the given input array using a default source of randomness.
   * @param array the array to be shuffled.
   */
  public static void shuffle(final int[] array) {
    shuffle(array, rand);
  }

  /**
   * Shuffle the given input array using the given source of randomness.
   * @param array the array to be shuffled.
   * @param rand the source of randomness used to shuffle the list.
   */
  public static void shuffle(final int[] array, final Random rand) {
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
   * Shuffle the given input array using a default source of randomness.
   * @param array the array to be shuffled.
   * @param <T> the component type of the given array.
   */
  public static <T> void shuffle(final T[] array) {
    shuffle(array, rand);
  }

  /**
   * Shuffle the given input array using the given source of randomness.
   * @param array the array to be shuffled.
   * @param rand the source of randomness used to shuffle the list.
   * @param <T> the component type of the given array.
   */
  public static <T> void shuffle(final T[] array, final Random rand) {
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
