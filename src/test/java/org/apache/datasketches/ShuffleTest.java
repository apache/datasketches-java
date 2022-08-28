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

package org.apache.datasketches;

import static org.testng.Assert.fail;

import org.testng.annotations.Test;

public class ShuffleTest {

  @Test
  public void checkFloat() {
    float[] array = new float[10];
    for (int i = 0; i < array.length; i++) { array[i] = i; }
    Shuffle.shuffle(array);
    int neCount = 0;
    for (int i = 0; i < array.length; i++) {
      if (array[i] != i) { neCount++; }
    }
    //System.out.println(neCount);
    if (neCount == 0) { fail(); }
  }

  @Test
  public void checkDouble() {
    double[] array = new double[10];
    for (int i = 0; i < array.length; i++) { array[i] = i; }
    Shuffle.shuffle(array);
    int neCount = 0;
    for (int i = 0; i < array.length; i++) {
      if (array[i] != i) { neCount++; }
    }
    //System.out.println(neCount);
    if (neCount == 0) { fail(); }
  }

  @Test
  public void checkLong() {
    long[] array = new long[10];
    for (int i = 0; i < array.length; i++) { array[i] = i; }
    Shuffle.shuffle(array);
    int neCount = 0;
    for (int i = 0; i < array.length; i++) {
      if (array[i] != i) { neCount++; }
    }
    //System.out.println(neCount);
    if (neCount == 0) { fail(); }
  }

  @Test
  public void checkInt() {
    int[] array = new int[10];
    for (int i = 0; i < array.length; i++) { array[i] = i; }
    Shuffle.shuffle(array);
    int neCount = 0;
    for (int i = 0; i < array.length; i++) {
      if (array[i] != i) { neCount++; }
    }
    //System.out.println(neCount);
    if (neCount == 0) { fail(); }
  }
}

