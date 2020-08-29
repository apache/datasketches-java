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

package org.apache.datasketches.req;

import static org.apache.datasketches.req.ReqAuxiliary.binarySearch;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;


/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class ReqAuxiliaryTest {

  @Test
  public void checkBinSearch() {
    // index       {0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12};
    // range                 |                       |
    float[] arr1 = {1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 6};
    assertEquals(binarySearch(arr1, 0, 12, 0.5f), -1);
    assertEquals(binarySearch(arr1, 0, 12, 1.5f),  1);
    assertEquals(binarySearch(arr1, 0, 12, 2.5f),  3);
    assertEquals(binarySearch(arr1, 0, 12, 3.5f),  5);
    assertEquals(binarySearch(arr1, 0, 12, 4.5f),  7);
    assertEquals(binarySearch(arr1, 0, 12, 5.5f),  9);
    assertEquals(binarySearch(arr1, 0, 12, 6.5f), 12);
    assertEquals(binarySearch(arr1, 0, 12,   1f), -1);
    assertEquals(binarySearch(arr1, 0, 12,   2f),  1);
    assertEquals(binarySearch(arr1, 0, 12,   3f),  3);
    assertEquals(binarySearch(arr1, 0, 12,   4f),  5);
    assertEquals(binarySearch(arr1, 0, 12,   5f),  7);
    assertEquals(binarySearch(arr1, 0, 12,   6f),  9);
    assertEquals(binarySearch(arr1, 0, 12,   7f), 12);

    //Check internal range:
    assertEquals(binarySearch(arr1, 3, 11, 0.5f), -1);
    assertEquals(binarySearch(arr1, 3, 11, 1.5f), -1);
    assertEquals(binarySearch(arr1, 3, 11, 2.5f),  3);
    assertEquals(binarySearch(arr1, 3, 11, 3.5f),  5);
    assertEquals(binarySearch(arr1, 3, 11, 4.5f),  7);
    assertEquals(binarySearch(arr1, 3, 11, 5.5f),  9);
    assertEquals(binarySearch(arr1, 3, 11, 6.5f), 11);
    assertEquals(binarySearch(arr1, 3, 11,   1f), -1);
    assertEquals(binarySearch(arr1, 3, 11,   2f), -1);
    assertEquals(binarySearch(arr1, 3, 11,   3f),  3);
    assertEquals(binarySearch(arr1, 3, 11,   4f),  5);
    assertEquals(binarySearch(arr1, 3, 11,   5f),  7);
    assertEquals(binarySearch(arr1, 3, 11,   6f),  9);
    assertEquals(binarySearch(arr1, 3, 11,   7f), 11);
  }

  @Test
  public void checkBinSearch2() {
    // index       { 0,   1,   2,   3,   4,   5,   6};
    // range         |                        |
    float[] arr1 = {178, 182, 186, 190, 194, 197, 200};
    assertEquals(binarySearch(arr1, 0, 6, 170f), -1);
    assertEquals(binarySearch(arr1, 0, 5, 170f), -1);
    assertEquals(binarySearch(arr1, 0, 6, 180f), 0);
    assertEquals(binarySearch(arr1, 0, 5, 180f), 0);
  }


  static final void printf(final String format, final Object ...args) {
    System.out.printf(format, args);
  }

  static final void print(final Object o) { System.out.print(o.toString()); }

  static final void println(final Object o) { System.out.println(o.toString()); }
}
