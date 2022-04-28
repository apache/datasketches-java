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

import static org.testng.Assert.assertTrue;

import org.apache.datasketches.req.ReqAuxiliary.Row;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */

public class ReqAuxiliaryTest {

  @Test
  public void checkMergeSortIn() {
    checkMergeSortInImpl(true);
    checkMergeSortInImpl(false);
  }

  private static void checkMergeSortInImpl(final boolean hra) {
    final FloatBuffer buf1 = new FloatBuffer(25, 0, hra);
    for (int i = 1; i < 12; i += 2) { buf1.append(i); } //6 items
    final FloatBuffer buf2 = new FloatBuffer(25, 0, hra);
    for (int i = 2; i <= 12; i += 2) { buf2.append(i); } //6 items
    final long N = 12;

    final float[] items = new float[25];
    final long[] weights = new long[25];

    final ReqAuxiliary aux = new ReqAuxiliary(items, weights, hra, N);
    aux.mergeSortIn(buf1, 1, 0);
    aux.mergeSortIn(buf2, 2, 6);
    println(aux.toString(3, 12));
    Row row = aux.getRow(0);
    for (int i = 1; i < 12; i++) {
      final Row rowi = aux.getRow(i);
      assertTrue(rowi.item >= row.item);
      row = rowi;
    }
  }

  /**
   * output
   * @param o object
   */
  static final void println(final Object o) {
    //System.out.println(o.toString());
  }
}
