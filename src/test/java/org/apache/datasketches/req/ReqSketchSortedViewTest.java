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

import org.apache.datasketches.req.ReqSketchSortedView.Row;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */

public class ReqSketchSortedViewTest {

  /**
   * just tests the mergeSortIn. It does NOT test anything else.
   */
  @Test
  public void checkMergeSortIn() {
    checkMergeSortInImpl(true);
    checkMergeSortInImpl(false);
  }

  private static void checkMergeSortInImpl(final boolean hra) {
    final FloatBuffer buf1 = new FloatBuffer(25, 0, hra);
    for (int i = 1; i < 12; i += 2) { buf1.append(i); } //6 odd values
    final FloatBuffer buf2 = new FloatBuffer(25, 0, hra);
    for (int i = 2; i <= 12; i += 2) { buf2.append(i); } //6 even values
    final long N = 18;

    final float[] values = new float[25];
    final long[] valueWeights = new long[25]; //not used

    final ReqSketchSortedView rssv = new ReqSketchSortedView(values, valueWeights, hra, N);
    rssv.mergeSortIn(buf1, 1, 0);
    rssv.mergeSortIn(buf2, 2, 6); //at weight of 2
    println(rssv.toString(3, 12));
    Row row = rssv.getRow(0);
    for (int i = 1; i < 12; i++) {
      final Row rowi = rssv.getRow(i);
      assertTrue(rowi.value >= row.value);
      row = rowi;
    }
  }

  @Test
  public void checkIterator() {
    int k = 4;
    boolean hra = false;
    int numV = 3;
    int dup = 2;
    println("");
    println("CHECK ReqSketchSortedViewIterator");
    println("  k: " + k + ", hra: " + hra);
    ReqSketchBuilder bldr = ReqSketch.builder();
    ReqSketch sketch = bldr.build();
    int n = numV * dup; //Total values including duplicates
    println("  numV: " + numV + ", dup: " + dup);

    float[] arr = new float[n];
    int h = 0;
    for (int i = 0; i < numV; i++) {
      float flt = (i + 1) * 10;
      for (int j = 1; j <= dup; j++) { arr[h++] = flt; }
    }
    for (int i = 0; i < n; i++) { sketch.update(arr[i]); }

    ReqSketchSortedViewIterator itr = sketch.getSortedView().iterator();
    println("");
    String[] header = {"Value", "Wt", "CumWtNotInc", "nRankNotInc", "CumWtInc", "nRankInc"};
    String hfmt = "%12s%12s%12s%12s%12s%12s\n";
    String fmt = "%12.1f%12d%12d%12.3f%12d%12.3f\n";
    printf(hfmt, (Object[]) header);
    while (itr.next()) {
      float v = itr.getValue();
      long wt = itr.getWeight();
      long cumWtNotInc   = itr.getCumulativeWeight(false);
      double nRankNotInc = itr.getNormalizedRank(false);
      long cumWtInc      = itr.getCumulativeWeight(true);
      double nRankInc    = itr.getNormalizedRank(true);
      printf(fmt, v, wt, cumWtNotInc, nRankNotInc, cumWtInc, nRankInc);
    }
  }

  private final static boolean enablePrinting = true;

  /**
   * @param format the format
   * @param args the args
   */
  static final void printf(final String format, final Object ...args) {
    if (enablePrinting) { System.out.printf(format, args); }
  }

  /**
   * @param o the Object to println
   */
  static final void println(final Object o) {
    if (enablePrinting) { System.out.println(o.toString()); }
  }

}
