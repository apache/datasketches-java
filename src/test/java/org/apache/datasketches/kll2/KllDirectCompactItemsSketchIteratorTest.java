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

package org.apache.datasketches.kll2;

import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.EXCLUSIVE;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Comparator;

import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.quantilescommon.GenericSortedViewIterator;
import org.apache.datasketches.quantilescommon.ItemsSketchSortedView;
import org.apache.datasketches.quantilescommon.QuantilesGenericSketchIterator;
import org.testng.Assert;
import org.testng.annotations.Test;

@SuppressWarnings("unused")
public class KllDirectCompactItemsSketchIteratorTest {
  private ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();

  @Test
  public void emptySketch() {
    KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
    byte[] byteArr = sk.toByteArray();
    KllItemsSketch<String> sk2 = KllItemsSketch.wrap(Memory.wrap(byteArr), Comparator.naturalOrder(), serDe);
    assertTrue(sk2 instanceof KllDirectCompactItemsSketch);
    QuantilesGenericSketchIterator<String> itr = sk2.iterator();
    assertFalse(itr.next());
  }

  @Test
  public void oneItemSketch() {
    KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
    sk.update("1");
    byte[] byteArr = sk.toByteArray();
    KllItemsSketch<String> sk2 = KllItemsSketch.wrap(Memory.wrap(byteArr), Comparator.naturalOrder(), serDe);
    assertTrue(sk2 instanceof KllDirectCompactItemsSketch);
    QuantilesGenericSketchIterator<String> itr = sk2.iterator();
    assertTrue(itr.next());
    assertEquals(itr.getQuantile(), "1");
    assertEquals(itr.getWeight(), 1);
    assertFalse(itr.next());
  }

  @Test
  public void twoItemSketchForIterator() {
    KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
    sk.update("1");
    sk.update("2");
    byte[] byteArr = sk.toByteArray();
    KllItemsSketch<String> sk2 = KllItemsSketch.wrap(Memory.wrap(byteArr), Comparator.naturalOrder(), serDe);
    assertTrue(sk2 instanceof KllDirectCompactItemsSketch);
    QuantilesGenericSketchIterator<String> itr = sk2.iterator();
    assertTrue(itr.next());

    assertEquals(itr.getQuantile(), "2");
    assertEquals(itr.getWeight(), 1);

    assertTrue(itr.next());

    assertEquals(itr.getQuantile(), "1");
    assertEquals(itr.getWeight(), 1);
  }

  @Test
  public void twoItemSketchForSortedViewIterator() {
    KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(20, Comparator.naturalOrder(), serDe);
    sk.update("1");
    sk.update("2");
    println(sk.toString(true, true));
    byte[] byteArr = sk.toByteArray();
    KllItemsSketch<String> sk2 = KllItemsSketch.wrap(Memory.wrap(byteArr), Comparator.naturalOrder(), serDe);
    assertTrue(sk2 instanceof KllDirectCompactItemsSketch);
    ItemsSketchSortedView<String> sv = sk2.getSortedView();
    GenericSortedViewIterator<String> itr = sv.iterator();

    assertTrue(itr.next());

    assertEquals(itr.getQuantile(), "1");
    assertEquals(itr.getWeight(), 1);
    assertEquals(itr.getNaturalRank(EXCLUSIVE), 0);
    assertEquals(itr.getNaturalRank(INCLUSIVE), 1);
    assertEquals(itr.getNormalizedRank(EXCLUSIVE), 0);
    assertEquals(itr.getNormalizedRank(INCLUSIVE), 0.5);

    assertTrue(itr.next());

    assertEquals(itr.getQuantile(), "2");
    assertEquals(itr.getWeight(), 1);
    assertEquals(itr.getNaturalRank(EXCLUSIVE), 1);
    assertEquals(itr.getNaturalRank(INCLUSIVE), 2);
    assertEquals(itr.getNormalizedRank(EXCLUSIVE), 0.5);
    assertEquals(itr.getNormalizedRank(INCLUSIVE), 1.0);
  }

  @Test
  public void bigSketches() {
    final int digits = 6;
    for (int n = 1000; n < 100_000; n += 2000) {
      KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(Comparator.naturalOrder(), serDe);
      for (int i = 0; i < n; i++) {
        sk.update(Util.longToFixedLengthString(i, digits));
      }
      byte[] byteArr = sk.toByteArray();
      KllItemsSketch<String> sk2 = KllItemsSketch.wrap(Memory.wrap(byteArr), Comparator.naturalOrder(), serDe);
      assertTrue(sk2 instanceof KllDirectCompactItemsSketch);
      QuantilesGenericSketchIterator<String> itr = sk2.iterator();
      int count = 0;
      int weight = 0;
      while (itr.next()) {
        count++;
        weight += (int)itr.getWeight();
      }
      Assert.assertEquals(count, sk.getNumRetained());
      Assert.assertEquals(weight, n);
    }
  }

  private final static boolean enablePrinting = false;

  /**
   * @param format the format
   * @param args the args
   */
  private static final void printf(final String format, final Object ...args) {
    if (enablePrinting) { System.out.printf(format, args); }
  }

  /**
   * @param o the Object to println
   */
  private static final void println(final Object o) {
    if (enablePrinting) { System.out.println(o.toString()); }
  }

}
