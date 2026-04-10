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

import static org.testng.Assert.fail;

import java.util.Comparator;

import org.apache.datasketches.common.SketchesArgumentException;
import org.testng.annotations.Test;

public final class SortedViewEmptyAccessorTest {

  @Test
  public void checkFloatsSortedViewEmptyAccessors() {
    final FloatsSketchSortedView sv = new FloatsSketchSortedView(
        new float[] {1F},
        new long[] {1},
        0,
        1F,
        1F);

    assertEmptyAccessorThrows(() -> sv.getMaxItem());
    assertEmptyAccessorThrows(() -> sv.getMinItem());
  }

  @Test
  public void checkDoublesSortedViewEmptyAccessors() {
    final DoublesSketchSortedView sv = new DoublesSketchSortedView(
        new double[] {1.0},
        new long[] {1},
        0,
        1.0,
        1.0);

    assertEmptyAccessorThrows(() -> sv.getMaxItem());
    assertEmptyAccessorThrows(() -> sv.getMinItem());
  }

  @Test
  public void checkLongsSortedViewEmptyAccessors() {
    final LongsSketchSortedView sv = new LongsSketchSortedView(
        new long[] {1},
        new long[] {1},
        0,
        1,
        1);

    assertEmptyAccessorThrows(() -> sv.getMaxItem());
    assertEmptyAccessorThrows(() -> sv.getMinItem());
  }

  @Test
  public void checkItemsSortedViewEmptyAccessors() {
    final ItemsSketchSortedView<String> sv = new ItemsSketchSortedView<>(
        new String[] {"1"},
        new long[] {1},
        0,
        Comparator.naturalOrder(),
        "1",
        "1",
        String.class,
        .01,
        1);

    assertEmptyAccessorThrows(() -> sv.getMaxItem());
    assertEmptyAccessorThrows(() -> sv.getMinItem());
  }

  private static void assertEmptyAccessorThrows(final Runnable accessor) {
    try {
      accessor.run();
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }
  }
}
