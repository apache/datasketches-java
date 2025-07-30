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

package org.apache.datasketches.partitions;

import static org.apache.datasketches.partitions.BoundsRule.INCLUDE_BOTH;
import static org.apache.datasketches.partitions.BoundsRule.INCLUDE_UPPER;
import static org.apache.datasketches.quantilescommon.LongsAsOrderableStrings.digits;
import static org.apache.datasketches.quantilescommon.LongsAsOrderableStrings.getString;

import java.util.Comparator;

import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.kll.KllItemsSketch;

/**
 * This is an simulated data set with a given N used for testing.
 * @author Lee Rhodes
 */
public class KllItemsSketchFillRequestLongAsString implements SketchFillRequest<String, KllItemsSketch<String>> {
  private final int k;
  private final int numDigits;

  public KllItemsSketchFillRequestLongAsString() {
    k = 1 << 10;
    numDigits = 3;
  }

  public KllItemsSketchFillRequestLongAsString(final int k, final long totalN) {
    this.k = k;
    numDigits = digits(totalN);
  }

  @Override
  public KllItemsSketch<String> getRange(final String lowerQuantile, final String upperQuantile,
      final BoundsRule bounds) {
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), new ArrayOfStringsSerDe());
    long upper, lower;
    try {
      lower = Long.parseLong(lowerQuantile.trim());
      upper = Long.parseLong(upperQuantile.trim());
    } catch (final NumberFormatException e) { throw new SketchesArgumentException(e.toString()); }
    if (bounds == INCLUDE_BOTH) {
      for (long i = lower; i <= upper; i++) { sk.update(getString(i, numDigits)); }
    } else if (bounds == INCLUDE_UPPER) {
      for (long i = lower + 1; i <= upper; i++) { sk.update(getString(i, numDigits)); }
    } else { //INCLUDE_LOWER
      for (long i = lower; i < upper; i++) { sk.update(getString(i, numDigits)); }
    }
    return sk;
  }

  public KllItemsSketch<String> getRange(final long lowerQuantile, final long upperQuantile, final BoundsRule bounds) {
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(k, Comparator.naturalOrder(), new ArrayOfStringsSerDe());
    final long lower = lowerQuantile;
    final long upper = upperQuantile;
    if (bounds == INCLUDE_BOTH) {
      for (long i = lower; i <= upper; i++) { sk.update(getString(i, numDigits)); }
    } else if (bounds == INCLUDE_UPPER) {
      for (long i = lower + 1; i <= upper; i++) { sk.update(getString(i, numDigits)); }
    } else { //INCLUDE_LOWER
      for (long i = lower; i < upper; i++) { sk.update(getString(i, numDigits)); }
    }
    return sk;
  }

}
