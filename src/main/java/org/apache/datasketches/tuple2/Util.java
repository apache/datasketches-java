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

package org.apache.datasketches.tuple2;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.datasketches.common.Util.ceilingPowerOf2;
import static org.apache.datasketches.hash.XxHash.hashCharArr;
import static org.apache.datasketches.hash.XxHash.hashString;

import java.lang.reflect.Array;

import org.apache.datasketches.thetacommon2.ThetaUtil;

/**
 * Common utility functions for Tuples
 */
public final class Util {
  private static final int PRIME = 0x7A3C_CA71;

  /**
   * Converts a <i>double</i> to a <i>long[]</i>.
   * @param value the given double value
   * @return the long array
   */
  public static final long[] doubleToLongArray(final double value) {
    final double d = (value == 0.0) ? 0.0 : value; // canonicalize -0.0, 0.0
    final long[] array = { Double.doubleToLongBits(d) }; // canonicalize all NaN & +/- infinity forms
    return array;
  }

  /**
   * Converts a String to a UTF_8 byte array. If the given value is either null or empty this
   * method returns null.
   * @param value the given String value
   * @return the UTF_8 byte array
   */
  public static final byte[] stringToByteArray(final String value) {
    if ((value == null) || value.isEmpty()) { return null; }
    return value.getBytes(UTF_8);
  }

  /**
   * Gets the starting capacity of a new sketch given the Nominal Entries and the log Resize Factor.
   * @param nomEntries the given Nominal Entries
   * @param lgResizeFactor the given log Resize Factor
   * @return the starting capacity
   */
  public static int getStartingCapacity(final int nomEntries, final int lgResizeFactor) {
    return 1 << ThetaUtil.startingSubMultiple(
      // target table size is twice the number of nominal entries
      Integer.numberOfTrailingZeros(ceilingPowerOf2(nomEntries) * 2),
      lgResizeFactor,
      ThetaUtil.MIN_LG_ARR_LONGS
    );
  }

  /**
   * Concatenate array of Strings to a single String.
   * @param strArr the given String array
   * @return the concatenated String
   */
  public static String stringConcat(final String[] strArr) {
    final int len = strArr.length;
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < len; i++) {
      sb.append(strArr[i]);
      if ((i + 1) < len) { sb.append(','); }
    }
    return sb.toString();
  }

  /**
   * Returns the hash of the given string
   * @param s the string to hash
   * @return the hash of the given string
   */
  public static long stringHash(final String s) {
    return hashString(s, 0, s.length(), PRIME);
  }

  /**
   * Returns the hash of the concatenated strings
   * @param strArray array of Strings
   * @return the hash of concatenated strings.
   */
  public static long stringArrHash(final String[] strArray) {
    final String s = stringConcat(strArray);
    return hashCharArr(s.toCharArray(), 0, s.length(), PRIME);
  }

  /**
   * Will copy compact summary arrays as well as hashed summary tables (with nulls).
   * @param <S> type of summary
   * @param summaryArr the given summary array or table
   * @return the copy
   */
  @SuppressWarnings("unchecked")
  public static <S extends Summary> S[] copySummaryArray(final S[] summaryArr) {
    final int len = summaryArr.length;
    final S[] tmpSummaryArr = newSummaryArray(summaryArr, len);
    for (int i = 0; i < len; i++) {
      final S summary = summaryArr[i];
      if (summary == null) { continue; }
      tmpSummaryArr[i] = (S) summary.copy();
    }
    return tmpSummaryArr;
  }

  /**
   * Creates a new Summary Array with the specified length
   * @param summaryArr example array, only used to obtain the component type. It has no data.
   * @param length the desired length of the returned array.
   * @param <S> the summary class type
   * @return a new Summary Array with the specified length
   */
  @SuppressWarnings("unchecked")
  public static <S extends Summary> S[] newSummaryArray(final S[] summaryArr, final int length) {
    final Class<S> summaryType = (Class<S>) summaryArr.getClass().getComponentType();
    final S[] tmpSummaryArr = (S[]) Array.newInstance(summaryType, length);
    return tmpSummaryArr;
  }

}
