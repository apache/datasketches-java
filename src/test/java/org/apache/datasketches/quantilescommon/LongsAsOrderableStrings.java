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

import static java.lang.Math.ceil;
import static java.lang.Math.log;
import static org.apache.datasketches.common.Util.characterPad;

import org.apache.datasketches.common.SketchesArgumentException;

/**
 * Creates a string from a positive long value that is orderable in the
 * same order as its long value.
 */
public final class LongsAsOrderableStrings {

  /**
   * Converts the given long into a string with leading spaces based on the given numDigits.
   * This allows the stings to be ordered as if they were longs.
   * @param value the value to convert
   * @param numDigits the maximum required number of total spaces for digits.
   * @return the given long into a string with leading spaces
   */
  public static String getString(final long value, final int numDigits) {
    return characterPad(Long.toString(value), numDigits, ' ', false);
  }

  /**
   * Converts the given String back to a long by trimming any leading or trailing spaces.
   * @param value the given string to convert
   * @return the given String back to a long
   */
  public static long getLong(final String value) {
    long out;
    try { out = Long.parseLong(value.trim()); } catch (NumberFormatException e) {
      throw new SketchesArgumentException(e.toString());
    }
    return out;
  }

  /**
   * Computes the number of digits required to display the given positive long value.
   * This does not include commas or other digit separators.
   * This works with longs less than 1E15.
   * @param maxValue the maximum anticipated long value.
   * @return the number of required display digits
   */
  public static int digits(final long maxValue) {
    if (maxValue <= 0) { return 1; }
    return (int) ceil(log(maxValue + 1) / log(10.0));
  }

}
