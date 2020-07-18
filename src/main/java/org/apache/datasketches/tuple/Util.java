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

package org.apache.datasketches.tuple;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.datasketches.Util.MIN_LG_ARR_LONGS;
import static org.apache.datasketches.Util.ceilingPowerOf2;
import static org.apache.datasketches.Util.startingSubMultiple;
import static org.apache.datasketches.hash.MurmurHash3.hash;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.XxHash64;

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
    final long[] array = { Double.doubleToLongBits(d) }; // canonicalize all NaN forms
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
   * Computes and checks the 16-bit seed hash from the given long seed.
   * The seed hash may not be zero in order to maintain compatibility with older serialized
   * versions that did not have this concept.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>
   * @return the seed hash.
   */
  public static short computeSeedHash(final long seed) {
    final long[] seedArr = {seed};
    final short seedHash = (short)((hash(seedArr, 0L)[0]) & 0xFFFFL);
    if (seedHash == 0) {
      throw new SketchesArgumentException(
          "The given seed: " + seed + " produced a seedHash of zero. "
              + "You must choose a different seed.");
    }
    return seedHash;
  }

  /**
   * Checks the two given seed hashes. If they are not equal, this method throws an Exception.
   * @param seedHashA given seed hash A
   * @param seedHashB given seed hash B
   */
  public static final void checkSeedHashes(final short seedHashA, final short seedHashB) {
    if (seedHashA != seedHashB) {
      throw new SketchesArgumentException("Incompatible Seed Hashes. " + seedHashA + ", "
          + seedHashB);
    }
  }

  /**
   * Gets the starting capacity of a new sketch given the Nominal Entries and the log Resize Factor.
   * @param nomEntries the given Nominal Entries
   * @param lgResizeFactor the given log Resize Factor
   * @return the starting capacity
   */
  public static int getStartingCapacity(final int nomEntries, final int lgResizeFactor) {
    return 1 << startingSubMultiple(
      // target table size is twice the number of nominal entries
      Integer.numberOfTrailingZeros(ceilingPowerOf2(nomEntries) * 2),
      lgResizeFactor,
      MIN_LG_ARR_LONGS
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
   * @param s the string to hash
   * @return the hash of the string
   */
  public static long stringHash(final String s) {
    return XxHash64.hashChars(s.toCharArray(), 0, s.length(), PRIME);
  }

  /**
   * @param strArray array of Strings
   * @return long hash of concatenated strings.
   */
  public static long stringArrHash(final String[] strArray) {
    final String s = stringConcat(strArray);
    return XxHash64.hashChars(s.toCharArray(), 0, s.length(), PRIME);
  }

}
