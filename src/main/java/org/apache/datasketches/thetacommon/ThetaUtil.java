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

package org.apache.datasketches.thetacommon;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;

/**
 * Utility methods for the Theta Family of sketches
 * @author Lee Rhodes
 *
 */
public final class ThetaUtil {

  /**
   * The smallest Log2 nom entries allowed: 4.
   */
  public static final int MIN_LG_NOM_LONGS = 4;
  /**
   * The largest Log2 nom entries allowed: 26.
   */
  public static final int MAX_LG_NOM_LONGS = 26;
  /**
   * The hash table rebuild threshold = 15.0/16.0.
   */
  public static final double REBUILD_THRESHOLD = 15.0 / 16.0;
  /**
   * The resize threshold = 0.5; tuned for speed.
   */
  public static final double RESIZE_THRESHOLD = 0.5;
  /**
   * The default nominal entries is provided as a convenience for those cases where the
   * nominal sketch size in number of entries is not provided.
   * A sketch of 4096 entries has a Relative Standard Error (RSE) of +/- 1.56% at a confidence of
   * 68%; or equivalently, a Relative Error of +/- 3.1% at a confidence of 95.4%.
   * <a href="{@docRoot}/resources/dictionary.html#defaultNomEntries">See Default Nominal Entries</a>
   */
  public static final int DEFAULT_NOMINAL_ENTRIES = 4096;

  private ThetaUtil() {}

  /**
   * The smallest Log2 cache size allowed: 5.
   */
  public static final int MIN_LG_ARR_LONGS = 5;

  /**
   * Gets the smallest allowed exponent of 2 that it is a sub-multiple of the target by zero,
   * one or more resize factors.
   *
   * @param lgTarget Log2 of the target size
   * @param lgRF Log_base2 of Resize Factor.
   * <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @param lgMin Log2 of the minimum allowed starting size
   * @return The Log2 of the starting size
   */
  public static int startingSubMultiple(final int lgTarget, final int lgRF, final int lgMin) {
    return lgTarget <= lgMin ? lgMin : lgRF == 0 ? lgTarget : (lgTarget - lgMin) % lgRF + lgMin;
  }

  /**
   * Checks that the given nomLongs is within bounds and returns the Log2 of the ceiling power of 2
   * of the given nomLongs.
   * @param nomLongs the given number of nominal longs.  This can be any value from 16 to
   * 67108864, inclusive.
   * @return The Log2 of the ceiling power of 2 of the given nomLongs.
   */
  public static int checkNomLongs(final int nomLongs) {
    final int lgNomLongs = Integer.numberOfTrailingZeros(Util.ceilingPowerOf2(nomLongs));
    if (lgNomLongs > MAX_LG_NOM_LONGS || lgNomLongs < MIN_LG_NOM_LONGS) {
      throw new SketchesArgumentException("Nominal Entries must be >= 16 and <= 67108864: "
        + nomLongs);
    }
    return lgNomLongs;
  }

}

