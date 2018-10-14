/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

/**
 * Note: except for brief transitional moments, these sketches always obey the following strict
 * mapping between the flavor of a sketch and the number of coupons that it has collected.
 * @author Lee Rhodes
 * @author Kevin Lang
 */
enum Flavor {
  EMPTY,   //    0  == C <    1
  SPARSE,  //    1  <= C <   3K/32
  HYBRID,  // 3K/32 <= C <   K/2
  PINNED,  //   K/2 <= C < 27K/8  [NB: 27/8 = 3 + 3/8]
  SLIDING; // 27K/8 <= C

  private static Flavor[] fmtArr = Flavor.class.getEnumConstants();

  /**
   * Returns the Flavor given its enum ordinal
   * @param ordinal the given enum ordinal
   * @return the Flavor given its enum ordinal
   */
  static Flavor ordinalToFlavor(final int ordinal) {
    return fmtArr[ordinal];
  }

}
