/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

/**
 * There are seven different preamble formats (8 combinations) that determine the layout of the
 * <i>HiField</i> variables after the first 8 bytes of the preamble.
 * Do not change the order.
 */
enum Format {
  EMPTY_MERGED,
  EMPTY_HIP,
  SPARSE_HYBRID_MERGED,
  SPARSE_HYBRID_HIP,
  PINNED_SLIDING_MERGED_NOSV,
  PINNED_SLIDING_HIP_NOSV,
  PINNED_SLIDING_MERGED,
  PINNED_SLIDING_HIP;

  private static Format[] fmtArr = Format.class.getEnumConstants();

  /**
   * Returns the Format given its enum ordinal
   * @param ordinal the given enum ordinal
   * @return the Format given its enum ordinal
   */
  static Format ordinalToFormat(final int ordinal) {
    return fmtArr[ordinal];
  }

} //end enum Format
