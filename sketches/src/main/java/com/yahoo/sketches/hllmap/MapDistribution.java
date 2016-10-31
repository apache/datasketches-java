/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hllmap;

public final class MapDistribution {

  // excluding the first and the last levels
  public static final int NUM_LEVELS = 8; //total of traverse + coupon map levels
  public static final int NUM_TRAVERSE_LEVELS = 3;

  static final int COUPON_MAP_MIN_NUM_ENTRIES = 157;
  static final double COUPON_MAP_SHRINK_TRIGGER_FACTOR = 0.5;
  static final double COUPON_MAP_GROW_TRIGGER_FACTOR = 15.0 / 16.0;
  static final double COUPON_MAP_TARGET_FILL_FACTOR = 2.0 / 3.0;

  public static final int HLL_INIT_NUM_ENTRIES = 100;
  public static final float HLL_RESIZE_FACTOR = 2.0F;

}
