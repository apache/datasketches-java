/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

/**
 * Base class and API for the intermediate maps.
 *
 * @author Lee Rhodes
 * @author Alex Saydakov
 * @author Kevin Lang
 */
abstract class CouponMap extends Map {
  private static final String LS = System.getProperty("line.separator");

  // These parameters are tuned to avoid pathological resizing.
  // Consider modeling the behavior before changing
  static final int COUPON_MAP_MIN_NUM_ENTRIES = 157;
  static final double COUPON_MAP_SHRINK_TRIGGER_FACTOR = 0.5;

  /**
   * @param keySizeBytes size of keys in bytes
   */
  CouponMap(final int keySizeBytes) {
    super(keySizeBytes);
  }

  abstract int findKey(byte[] key);

  abstract int findOrInsertKey(byte[] key);

  abstract void deleteKey(int index);

  abstract void updateEstimate(int index, double estimate);

  abstract double findOrInsertCoupon(int index, short coupon);

  abstract int getCouponCount(int index);

  abstract CouponsIterator getCouponsIterator(byte[] key);

  abstract int getMaxCouponsPerEntry();

  abstract int getCapacityCouponsPerEntry();

  abstract int getActiveEntries();

  abstract int getDeletedEntries();

  @Override
  public String toString() {
    final String mcpe = Map.fmtLong(getMaxCouponsPerEntry());
    final String ccpe = Map.fmtLong(getCapacityCouponsPerEntry());
    final String te = Map.fmtLong(getTableEntries());
    final String ce = Map.fmtLong(getCapacityEntries());
    final String cce = Map.fmtLong(getCurrentCountEntries());
    final String ae = Map.fmtLong(getActiveEntries());
    final String de = Map.fmtLong(getDeletedEntries());
    final String esb = Map.fmtDouble(getEntrySizeBytes());
    final String mub = Map.fmtLong(getMemoryUsageBytes());

    final StringBuilder sb = new StringBuilder();
    final String thisSimpleName = this.getClass().getSimpleName();
    sb.append("### ").append(thisSimpleName).append(" SUMMARY: ").append(LS);
    sb.append("    Max Coupons Per Entry     : ").append(mcpe).append(LS);
    sb.append("    Capacity Coupons Per Entry: ").append(ccpe).append(LS);
    sb.append("    Table Entries             : ").append(te).append(LS);
    sb.append("    Capacity Entries          : ").append(ce).append(LS);
    sb.append("    Current Count Entries     : ").append(cce).append(LS);
    sb.append("      Active Entries          : ").append(ae).append(LS);
    sb.append("      Deleted Entries         : ").append(de).append(LS);
    sb.append("    Entry Size Bytes          : ").append(esb).append(LS);
    sb.append("    Memory Usage Bytes        : ").append(mub).append(LS);
    sb.append("### END SKETCH SUMMARY").append(LS);
    return sb.toString();
  }

}
