/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hllmap;

import static com.yahoo.sketches.hllmap.Util.fmtDouble;
import static com.yahoo.sketches.hllmap.Util.fmtLong;

abstract class CouponMap extends Map {

  private static final String LS = System.getProperty("line.separator");

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
    final String mcpe = fmtLong(getMaxCouponsPerEntry());
    final String ccpe = fmtLong(getCapacityCouponsPerEntry());
    final String te = fmtLong(getTableEntries());
    final String ce = fmtLong(getCapacityEntries());
    final String cce = fmtLong(getCurrentCountEntries());
    final String ae = fmtLong(getActiveEntries());
    final String de = fmtLong(getDeletedEntries());
    final String esb = fmtDouble(getEntrySizeBytes());
    final String mub = fmtLong(getMemoryUsageBytes());

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
