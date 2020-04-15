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

package org.apache.datasketches.hll;

import static java.lang.Math.max;
import static org.apache.datasketches.hll.HllUtil.COUPON_RSE;
import static org.apache.datasketches.hll.HllUtil.EMPTY;
import static org.apache.datasketches.hll.HllUtil.KEY_MASK_26;
import static org.apache.datasketches.hll.ToByteArrayImpl.toCouponByteArray;

import org.apache.datasketches.SketchesArgumentException;

/**
 * @author Lee Rhodes
 */
abstract class AbstractCoupons extends HllSketchImpl {

  AbstractCoupons(final int lgConfigK, final TgtHllType tgtHllType, final CurMode curMode) {
    super(lgConfigK, tgtHllType, curMode);
  }

  @Override
  double getCompositeEstimate() {
    return getEstimate();
  }

  abstract int getCouponCount();

  abstract int[] getCouponIntArr();

  /**
   * This is the estimator for the Coupon List mode and Coupon Hash Set mode.
   *
   * <p>Note: This is an approximation to the true mapping from numCoupons to N,
   * which has a range of validity roughly from 0 to 6 million coupons.</p>
   *
   * <p>The k of the implied coupon sketch, which must not be confused with the k of the HLL
   * sketch.  In this application k is always 2^26, which is the number of address bits of the
   * 32-bit coupon.</p>
   * @return the unique count estimate.
   */
  @Override
  double getEstimate() {
    final int couponCount = getCouponCount();
    final double est = CubicInterpolation.usingXAndYTables(CouponMapping.xArr,
        CouponMapping.yArr, couponCount);
    return max(est, couponCount);
  }

  @Override
  double getHipEstimate() {
    return getEstimate();
  }

  abstract int getLgCouponArrInts();

  @Override
  double getLowerBound(final int numStdDev) {
    HllUtil.checkNumStdDev(numStdDev);
    final int couponCount = getCouponCount();
    final double est = CubicInterpolation.usingXAndYTables(CouponMapping.xArr,
        CouponMapping.yArr, couponCount);
    final double tmp = est / (1.0 + (numStdDev * COUPON_RSE));
    return max(tmp, couponCount);
  }

  @Override
  double getUpperBound(final int numStdDev) {
    HllUtil.checkNumStdDev(numStdDev);
    final int couponCount = getCouponCount();
    final double est = CubicInterpolation.usingXAndYTables(CouponMapping.xArr,
        CouponMapping.yArr, couponCount);
    final double tmp = est / (1.0 - (numStdDev * COUPON_RSE));
    return max(tmp, couponCount);
  }

  @Override
  int getUpdatableSerializationBytes() {
    return getMemDataStart() + (4 << getLgCouponArrInts());
  }

  @Override
  boolean isEmpty() {
    return getCouponCount() == 0;
  }

  @Override
  boolean isOutOfOrder() {
    return false;
  }

  @Override
  boolean isRebuildCurMinNumKxQFlag() {
    return false;
  }

  @Override
  void putEmptyFlag(final boolean empty) {} //no-op for coupons

  @Override
  void putOutOfOrder(final boolean outOfOrder) {} //no-op for coupons

  @Override
  void putRebuildCurMinNumKxQFlag(final boolean rebuild) {} //no-op for coupons

  @Override
  byte[] toCompactByteArray() {
    return toCouponByteArray(this, true);
  }

  @Override
  byte[] toUpdatableByteArray() {
    return toCouponByteArray(this, false);
  }

  //FIND for Heap and Direct
  //Searches the Coupon hash table for an empty slot or a duplicate depending on the context.
  //If entire entry is empty, returns one's complement of index = found empty.
  //If entry equals given coupon, returns its index = found duplicate coupon
  //Continues searching
  //If the probe comes back to original index, throws an exception.
  //Called by CouponHashSet.couponUpdate()
  //Called by CouponHashSet.growHashSet()
  //Called by DirectCouponHashSet.growHashSet()
  static final int find(final int[] array, final int lgArrInts, final int coupon) {
    final int arrMask = array.length - 1;
    int probe = coupon & arrMask;
    final int loopIndex = probe;
    do {
      final int couponAtIdx = array[probe];
      if (couponAtIdx == EMPTY) {
        return ~probe; //empty
      }
      else if (coupon == couponAtIdx) {
        return probe; //duplicate
      }
      final int stride = ((coupon & KEY_MASK_26) >>> lgArrInts) | 1;
      probe = (probe + stride) & arrMask;
    } while (probe != loopIndex);
    throw new SketchesArgumentException("Key not found and no empty slots!");
  }

}
