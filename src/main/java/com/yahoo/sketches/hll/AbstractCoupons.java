/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;

/**
 * @author Lee Rhodes
 */
abstract class AbstractCoupons extends HllSketchImpl {

  abstract int getCouponCount();

  abstract int getCouponIntArrLen();

  abstract int getLgCouponArrInts();

  abstract void populateCouponIntArrFromMem(Memory srcMem, int lenInts); //TODO ??

  abstract void populateMemFromCouponIntArr(WritableMemory dstWmem, int lenInts);

  abstract void putCouponCount(int couponCount);

  abstract void putCouponIntArr(int[] couponIntArr);

  abstract void putLgCouponArrInts(int lgCouponArrInts);

}
