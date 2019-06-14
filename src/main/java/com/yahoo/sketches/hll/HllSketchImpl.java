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

package com.yahoo.sketches.hll;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;

/**
 * The Abstract HllSketch implementation
 *
 * @author Lee Rhodes
 */
abstract class HllSketchImpl {
  final int lgConfigK;
  final TgtHllType tgtHllType;
  final CurMode curMode;

  HllSketchImpl(final int lgConfigK, final TgtHllType tgtHllType, final CurMode curMode) {
    this.lgConfigK = lgConfigK;
    this.tgtHllType = tgtHllType;
    this.curMode = curMode;
  }

  abstract HllSketchImpl copy();

  abstract HllSketchImpl copyAs(TgtHllType tgtHllType);

  abstract HllSketchImpl couponUpdate(int coupon);

 CurMode getCurMode() {
   return curMode;
 }

  abstract int getCompactSerializationBytes();

  abstract double getCompositeEstimate();

  abstract double getEstimate();

  abstract PairIterator iterator();

  int getLgConfigK() {
    return lgConfigK;
  }

  abstract double getLowerBound(int numStdDev);

  abstract int getMemDataStart();

  abstract int getPreInts();

  TgtHllType getTgtHllType() {
    return tgtHllType;
  }

  abstract int getUpdatableSerializationBytes();

  abstract double getUpperBound(int numStdDev);

  abstract WritableMemory getWritableMemory();

  abstract boolean isCompact();

  abstract boolean isEmpty();

  abstract boolean isMemory();

  abstract boolean isOffHeap();

  abstract boolean isOutOfOrderFlag();

  abstract boolean isSameResource(Memory mem);

  abstract void putOutOfOrderFlag(boolean oooFlag);

  abstract HllSketchImpl reset();

  abstract byte[] toCompactByteArray();

  abstract byte[] toUpdatableByteArray();

}
