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

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

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

  /**
   * Returns a copy of this sketch on the Heap.
   * The LgConfigK, TgtHllType and CurMode are not changed.
   * @return Returns a copy of this sketch on the Heap.
   */
  abstract HllSketchImpl copy();

  /**
   * Returns a copy of this sketch on the Heap with the given TgtHllType.
   * The LgConfigK and CurMode are not changed.
   * @return Returns a copy of this sketch on the Heap with the given TgtHllType.
   */
  abstract HllSketchImpl copyAs(TgtHllType tgtHllType);

  abstract HllSketchImpl couponUpdate(int coupon);

  abstract int getCompactSerializationBytes();

  abstract double getCompositeEstimate();

  CurMode getCurMode() {
    return curMode;
  }

  abstract double getEstimate();

  abstract double getHipEstimate();

  int getLgConfigK() {
    return lgConfigK;
  }

  abstract double getLowerBound(int numStdDev);

  abstract int getMemDataStart();

  abstract Memory getMemory();

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

  abstract boolean isOutOfOrder();

  abstract boolean isRebuildCurMinNumKxQFlag();

  abstract boolean isSameResource(Memory mem);

  abstract PairIterator iterator();

  abstract void mergeTo(HllSketch that);

  abstract void putEmptyFlag(boolean empty);

  abstract void putOutOfOrder(boolean oooFlag);

  abstract void putRebuildCurMinNumKxQFlag(boolean rebuild);

  abstract HllSketchImpl reset();

  abstract byte[] toCompactByteArray();

  abstract byte[] toUpdatableByteArray();

}
