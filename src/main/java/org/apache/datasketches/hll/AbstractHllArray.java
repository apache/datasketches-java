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

import static org.apache.datasketches.Util.invPow2;
import static org.apache.datasketches.hll.PreambleUtil.HLL_BYTE_ARR_START;
import static org.apache.datasketches.hll.PreambleUtil.HLL_PREINTS;
import static org.apache.datasketches.hll.TgtHllType.HLL_4;
import static org.apache.datasketches.hll.TgtHllType.HLL_6;

import org.apache.datasketches.SketchesStateException;

/**
 * @author Lee Rhodes
 */
abstract class AbstractHllArray extends HllSketchImpl {
  AuxHashMap auxHashMap = null; //used for both heap and direct HLL4
  final int auxStart; //used for direct HLL4

  AbstractHllArray(final int lgConfigK, final TgtHllType tgtHllType, final CurMode curMode) {
    super(lgConfigK, tgtHllType, curMode);
    auxStart = HLL_BYTE_ARR_START + hll4ArrBytes(lgConfigK);
  }

  abstract void addToHipAccum(double delta);

  @Override
  HllArray copyAs(final TgtHllType tgtHllType) {
    if (tgtHllType == getTgtHllType()) {
      return (HllArray) copy();
    }
    if (tgtHllType == HLL_4) {
      return Conversions.convertToHll4(this);
    }
    if (tgtHllType == HLL_6) {
      return Conversions.convertToHll6(this);
    }
    return Conversions.convertToHll8(this);
  }

  abstract void decNumAtCurMin();

  AuxHashMap getAuxHashMap() {
    return auxHashMap;
  }

  PairIterator getAuxIterator() {
    return (auxHashMap == null) ? null : auxHashMap.getIterator();
  }

  @Override
  int getCompactSerializationBytes() {
    final AuxHashMap auxHashMap = getAuxHashMap();
    final int auxCountBytes = (auxHashMap == null) ? 0 : auxHashMap.getAuxCount() << 2;
    return HLL_BYTE_ARR_START + getHllByteArrBytes() + auxCountBytes;
  }

  /**
   * This is the (non-HIP) estimator.
   * It is called "composite" because multiple estimators are pasted together.
   * @return the composite estimate
   */
  //In C: again-two-registers.c hhb_get_composite_estimate L1489
  @Override
  double getCompositeEstimate() {
    return HllEstimators.hllCompositeEstimate(this);
  }

  abstract int getCurMin();

  @Override
  double getEstimate() {
    if (isOutOfOrder()) {
      return getCompositeEstimate();
    }
    return getHipAccum();
  }

  /**
   * For each actual update of the sketch, where the state of the sketch is changed, this register
   * tracks the Historical Inverse Probability or HIP. Before the update is recorded this register
   * is incremented by adding the inverse probability 1/Q (defined below). Since KxQ is scaled by K,
   * the actual increment is K/KxQ as can be seen in the hipAndKxQIncrementalUpdate(...) method
   * below.
   * @return the HIP Accumulator
   */
  abstract double getHipAccum();

  @Override
  double getHipEstimate() {
    return getHipAccum();
  }

  abstract int getHllByteArrBytes();

  /**
   * Q = KxQ/K is the probability that an incoming event can modify the state of the sketch.
   * KxQ is literally K times Q. The HIP estimator is based on tracking this probability as the
   * sketch gets populated. It is tracked in the hipAccum register.
   *
   * <p>The KxQ registers serve dual purposes: They are used in the HIP estimator and in
   * the "raw" HLL estimator defined in the Flajolet, et al, 2007 HLL paper. In order to do this,
   * the way the KxQ registers are computed here differ from how they are defined in the paper.</p>
   *
   * <p>The paper Fig 2 defines</p>
   * <pre>Z := ( sum[j=1,m](2^(-M[j])) )^(-1).</pre>
   * But the HIP estimator requires a computation of the probability defined above.
   * We accomplish both by redefing Z as
   * <pre>Z := ( m + sum[j=1,m](2^(-M[j] - 1)) )^(-1).</pre>
   * They are mathematically equivalent since:
   * <pre>m + sum[j=1,m](2^(-M[j] - 1)) == m + sum[j=1,m](2^(-M[j])) - m == sum[j=1,m](2^(-M[j])).</pre>
   *
   * @return KxQ0
   */
  abstract double getKxQ0();

  /**
   * This second KxQ register is shifted by 32 bits to give us more than 90 bits of mantissa
   * precision, which produces more accurate results for very large counts.
   * @return KxQ1
   */
  abstract double getKxQ1();

  @Override
  double getLowerBound(final int numStdDev) {
    HllUtil.checkNumStdDev(numStdDev);
    return HllEstimators.hllLowerBound(this, numStdDev);
  }

  @Override
  int getMemDataStart() {
    return HLL_BYTE_ARR_START;
  }

  abstract AuxHashMap getNewAuxHashMap();

  /**
   * Returns the number of slots that have the value CurMin.
   * If CurMin is 0, then it returns the number of zeros in the array.
   * @return the number of slots that have the value CurMin.
   */
  abstract int getNumAtCurMin();

  @Override
  int getPreInts() {
    return HLL_PREINTS;
  }

  //overridden by Hll4Array and DirectHll4Array
  abstract int getNibble(int slotNo);

  abstract int getSlotValue(int slotNo);

  @Override //used by HLL6 and HLL8, Overridden by HLL4
  int getUpdatableSerializationBytes() {
    return HLL_BYTE_ARR_START + getHllByteArrBytes();
  }

  @Override
  double getUpperBound(final int numStdDev) {
    HllUtil.checkNumStdDev(numStdDev);
    return HllEstimators.hllUpperBound(this, numStdDev);
  }

  @Override
  abstract PairIterator iterator();

  @Override
  void mergeTo(final HllSketch that) {
    throw new SketchesStateException("Possible Corruption, improper access.");
  }

  abstract void putAuxHashMap(AuxHashMap auxHashMap, boolean compact);

  abstract void putCurMin(int curMin);

  abstract void putHipAccum(double hipAccum);

  abstract void putKxQ0(double kxq0);

  abstract void putKxQ1(double kxq1);

  //overridden by Hll4Array and DirectHll4Array
  abstract void putNibble(int slotNo, int nibValue);

  abstract void putNumAtCurMin(int numAtCurMin);

  abstract void updateSlotWithKxQ(final int slotNo, final int value);

  abstract void updateSlotNoKxQ(final int slotNo, final int value);

  //Compute HLL byte array lengths, used by both heap and direct.

  static final int hll4ArrBytes(final int lgConfigK) {
    return 1 << (lgConfigK - 1);
  }

  static final int hll6ArrBytes(final int lgConfigK) {
    final int numSlots = 1 << lgConfigK;
    return ((numSlots * 3) >>> 2) + 1;
  }

  static final int hll8ArrBytes(final int lgConfigK) {
    return 1 << lgConfigK;
  }

  /**
   * Common HIP and KxQ incremental update for all heap and direct Hll.
   * This is used when incrementally updating an existing array with non-zero values.
   * @param host the origin implementation
   * @param oldValue old value
   * @param newValue new value
   */
  //Called here and by Heap and Direct 6 and 8 bit implementations
  //In C: again-two-registers.c Lines 851 to 871
  static final void hipAndKxQIncrementalUpdate(final AbstractHllArray host,
      final int oldValue, final int newValue) {
    assert newValue > oldValue;
    final double kxq0 = host.getKxQ0();
    final double kxq1 = host.getKxQ1();
    //update hipAccum BEFORE updating kxq0 and kxq1
    host.addToHipAccum((1 << host.getLgConfigK()) / (kxq0 + kxq1));
    incrementalUpdateKxQ(host, oldValue, newValue, kxq0, kxq1);
  }

  //separate KxQ updates
  static final void incrementalUpdateKxQ(final AbstractHllArray host,
      final int oldValue, final int newValue, double kxq0, double kxq1) {
    //update kxq0 and kxq1; subtract first, then add.
    if (oldValue < 32) { host.putKxQ0(kxq0 -= invPow2(oldValue)); }
    else               { host.putKxQ1(kxq1 -= invPow2(oldValue)); }
    if (newValue < 32) { host.putKxQ0(kxq0 += invPow2(newValue)); }
    else               { host.putKxQ1(kxq1 += invPow2(newValue)); }
  }
}
