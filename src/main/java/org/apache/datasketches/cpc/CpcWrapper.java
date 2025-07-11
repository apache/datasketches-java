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

package org.apache.datasketches.cpc;

import static org.apache.datasketches.cpc.CpcConfidence.getHipConfidenceLB;
import static org.apache.datasketches.cpc.CpcConfidence.getHipConfidenceUB;
import static org.apache.datasketches.cpc.CpcConfidence.getIconConfidenceLB;
import static org.apache.datasketches.cpc.CpcConfidence.getIconConfidenceUB;
import static org.apache.datasketches.cpc.IconEstimator.getIconEstimate;
import static org.apache.datasketches.cpc.PreambleUtil.checkLoPreamble;
import static org.apache.datasketches.cpc.PreambleUtil.getHipAccum;
import static org.apache.datasketches.cpc.PreambleUtil.getNumCoupons;
import static org.apache.datasketches.cpc.PreambleUtil.hasHip;
import static org.apache.datasketches.cpc.PreambleUtil.isCompressed;
import static org.apache.datasketches.cpc.RuntimeAsserts.rtAssert;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SuppressFBWarnings;

/**
 * This provides a read-only view of a serialized image of a CpcSketch, which can be
 * on-heap or off-heap represented as a MemorySegment object, or on-heap represented as a byte array.
 * @author Lee Rhodes
 * @author Kevin Lang
 */
public final class CpcWrapper {
  MemorySegment seg;

  /**
   * Construct a read-only view of the given MemorySegment that contains a CpcSketch
   * @param seg the given MemorySegment
   */
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "This is OK here")
  public CpcWrapper(final MemorySegment seg) {
    this.seg = seg;
    checkLoPreamble(seg);
    rtAssert(isCompressed(seg));

  }

  /**
   * Construct a read-only view of the given byte array that contains a CpcSketch.
   * @param byteArray the given byte array
   */
  public CpcWrapper(final byte[] byteArray) {
    this(MemorySegment.ofArray(byteArray));
  }

  /**
   * Returns the best estimate of the cardinality of the sketch.
   * @return the best estimate of the cardinality of the sketch.
   */
  public double getEstimate() {
    if (!hasHip(seg)) {
      return getIconEstimate(PreambleUtil.getLgK(seg), getNumCoupons(seg));
    }
    return getHipAccum(seg);
  }

  /**
   * Return the DataSketches identifier for this CPC family of sketches.
   * @return the DataSketches identifier for this CPC family of sketches.
   */
  public static Family getFamily() {
    return Family.CPC;
  }

  /**
   * Returns the configured Log_base2 of K of this sketch.
   * @return the configured Log_base2 of K of this sketch.
   */
  public int getLgK() {
    return PreambleUtil.getLgK(seg);
  }

  /**
   * Returns the best estimate of the lower bound of the confidence interval given <i>kappa</i>,
   * the number of standard deviations from the mean.
   * @param kappa the given number of standard deviations from the mean: 1, 2 or 3.
   * @return the best estimate of the lower bound of the confidence interval given <i>kappa</i>.
   */
  public double getLowerBound(final int kappa) {
    if (!hasHip(seg)) {
      return getIconConfidenceLB(PreambleUtil.getLgK(seg), getNumCoupons(seg), kappa);
    }
    return getHipConfidenceLB(PreambleUtil.getLgK(seg), getNumCoupons(seg), getHipAccum(seg), kappa);
  }

  /**
   * Returns the best estimate of the upper bound of the confidence interval given <i>kappa</i>,
   * the number of standard deviations from the mean.
   * @param kappa the given number of standard deviations from the mean: 1, 2 or 3.
   * @return the best estimate of the upper bound of the confidence interval given <i>kappa</i>.
   */
  public double getUpperBound(final int kappa) {
    if (!hasHip(seg)) {
      return getIconConfidenceUB(PreambleUtil.getLgK(seg), getNumCoupons(seg), kappa);
    }
    return getHipConfidenceUB(PreambleUtil.getLgK(seg), getNumCoupons(seg), getHipAccum(seg), kappa);
  }

}
