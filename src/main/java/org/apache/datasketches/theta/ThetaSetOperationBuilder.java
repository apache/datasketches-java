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

package org.apache.datasketches.theta;

import static org.apache.datasketches.common.Util.LS;
import static org.apache.datasketches.common.Util.TAB;
import static org.apache.datasketches.common.Util.ceilingPowerOf2;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.thetacommon.ThetaUtil;

/**
 * For building a new ThetaSetOperation.
 *
 * @author Lee Rhodes
 */
public final class ThetaSetOperationBuilder {
  private int bLgNomLongs;
  private long bSeed;
  private ResizeFactor bRF;
  private float bP;

  /**
   * Constructor for building a new ThetaSetOperation.  The default configuration is
   * <ul>
   * <li>Max Nominal Entries (max K):
   *   {@value org.apache.datasketches.thetacommon.ThetaUtil#DEFAULT_NOMINAL_ENTRIES}</li>
   * <li>Seed: {@value org.apache.datasketches.common.Util#DEFAULT_UPDATE_SEED}</li>
   * <li>{@link ResizeFactor#X8}</li>
   * <li>Input Sampling Probability: 1.0</li>
   * <li>MemorySegment: null</li>
   * </ul>
   */
  public ThetaSetOperationBuilder() {
    bLgNomLongs = Integer.numberOfTrailingZeros(ThetaUtil.DEFAULT_NOMINAL_ENTRIES);
    bSeed = Util.DEFAULT_UPDATE_SEED;
    bP = (float) 1.0;
    bRF = ResizeFactor.X8;
  }

  /**
   * Sets the Maximum Nominal Entries (max K) for this set operation. The effective value of K of the result of a
   * Set Operation can be less than max K, but never greater.
   * The minimum value is 16 and the maximum value is 67,108,864, which is 2^26.
   * @param nomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entries</a>
   * This will become the ceiling power of 2 if it is not a power of 2.
   * @return this ThetaSetOperationBuilder
   */
  public ThetaSetOperationBuilder setNominalEntries(final int nomEntries) {
    bLgNomLongs = Integer.numberOfTrailingZeros(ceilingPowerOf2(nomEntries));
    if ((bLgNomLongs > ThetaUtil.MAX_LG_NOM_LONGS) || (bLgNomLongs < ThetaUtil.MIN_LG_NOM_LONGS)) {
      throw new SketchesArgumentException("Nominal Entries must be >= 16 and <= 67108864: "
        + nomEntries);
    }
    return this;
  }

  /**
   * Alternative method of setting the Nominal Entries for this set operation from the log_base2 value.
   * The minimum value is 4 and the maximum value is 26.
   * Be aware that set operations as large as this maximum value may not have been
   * thoroughly characterized for performance.
   *
   * @param lgNomEntries the log_base2 Nominal Entries.
   * @return this ThetaSetOperationBuilder
   */
  public ThetaSetOperationBuilder setLogNominalEntries(final int lgNomEntries) {
    bLgNomLongs = ThetaUtil.checkNomLongs(1 << lgNomEntries);
    return this;
  }

  /**
   * Returns Log-base 2 Nominal Entries
   * @return Log-base 2 Nominal Entries
   */
  public int getLgNominalEntries() {
    return bLgNomLongs;
  }

  /**
   * Sets the long seed value that is require by the hashing function.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return this ThetaSetOperationBuilder
   */
  public ThetaSetOperationBuilder setSeed(final long seed) {
    bSeed = seed;
    return this;
  }

  /**
   * Returns the seed
   * @return the seed
   */
  public long getSeed() {
    return bSeed;
  }

  /**
   * Sets the upfront uniform sampling probability, <i>p</i>. Although this functionality is
   * implemented for ThetaUnions only, it rarely makes sense to use it. The proper use of upfront
   * sampling is when building the sketches.
   * @param p <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @return this ThetaSetOperationBuilder
   */
  public ThetaSetOperationBuilder setP(final float p) {
    if ((p <= 0.0) || (p > 1.0)) {
      throw new SketchesArgumentException("p must be > 0 and <= 1.0: " + p);
    }
    bP = p;
    return this;
  }

  /**
   * Returns the pre-sampling probability <i>p</i>
   * @return the pre-sampling probability <i>p</i>
   */
  public float getP() {
    return bP;
  }

  /**
   * Sets the cache Resize Factor
   * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @return this ThetaSetOperationBuilder
   */
  public ThetaSetOperationBuilder setResizeFactor(final ResizeFactor rf) {
    bRF = rf;
    return this;
  }

  /**
   * Returns the Resize Factor
   * @return the Resize Factor
   */
  public ResizeFactor getResizeFactor() {
    return bRF;
  }

  /**
   * Returns a ThetaSetOperation with the current configuration of this Builder and the given Family.
   * @param family the chosen ThetaSetOperation family
   * @return a ThetaSetOperation
   */
  public ThetaSetOperation build(final Family family) {
    return build(family, null);
  }

  /**
   * Returns a ThetaSetOperation with the current configuration of this Builder, the given Family
   * and the given destination MemorySegment. Note that the destination MemorySegment cannot be used with ThetaAnotB.
   * @param family the chosen ThetaSetOperation family
   * @param dstSeg The destination MemorySegment.
   * @return a ThetaSetOperation
   */
  public ThetaSetOperation build(final Family family, final MemorySegment dstSeg) {
    ThetaSetOperation setOp = null;
    switch (family) {
      case UNION: {
        if (dstSeg == null) {
          setOp = ThetaUnionImpl.initNewHeapInstance(bLgNomLongs, bSeed, bP, bRF);
        }
        else {
          setOp = ThetaUnionImpl.initNewDirectInstance(bLgNomLongs, bSeed, bP, bRF, dstSeg);
        }
        break;
      }
      case INTERSECTION: {
        if (dstSeg == null) {
          setOp = ThetaIntersectionImpl.initNewHeapInstance(bSeed);
        }
        else {
          setOp = ThetaIntersectionImpl.initNewDirectInstance(bSeed, dstSeg);
        }
        break;
      }
      case A_NOT_B: {
        if (dstSeg == null) {
          setOp = new ThetaAnotBimpl(bSeed);
        }
        else {
          throw new SketchesArgumentException(
            "ThetaAnotB can not be persisted.");
        }
        break;
      }
      default:
        throw new SketchesArgumentException(
            "Given Family cannot be built as a ThetaSetOperation: " + family.toString());
    }
    return setOp;
  }

  /**
   * Convenience method, returns a configured ThetaSetOperation ThetaUnion with
   * <a href="{@docRoot}/resources/dictionary.html#defaultNomEntries">Default Nominal Entries</a>
   * @return a ThetaUnion object
   */
  public ThetaUnion buildUnion() {
    return (ThetaUnion) build(Family.UNION);
  }

  /**
   * Convenience method, returns a configured ThetaSetOperation ThetaUnion with
   * <a href="{@docRoot}/resources/dictionary.html#defaultNomEntries">Default Nominal Entries</a>
   * and the given destination MemorySegment.
   * @param dstSeg The destination MemorySegment.
   * @return a ThetaUnion object
   */
  public ThetaUnion buildUnion(final MemorySegment dstSeg) {
    return (ThetaUnion) build(Family.UNION, dstSeg);
  }

  /**
   * Convenience method, returns a configured ThetaIntersection with
   * <a href="{@docRoot}/resources/dictionary.html#defaultNomEntries">Default Nominal Entries</a>
   * @return a ThetaIntersection object
   */
  public ThetaIntersection buildIntersection() {
    return (ThetaIntersection) build(Family.INTERSECTION);
  }

  /**
   * Convenience method, returns a configured ThetaIntersection with
   * <a href="{@docRoot}/resources/dictionary.html#defaultNomEntries">Default Nominal Entries</a>
   * and the given destination MemorySegment.
   * @param dstSeg The destination MemorySegment.
   * @return a ThetaIntersection object
   */
  public ThetaIntersection buildIntersection(final MemorySegment dstSeg) {
    return (ThetaIntersection) build(Family.INTERSECTION, dstSeg);
  }

  /**
   * Convenience method, returns a configured ThetaSetOperation ANotB with
   * <a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">Default Update Seed</a>
   * @return a ThetaANotB object
   */
  public ThetaAnotB buildANotB() {
    return (ThetaAnotB) build(Family.A_NOT_B);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("SetOperationBuilder configuration:").append(LS);
    sb.append("LgK:").append(TAB).append(bLgNomLongs).append(LS);
    sb.append("K:").append(TAB).append(1 << bLgNomLongs).append(LS);
    sb.append("Seed:").append(TAB).append(bSeed).append(LS);
    sb.append("p:").append(TAB).append(bP).append(LS);
    sb.append("ResizeFactor:").append(TAB).append(bRF).append(LS);
    return sb.toString();
  }

}
