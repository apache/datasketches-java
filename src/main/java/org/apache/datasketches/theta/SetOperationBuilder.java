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

import static org.apache.datasketches.Util.DEFAULT_NOMINAL_ENTRIES;
import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;
import static org.apache.datasketches.Util.LS;
import static org.apache.datasketches.Util.MAX_LG_NOM_LONGS;
import static org.apache.datasketches.Util.MIN_LG_NOM_LONGS;
import static org.apache.datasketches.Util.TAB;
import static org.apache.datasketches.Util.ceilingPowerOf2;

import org.apache.datasketches.Family;
import org.apache.datasketches.ResizeFactor;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.DefaultMemoryRequestServer;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;

/**
 * For building a new SetOperation.
 *
 * @author Lee Rhodes
 */
public class SetOperationBuilder {
  private int bLgNomLongs;
  private long bSeed;
  private ResizeFactor bRF;
  private float bP;
  private MemoryRequestServer bMemReqSvr;

  /**
   * Constructor for building a new SetOperation.  The default configuration is
   * <ul>
   * <li>Nominal Entries: {@value org.apache.datasketches.Util#DEFAULT_NOMINAL_ENTRIES}</li>
   * <li>Seed: {@value org.apache.datasketches.Util#DEFAULT_UPDATE_SEED}</li>
   * <li>{@link ResizeFactor#X8}</li>
   * <li>Input Sampling Probability: 1.0</li>
   * <li>Memory: null</li>
   * </ul>
   */
  public SetOperationBuilder() {
    bLgNomLongs = Integer.numberOfTrailingZeros(DEFAULT_NOMINAL_ENTRIES);
    bSeed = DEFAULT_UPDATE_SEED;
    bP = (float) 1.0;
    bRF = ResizeFactor.X8;
    bMemReqSvr = new DefaultMemoryRequestServer();
  }

  /**
   * Sets the Nominal Entries for this set operation. The minimum value is 16 and the maximum value
   * is 67,108,864, which is 2^26. Be aware that Unions as large as this maximum value have not
   * been thoroughly tested or characterized for performance.
   * @param nomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entres</a>
   * This will become the ceiling power of 2 if it is not.
   * @return this SetOperationBuilder
   */
  public SetOperationBuilder setNominalEntries(final int nomEntries) {
    bLgNomLongs = Integer.numberOfTrailingZeros(ceilingPowerOf2(nomEntries));
    if ((bLgNomLongs > MAX_LG_NOM_LONGS) || (bLgNomLongs < MIN_LG_NOM_LONGS)) {
      throw new SketchesArgumentException("Nominal Entries must be >= 16 and <= 67108864: "
        + nomEntries);
    }
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
   * @return this SetOperationBuilder
   */
  public SetOperationBuilder setSeed(final long seed) {
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
   * implemented for Unions only, it rarely makes sense to use it. The proper use of upfront
   * sampling is when building the sketches.
   * @param p <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @return this SetOperationBuilder
   */
  public SetOperationBuilder setP(final float p) {
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
   * @return this SetOperationBuilder
   */
  public SetOperationBuilder setResizeFactor(final ResizeFactor rf) {
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
   * Set the MemoryRequestServer
   * @param memReqSvr the given MemoryRequestServer
   * @return this SetOperationBuilder
   */
  public SetOperationBuilder setMemoryRequestServer(final MemoryRequestServer memReqSvr) {
    bMemReqSvr = memReqSvr;
    return this;
  }

  /**
   * Returns the MemoryRequestServer
   * @return the MemoryRequestServer
   */
  public MemoryRequestServer getMemoryRequestServer() {
    return bMemReqSvr;
  }

  /**
   * Returns a SetOperation with the current configuration of this Builder and the given Family.
   * @param family the chosen SetOperation family
   * @return a SetOperation
   */
  public SetOperation build(final Family family) {
    return build(family, null);
  }

  /**
   * Returns a SetOperation with the current configuration of this Builder, the given Family
   * and the given destination memory. Note that the destination memory cannot be used with AnotB.
   * @param family the chosen SetOperation family
   * @param dstMem The destination Memory.
   * @return a SetOperation
   */
  public SetOperation build(final Family family, final WritableMemory dstMem) {
    SetOperation setOp = null;
    switch (family) {
      case UNION: {
        if (dstMem == null) {
          setOp = UnionImpl.initNewHeapInstance(bLgNomLongs, bSeed, bP, bRF);
        }
        else {
          setOp = UnionImpl.initNewDirectInstance(bLgNomLongs, bSeed, bP, bRF, bMemReqSvr, dstMem);
        }
        break;
      }
      case INTERSECTION: {
        if (dstMem == null) {
          setOp = IntersectionImpl.initNewHeapInstance(bSeed);
        }
        else {
          setOp = IntersectionImpl.initNewDirectInstance(bSeed, dstMem);
        }
        break;
      }
      case A_NOT_B: {
        if (dstMem == null) {
          setOp = new AnotBimpl(bSeed);
        }
        else {
          throw new SketchesArgumentException(
            "AnotB can not be persisted.");
        }
        break;
      }
      default:
        throw new SketchesArgumentException(
            "Given Family cannot be built as a SetOperation: " + family.toString());
    }
    return setOp;
  }

  /**
   * Convenience method, returns a configured SetOperation Union with
   * <a href="{@docRoot}/resources/dictionary.html#defaultNomEntries">Default Nominal Entries</a>
   * @return a Union object
   */
  public Union buildUnion() {
    return (Union) build(Family.UNION);
  }

  /**
   * Convenience method, returns a configured SetOperation Union with
   * <a href="{@docRoot}/resources/dictionary.html#defaultNomEntries">Default Nominal Entries</a>
   * and the given destination memory.
   * @param dstMem The destination Memory.
   * @return a Union object
   */
  public Union buildUnion(final WritableMemory dstMem) {
    return (Union) build(Family.UNION, dstMem);
  }

  /**
   * Convenience method, returns a configured SetOperation Intersection with
   * <a href="{@docRoot}/resources/dictionary.html#defaultNomEntries">Default Nominal Entries</a>
   * @return an Intersection object
   */
  public Intersection buildIntersection() {
    return (Intersection) build(Family.INTERSECTION);
  }

  /**
   * Convenience method, returns a configured SetOperation Intersection with
   * <a href="{@docRoot}/resources/dictionary.html#defaultNomEntries">Default Nominal Entries</a>
   * and the given destination memory.
   * @param dstMem The destination Memory.
   * @return an Intersection object
   */
  public Intersection buildIntersection(final WritableMemory dstMem) {
    return (Intersection) build(Family.INTERSECTION, dstMem);
  }

  /**
   * Convenience method, returns a configured SetOperation ANotB with
   * <a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">Default Update Seed</a>
   * @return an ANotB object
   */
  public AnotB buildANotB() {
    return (AnotB) build(Family.A_NOT_B);
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
    final String mrsStr = bMemReqSvr.getClass().getSimpleName();
    sb.append("MemoryRequestServer:").append(TAB).append(mrsStr).append(LS);
    return sb.toString();
  }

}
