/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.DEFAULT_NOMINAL_ENTRIES;
import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.Util.LS;
import static com.yahoo.sketches.Util.MAX_LG_NOM_LONGS;
import static com.yahoo.sketches.Util.MIN_LG_NOM_LONGS;
import static com.yahoo.sketches.Util.TAB;
import static com.yahoo.sketches.Util.ceilingPowerOf2;

import com.yahoo.memory.DefaultMemoryRequestServer;
import com.yahoo.memory.MemoryRequestServer;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;

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
   * <li>Nominal Entries: {@value com.yahoo.sketches.Util#DEFAULT_NOMINAL_ENTRIES}</li>
   * <li>Seed: {@value com.yahoo.sketches.Util#DEFAULT_UPDATE_SEED}</li>
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
          setOp = new HeapAnotB(bSeed);
        }
        else {
          throw new SketchesArgumentException(
            "AnotB is a stateless operation and cannot be persisted.");
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
