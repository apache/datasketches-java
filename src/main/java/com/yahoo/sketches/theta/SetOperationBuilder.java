/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.DEFAULT_NOMINAL_ENTRIES;
import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.Util.LS;
import static com.yahoo.sketches.Util.TAB;
import static com.yahoo.sketches.Util.ceilingPowerOf2;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.memory.Memory;

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
  private Memory bDstMem;
  
  /**
   * Constructor for building a new SetOperation.  The default configuration is 
   * <ul>
   * <li>Nominal Entries: {@value com.yahoo.sketches.Util#DEFAULT_NOMINAL_ENTRIES}</li>
   * <li>Seed: {@value com.yahoo.sketches.Util#DEFAULT_UPDATE_SEED}</li>
   * <li>{@link com.yahoo.sketches.ResizeFactor#X8}</li>
   * <li>Input Sampling Probability: 1.0</li>
   * <li>Memory: null</li>
   * </ul>
   */
  public SetOperationBuilder() {
    bLgNomLongs = Integer.numberOfTrailingZeros(DEFAULT_NOMINAL_ENTRIES);
    bSeed = DEFAULT_UPDATE_SEED;
    bP = (float) 1.0;
    bRF = ResizeFactor.X8;
    bDstMem = null;
  }
  
  /**
   * Sets the Nominal Entries for this set operation.
   * @param nomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entres</a>
   * This will become the ceiling power of 2 if it is not.
   * @return this SetOperationBuilder
   */
  public SetOperationBuilder setNominalEntries(int nomEntries) {
    bLgNomLongs = Integer.numberOfTrailingZeros(ceilingPowerOf2(nomEntries));
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
  public SetOperationBuilder setSeed(long seed) {
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
  public SetOperationBuilder setP(float p) {
    if ((p <= 0.0) || (p > 1.0)) {
      throw new IllegalArgumentException("p must be > 0 and <= 1.0: "+p);
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
  public SetOperationBuilder setResizeFactor(ResizeFactor rf) {
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
   * Initializes the backing Memory store. 
   * @param dstMem  The destination Memory. 
   * <a href="{@docRoot}/resources/dictionary.html#dstMem">See Destination Memory</a>
   * @return this SetOperationBuilder
   */
  public SetOperationBuilder initMemory(Memory dstMem) {
    bDstMem = dstMem;
    return this;
  }
  
  /**
   * Returns the Destination Memory
   * <a href="{@docRoot}/resources/dictionary.html#dstMem">See Destination Memory</a>.
   * @return the Destination Memory
   */
  public Memory getMemory() {
    return bDstMem;
  }
  
  /**
   * Returns a SetOperation with the current configuration of this Builder and the given Family.
   * @param family the chosen SetOperation family
   * @return a SetOperation
   */
  public SetOperation build(Family family) {
    SetOperation setOp = null;
    switch (family) {
      case UNION: {
        if (bDstMem == null) {
          setOp = UnionImpl.initNewHeapInstance(bLgNomLongs, bSeed, bP, bRF);
        } 
        else {
          setOp = UnionImpl.initNewDirectInstance(bLgNomLongs, bSeed, bP, bRF, bDstMem);
        }
        break;
      }
      case INTERSECTION: {
        if (bDstMem == null) {
          setOp = new HeapIntersection(bSeed);
        } 
        else {
          setOp = new DirectIntersection(bSeed, bDstMem);
        }
        break;
      }
      case A_NOT_B: {
        if (bDstMem == null) {
          setOp = new HeapAnotB(bSeed);
        } 
        else throw new IllegalArgumentException(
            "AnotB is a stateless operation and cannot be persisted.");
        break;
      }
      default: 
        throw new IllegalArgumentException(
            "Given Family cannot be built as a SetOperation: "+family.toString());
    }
    return setOp;
  }  
  
  /**
   * Returns a SetOperation with the current configuration of this Builder and the given
   * <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entries</a> and Family.
   * @param nomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entres</a>
   * This will become the ceiling power of 2 if it is not.
   * @param family build this SetOperation family
   * @return a SetOperation
   */
  public SetOperation build(int nomEntries, Family family) {
    bLgNomLongs = Integer.numberOfTrailingZeros(ceilingPowerOf2(nomEntries));
    return build(family);
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
   * Convenience method, returns a configured SetOperation Union with the given
   * <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entries</a>.
   * @param nomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entres</a>
   * @return a Union object
   */
  public Union buildUnion(int nomEntries) {
    return (Union) build(nomEntries, Family.UNION);
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
   * Convenience method, returns a configured SetOperation ANotB with
   * <a href="{@docRoot}/resources/dictionary.html#defaultNomEntries">Default Nominal Entries</a>
   * @return an ANotB object
   */
  public AnotB buildANotB() {
    return (AnotB) build(Family.A_NOT_B);
  }

  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("SetOperationBuilder configuration:").append(LS).
       append("LgK:").append(TAB).append(bLgNomLongs).append(LS).
       append("K:").append(TAB).append(1 << bLgNomLongs).append(LS).
       append("Seed:").append(TAB).append(bSeed).append(LS).
       append("p:").append(TAB).append(bP).append(LS).
       append("ResizeFactor:").append(TAB).append(bRF).append(LS).
       append("DstMemory:").append(TAB).append(bDstMem != null).append(LS);
    return sb.toString();
  }
  
}
