/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.DEFAULT_NOMINAL_ENTRIES;
import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.Util.ceilingPowerOf2;
import static com.yahoo.sketches.Util.LS;
import static com.yahoo.sketches.Util.TAB;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.memory.Memory;

/**
 * For building a new UpdateSketch.
 * 
 * @author Lee Rhodes 
 */
public class UpdateSketchBuilder {
  private int bLgNomLongs;
  private long bSeed;
  private ResizeFactor bRF;
  private Family bFam;
  private float bP;
  private Memory bDstMem;
  
  /**
   * Constructor for building a new UpdateSketch. The default configuration is 
   * <ul>
   * <li>Nominal Entries: {@value com.yahoo.sketches.Util#DEFAULT_NOMINAL_ENTRIES}</li>
   * <li>Seed: {@value com.yahoo.sketches.Util#DEFAULT_UPDATE_SEED}</li>
   * <li>Resize Factor: The default for sketches on the Java heap is 
   * {@link com.yahoo.sketches.ResizeFactor#X8}.
   * For direct sketches, which are targeted for native memory off the Java heap, this value will
   * be fixed at either {@link com.yahoo.sketches.ResizeFactor#X1} or 
   * {@link com.yahoo.sketches.ResizeFactor#X2}.</li>
   * <li>{@link com.yahoo.sketches.Family#QUICKSELECT}</li>
   * <li>Input Sampling Probability: 1.0</li>
   * <li>Memory: null</li>
   * </ul>
   */
  public UpdateSketchBuilder() {
    bLgNomLongs = Integer.numberOfTrailingZeros(DEFAULT_NOMINAL_ENTRIES);
    bSeed = DEFAULT_UPDATE_SEED;
    bP = (float) 1.0;
    bRF = ResizeFactor.X8;
    bFam = Family.QUICKSELECT;
    bDstMem = null;
  }
  
  /**
   * Sets the Nominal Entries for this sketch.
   * @param nomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entres</a>
   * This will become the ceiling power of 2 if it is not.
   * @return this UpdateSketchBuilder
   */
  public UpdateSketchBuilder setNominalEntries(int nomEntries) {
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
   * Sets the long seed value that is required by the hashing function.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return this UpdateSketchBuilder
   */
  public UpdateSketchBuilder setSeed(long seed) {
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
   * Sets the upfront uniform sampling probability, <i>p</i>
   * @param p <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @return this UpdateSketchBuilder
   */
  public UpdateSketchBuilder setP(float p) {
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
   * Sets the cache Resize Factor.
   * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @return this UpdateSketchBuilder
   */
  public UpdateSketchBuilder setResizeFactor(ResizeFactor rf) {
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
   * Set the Family.  
   * @param family the family for this builder
   * @return this UpdateSketchBuilder
   */
  public UpdateSketchBuilder setFamily(Family family) {
    this.bFam = family;
    return this;
  }
  
  /**
   * Returns the Family
   * @return the Family
   */
  public Family getFamily() {
    return bFam;
  }
  
  /**
   * Initialize the specified backing destination Memory store.  
   * Note: this cannot be used with the Alpha Family of sketches.
   * @param dstMem  The destination Memory. 
   * <a href="{@docRoot}/resources/dictionary.html#dstMem">See Destination Memory</a>.
   * @return this UpdateSketchBuilder
   */
  public UpdateSketchBuilder initMemory(Memory dstMem) {
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
   * Returns an UpdateSketch with the current configuration of this Builder.
   * @return an UpdateSketch
   */
  public UpdateSketch build() {
    UpdateSketch sketch = null;
    switch (bFam) {
      case ALPHA: {
        if (bDstMem == null) {
          sketch = HeapAlphaSketch.getInstance(bLgNomLongs, bSeed, bP, bRF);
        } 
        else {
          throw new IllegalArgumentException("AlphaSketch cannot be made Direct to Memory.");
        }
        break;
      }
      case QUICKSELECT: {
        if (bDstMem == null) {
          sketch = HeapQuickSelectSketch.getInstance(bLgNomLongs, bSeed, bP, bRF, false);
        } 
        else {
          sketch = DirectQuickSelectSketch.getInstance(bLgNomLongs, bSeed, bP, bRF, bDstMem, false);
        }
        break;
      }
      default: {
        throw new IllegalArgumentException(
          "Given Family cannot be built as a Theta Sketch: "+bFam.toString());
      }
    }
    return sketch;
  }
  
  /**
   * Returns an UpdateSketch with the current configuration of this Builder and the given
   * <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entres</a>.
   * @param nomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entres</a>
   * This will become the ceiling power of 2 if it is not.
   * @return an UpdateSketch
   */
  public UpdateSketch build(int nomEntries) {
    bLgNomLongs = Integer.numberOfTrailingZeros(ceilingPowerOf2(nomEntries));
    return build();
  }  
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("UpdateSketchBuilder configuration:").append(LS).
       append("LgK:").append(TAB).append(bLgNomLongs).append(LS).
       append("K:").append(TAB).append(1 << bLgNomLongs).append(LS).
       append("Seed:").append(TAB).append(bSeed).append(LS).
       append("p:").append(TAB).append(bP).append(LS).
       append("ResizeFactor:").append(TAB).append(bRF).append(LS).
       append("Family:").append(TAB).append(bFam).append(LS).
       append("DstMemory:").append(TAB).append(bDstMem != null).append(LS);
    return sb.toString();
  }
  
}
