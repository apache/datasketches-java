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

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;

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

  /**
   * Constructor for building a new UpdateSketch. The default configuration is
   * <ul>
   * <li>Nominal Entries: {@value com.yahoo.sketches.Util#DEFAULT_NOMINAL_ENTRIES}</li>
   * <li>Seed: {@value com.yahoo.sketches.Util#DEFAULT_UPDATE_SEED}</li>
   * <li>Resize Factor: The default for sketches on the Java heap is
   * {@link ResizeFactor#X8}.
   * For direct sketches, which are targeted for native memory off the Java heap, this value will
   * be fixed at either {@link ResizeFactor#X1} or
   * {@link ResizeFactor#X2}.</li>
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
  }

  /**
   * Sets the Nominal Entries for this sketch. The minimum value is 16 and the maximum value is
   * 67,108,864, which is 2^26. Be aware that sketches as large as this maximum value have not
   * been thoroughly tested or characterized for performance.
   * @param nomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entres</a>
   * This will become the ceiling power of 2 if it is not.
   * @return this UpdateSketchBuilder
   */
  public UpdateSketchBuilder setNominalEntries(final int nomEntries) {
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
   * Sets the long seed value that is required by the hashing function.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return this UpdateSketchBuilder
   */
  public UpdateSketchBuilder setSeed(final long seed) {
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
  public UpdateSketchBuilder setP(final float p) {
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
   * Sets the cache Resize Factor.
   * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @return this UpdateSketchBuilder
   */
  public UpdateSketchBuilder setResizeFactor(final ResizeFactor rf) {
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
  public UpdateSketchBuilder setFamily(final Family family) {
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
   * Returns an UpdateSketch with the current configuration of this Builder.
   * @return an UpdateSketch
   */
  public UpdateSketch build() {
    return build(null);
  }

  /**
   * Returns an UpdateSketch with the current configuration of this Builder
   * with the specified backing destination Memory store.
   * Note: this cannot be used with the Alpha Family of sketches.
   * @param dstMem The destination Memory.
   * @return an UpdateSketch
   */
  public UpdateSketch build(final WritableMemory dstMem) {
    UpdateSketch sketch = null;
    switch (bFam) {
      case ALPHA: {
        if (dstMem == null) {
          sketch = HeapAlphaSketch.newHeapInstance(bLgNomLongs, bSeed, bP, bRF);
        }
        else {
          throw new SketchesArgumentException("AlphaSketch cannot be made Direct to Memory.");
        }
        break;
      }
      case QUICKSELECT: {
        if (dstMem == null) {
          sketch = HeapQuickSelectSketch.initNewHeapInstance(bLgNomLongs, bSeed, bP, bRF, false);
        }
        else {
          sketch = DirectQuickSelectSketch.initNewDirectInstance(bLgNomLongs, bSeed, bP, bRF, dstMem, false);
        }
        break;
      }
      default: {
        throw new SketchesArgumentException(
          "Given Family cannot be built as a Theta Sketch: " + bFam.toString());
      }
    }
    return sketch;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("UpdateSketchBuilder configuration:").append(LS)
      .append("LgK:").append(TAB).append(bLgNomLongs).append(LS)
      .append("K:").append(TAB).append(1 << bLgNomLongs).append(LS)
      .append("Seed:").append(TAB).append(bSeed).append(LS)
      .append("p:").append(TAB).append(bP).append(LS)
      .append("ResizeFactor:").append(TAB).append(bRF).append(LS)
      .append("Family:").append(TAB).append(bFam).append(LS);
    return sb.toString();
  }

}
