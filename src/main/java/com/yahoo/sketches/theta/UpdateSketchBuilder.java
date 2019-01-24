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
import com.yahoo.sketches.SketchesStateException;

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
  private MemoryRequestServer bMemReqSvr;

  //Fields for concurrent theta sketch
  private int bNumPoolThreads;
  private int bLocalLgNomLongs;
  private int bCacheLimit;
  private boolean bPropagateOrderedCompact;


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
    bMemReqSvr = new DefaultMemoryRequestServer();
    // Default values for concurrent sketch
    bNumPoolThreads = ConcurrentPropagationService.NUM_POOL_THREADS;
    bLocalLgNomLongs = 4; //default is smallest legal QS sketch
    bCacheLimit = 0;
    bPropagateOrderedCompact = true;
  }

  /**
   * Sets the Nominal Entries for this sketch. The minimum value is 16 and the maximum value is
   * 67,108,864, which is 2^26. Be aware that sketches as large as this maximum value have not
   * been thoroughly tested or characterized for performance.
   * @param nomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entries</a>
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
   * Sets the Log Nominal Entries for this sketch. The minimum value is 4 and the
   * maximum value is 26. Be aware that sketches as large as this maximum
   * value have not been thoroughly tested or characterized for performance.
   *
   * @param lgNomEntries the Log Nominal Entries for the concurrent shared sketch
   * @return this ConcurrentThetaBuilder
   */
  public UpdateSketchBuilder setSharedLogNominalEntries(final int lgNomEntries) {
    bLgNomLongs = lgNomEntries;
    if ((bLgNomLongs > MAX_LG_NOM_LONGS) || (bLgNomLongs < MIN_LG_NOM_LONGS)) {
      throw new SketchesArgumentException(
          "Log Nominal Entries must be >= 4 and <= 26: " + lgNomEntries);
    }
    return this;
  }

  public void setbNumPoolThreads(int bNumPoolThreads) {
    this.bNumPoolThreads = bNumPoolThreads;
  }

  /**
   * Sets the Nominal Entries for the concurrent local sketch. The minimum value is 16 and the
   * maximum value is 67,108,864, which is 2^26. Be aware that sketches as large as this maximum
   * value have not been thoroughly tested or characterized for performance.
   *
   * @param nomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entries</a>
   *                   This will become the ceiling power of 2 if it is not.
   * @return this ConcurrentThetaBuilder
   */
  public UpdateSketchBuilder setLocalNominalEntries(final int nomEntries) {
    bLocalLgNomLongs = Integer.numberOfTrailingZeros(ceilingPowerOf2(nomEntries));
    if ((bLocalLgNomLongs > MAX_LG_NOM_LONGS) || (bLocalLgNomLongs < MIN_LG_NOM_LONGS)) {
      throw new SketchesArgumentException(
          "Nominal Entries must be >= 16 and <= 67108864: " + nomEntries);
    }
    return this;
  }

  /**
   * Sets the Log Nominal Entries for a concurrent local sketch. The minimum value is 4 and the
   * maximum value is 26. Be aware that sketches as large as this maximum
   * value have not been thoroughly tested or characterized for performance.
   *
   * @param lgNomEntries the Log Nominal Entries for a concurrent local sketch
   * @return this ConcurrentThetaBuilder
   */
  public UpdateSketchBuilder setLocalLogNominalEntries(final int lgNomEntries) {
    bLocalLgNomLongs = lgNomEntries;
    if ((bLocalLgNomLongs > MAX_LG_NOM_LONGS) || (bLocalLgNomLongs < MIN_LG_NOM_LONGS)) {
      throw new SketchesArgumentException(
          "Log Nominal Entries must be >= 4 and <= 26: " + lgNomEntries);
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
   * Returns Log-base 2 Nominal Entries for the concurrent local sketch
   * @return Log-base 2 Nominal Entries for the concurrent local sketch
   */
  public int getLocalLgNominalEntries() {
    return bLocalLgNomLongs;
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
    bFam = family;
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
   * Set the MemoryRequestServer
   * @param memReqSvr the given MemoryRequestServer
   * @return this UpdateSketchBuilder
   */
  public UpdateSketchBuilder setMemoryRequestServer(final MemoryRequestServer memReqSvr) {
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
   * Sets the cache limit size for the ConcurrentHeapThetaBuffer.
   *
   * @param cacheLimit the given cacheLimit. The default is zero.
   * @return this ConcurrentThetaBuilder
   */
  public UpdateSketchBuilder setCacheLimit(final int cacheLimit) {
    bCacheLimit = cacheLimit;
    return this;
  }

  /**
   * Gets the cache limit size for the ConcurrentHeapThetaBuffer.
   * @return the cache limit size for the ConcurrentHeapThetaBuffer.
   */
  public int getCacheLimit() {
    return bCacheLimit;
  }

  /**
   * Sets the Propagate Ordered Compact flag to the given value.
   *
   * @param prop the given value
   * @return this ConcurrentThetaBuilder
   */
  public UpdateSketchBuilder setPropagateOrderedCompact(final boolean prop) {
    bPropagateOrderedCompact = prop;
    return this;
  }

  /**
   * Gets the Propagate Ordered Compact flag
   * @return the Propagate Ordered Compact flag
   */
  public boolean getPropagateOrderedCompact() {
    return bPropagateOrderedCompact;
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
          sketch =  new HeapQuickSelectSketch(bLgNomLongs, bSeed, bP, bRF, false);
        }
        else {
          sketch = new DirectQuickSelectSketch(
              bLgNomLongs, bSeed, bP, bRF, bMemReqSvr, dstMem, false);
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

  /**
   * Returns a concurrent UpdateSketch with the current configuration of the Builder
   * and the given destination WritableMemory.
   * The relevant parameters are:
   * <ul><li>Shared Nominal Entries</li>
   * <li>seed</li>
   * <li>Pool Threads</li>
   * <li>Destination Writable Memory</li>
   * </ul>
   *
   * @param dstMem the given WritableMemory
   * @return a concurrent UpdateSketch with the current configuration of the Builder
   * and the given destination WritableMemory.
   */
  public UpdateSketch buildShared(final WritableMemory dstMem) {
    ConcurrentPropagationService.NUM_POOL_THREADS = bNumPoolThreads;
    if (dstMem == null) {
      return new ConcurrentHeapQuickSelectSketch(bLgNomLongs, bSeed);
    } else {
      return new ConcurrentDirectQuickSelectSketch(bLgNomLongs, bSeed, dstMem);
    }
  }

  ConcurrentSharedThetaSketch buildSharedInternal(final WritableMemory dstMem) {
    ConcurrentPropagationService.NUM_POOL_THREADS = bNumPoolThreads;
    if (dstMem == null) {
      return new ConcurrentHeapQuickSelectSketch(bLgNomLongs, bSeed);
    } else {
      return new ConcurrentDirectQuickSelectSketch(bLgNomLongs, bSeed, dstMem);
    }
  }

  /**
   * Returns a ConcurrentHeapThetaBuffer with the current configuration of this Builder
   * The relevant parameters are:
   * <ul><li>Local Nominal Entries</li>
   * <li>seed</li>
   * <li>Cache Limit</li>
   * <li>Propagate Compact</li>
   * </ul>
   *
   * @param shared the shared sketch to be accessed through the local theta buffer
   * @return an ConcurrentHeapThetaBuffer
   */
  public UpdateSketch buildLocal(final UpdateSketch shared) {
    if ((shared == null) || !(shared instanceof ConcurrentSharedThetaSketch)) {
      throw new SketchesStateException("The shared sketch must be built first.");
    }
    final ConcurrentSharedThetaSketch bShared = (ConcurrentSharedThetaSketch)shared;
    return new ConcurrentHeapThetaBuffer(bLocalLgNomLongs, bSeed, bCacheLimit, bShared,
        bPropagateOrderedCompact);
  }

  ConcurrentHeapThetaBuffer buildLocalInternal(final ConcurrentSharedThetaSketch shared) {
    if (shared == null) {
      throw new SketchesStateException("The shared sketch must be built first.");
    }
    return new ConcurrentHeapThetaBuffer(bLocalLgNomLongs, bSeed, bCacheLimit, shared,
        bPropagateOrderedCompact);
  }


  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("UpdateSketchBuilder configuration:").append(LS);
    sb.append("LgK:").append(TAB).append(bLgNomLongs).append(LS);
    sb.append("K:").append(TAB).append(1 << bLgNomLongs).append(LS);
    sb.append("LgB:").append(TAB).append(bLocalLgNomLongs).append(LS);
    sb.append("B:").append(TAB).append(1 << bLocalLgNomLongs).append(LS);
    sb.append("Seed:").append(TAB).append(bSeed).append(LS);
    sb.append("p:").append(TAB).append(bP).append(LS);
    sb.append("ResizeFactor:").append(TAB).append(bRF).append(LS);
    sb.append("Family:").append(TAB).append(bFam).append(LS);
    final String mrsStr = bMemReqSvr.getClass().getSimpleName();
    sb.append("MemoryRequestServer:").append(TAB).append(mrsStr).append(LS);
    sb.append("Cache Limit:").append(TAB).append(bCacheLimit).append(LS);
    sb.append("Propagate Ordered Compact").append(TAB).append(bPropagateOrderedCompact).append(LS);
    return sb.toString();
  }

}
