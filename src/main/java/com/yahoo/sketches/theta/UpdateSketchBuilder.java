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
  private boolean bPropagateOrderedCompact;
  private double bMaxConcurrencyError;
  private int bMaxNumLocalThreads;

  /**
   * Constructor for building a new UpdateSketch. The default configuration is
   * <ul>
   * <li>Nominal Entries: {@value com.yahoo.sketches.Util#DEFAULT_NOMINAL_ENTRIES}</li>
   * <li>Seed: {@value com.yahoo.sketches.Util#DEFAULT_UPDATE_SEED}</li>
   * <li>Input Sampling Probability: 1.0</li>
   * <li>Family: {@link com.yahoo.sketches.Family#QUICKSELECT}</li>
   * <li>Resize Factor: The default for sketches on the Java heap is {@link ResizeFactor#X8}.
   * For direct sketches, which are targeted for native memory off the Java heap, this value will
   * be fixed at either {@link ResizeFactor#X1} or {@link ResizeFactor#X2}.</li>
   * <li>MemoryRequestServer (Direct only): {@link com.yahoo.memory.DefaultMemoryRequestServer}.</li>
   * </ul>
   * Parameters unique to the concurrent sketches only:
   * <ul>
   * <li>Number of local Nominal Entries: 4</li>
   * <li>Concurrent NumPoolThreads: 3</li>
   * <li>Concurrent PropagateOrderedCompact: true</li>
   * <li>Concurrent MaxConcurrencyError: 0</li>
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
    bPropagateOrderedCompact = true;
    bMaxConcurrencyError = 0;
    bMaxNumLocalThreads = 1;
  }

  /**
   * Sets the Nominal Entries for this sketch.
   * This value is also used for building a shared concurrent sketch.
   * The minimum value is 16 (2^4) and the maximum value is 67,108,864 (2^26).
   * Be aware that sketches as large as this maximum value may not have been
   * thoroughly tested or characterized for performance.
   *
   * @param nomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entries</a>
   * This will become the ceiling power of 2 if the given value is not.
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
   * Alternative method of setting the Nominal Entries for this sketch from the log_base2 value.
   * This value is also used for building a shared concurrent sketch.
   * The minimum value is 4 and the maximum value is 26.
   * Be aware that sketches as large as this maximum value may not have been
   * thoroughly tested or characterized for performance.
   *
   * @param lgNomEntries the Log Nominal Entries for the concurrent shared sketch
   * @return this UpdateSketchBuilder
   */
  public UpdateSketchBuilder setLogNominalEntries(final int lgNomEntries) {
    bLgNomLongs = lgNomEntries;
    if ((bLgNomLongs > MAX_LG_NOM_LONGS) || (bLgNomLongs < MIN_LG_NOM_LONGS)) {
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
   * Sets the Nominal Entries for the concurrent local sketch. The minimum value is 16 and the
   * maximum value is 67,108,864, which is 2^26.
   * Be aware that sketches as large as this maximum
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
   * Alternative method of setting the Nominal Entries for a local concurrent sketch from the
   * log_base2 value.
   * The minimum value is 4 and the maximum value is 26.
   * Be aware that sketches as large as this maximum
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
   * Sets the number of pool threads used for background propagation in the concurrent sketches.
   * @param numPoolThreads the given number of pool threads
   */
  public void setNumPoolThreads(final int numPoolThreads) {
    bNumPoolThreads = numPoolThreads;
  }

  /**
   * Gets the number of background pool threads used for propagation in the concurrent sketches.
   * @return the number of background pool threads
   */
  public int getNumPoolThreads() {
    return bNumPoolThreads;
  }

  /**
   * Sets the Propagate Ordered Compact flag to the given value. Used with concurrent sketches.
   *
   * @param prop the given value
   * @return this UpdateSketchBuilder
   */
  public UpdateSketchBuilder setPropagateOrderedCompact(final boolean prop) {
    bPropagateOrderedCompact = prop;
    return this;
  }

  /**
   * Gets the Propagate Ordered Compact flag used with concurrent sketches.
   * @return the Propagate Ordered Compact flag
   */
  public boolean getPropagateOrderedCompact() {
    return bPropagateOrderedCompact;
  }

  /**
   * Sets the Maximum Concurrency Error.
   * @param maxConcurrencyError the given Maximum Concurrency Error.
   */
  public void setMaxConcurrencyError(final double maxConcurrencyError) {
    bMaxConcurrencyError = maxConcurrencyError;
  }

  /**
   * Gets the Maximum Concurrency Error
   * @return the Maximum Concurrency Error
   */
  public double getMaxConcurrencyError() {
    return bMaxConcurrencyError;
  }

  /**
   * Sets the Maximum Number of Local Threads.
   * This is used to set the size of the local concurrent buffers.
   * @param maxNumLocalThreads the given Maximum Number of Local Threads
   */
  public void setMaxNumLocalThreads(final int maxNumLocalThreads) {
    bMaxNumLocalThreads = maxNumLocalThreads;
  }

  /**
   * Gets the Maximum Number of Local Threads.
   * @return the Maximum Number of Local Threads.
   */
  public int getMaxNumLocalThreads() {
    return bMaxNumLocalThreads;
  }

  // BUILD FUNCTIONS

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
   * Returns an on-heap concurrent shared UpdateSketch with the current configuration of the
   * Builder.
   *
   * <p>The parameters unique to the shared concurrent sketch are:
   * <ul>
   * <li>Number of Pool Threads (default is 3)</li>
   * <li>Maximum Concurrency Error</li>
   * </ul>
   *
   * <p>Key parameters that are in common with other <i>Theta</i> sketches:
   * <ul>
   * <li>Nominal Entries or Log Nominal Entries (for the shared concurrent sketch)</li>
   * </ul>
   *
   * @return an on-heap concurrent UpdateSketch with the current configuration of the Builder.
   */
  public UpdateSketch buildShared() {
    return (UpdateSketch) buildSharedInternal(null);
  }

  /**
   * Returns a direct (potentially off-heap) concurrent shared UpdateSketch with the current
   * configuration of the Builder and the given destination WritableMemory.
   *
   * <p>The parameters unique to the shared concurrent sketch are:
   * <ul>
   * <li>Number of Pool Threads (default is 3)</li>
   * <li>Maximum Concurrency Error</li>
   * </ul>
   *
   * <p>Key parameters that are in common with other <i>Theta</i> sketches:
   * <ul>
   * <li>Nominal Entries or Log Nominal Entries (for the shared concurrent sketch)</li>
   * <li>Destination Writable Memory (if not null, returned sketch is Direct. Default is null.)</li>
   * </ul>
   *
   * @param dstMem the given WritableMemory for Direct, otherwise <i>null</i>.
   * @return a concurrent UpdateSketch with the current configuration of the Builder
   * and the given destination WritableMemory.
   */
  public UpdateSketch buildShared(final WritableMemory dstMem) {
    return (UpdateSketch) buildSharedInternal(dstMem);
  }

  public UpdateSketch buildSharedFromSketch(final UpdateSketch sketch, final WritableMemory dstMem) {
    return (UpdateSketch) buildSharedFromSketchInternal(sketch, dstMem);
  }

  private ConcurrentSharedThetaSketch buildSharedInternal(final WritableMemory dstMem) {
    ConcurrentPropagationService.NUM_POOL_THREADS = bNumPoolThreads;
    if (dstMem == null) {
      return new ConcurrentHeapQuickSelectSketch(bLgNomLongs, bSeed, bMaxConcurrencyError);
    } else {
      return new ConcurrentDirectQuickSelectSketch(bLgNomLongs, bSeed, bMaxConcurrencyError, dstMem);
    }
  }

  private ConcurrentSharedThetaSketch buildSharedFromSketchInternal(
      final UpdateSketch sketch, final WritableMemory dstMem) {
    ConcurrentPropagationService.NUM_POOL_THREADS = bNumPoolThreads;
    if (sketch instanceof HeapQuickSelectSketch) {
      return new ConcurrentHeapQuickSelectSketch(
          (HeapQuickSelectSketch)sketch, bSeed, bMaxConcurrencyError);
    }
    if (sketch instanceof DirectQuickSelectSketch) {
      return new ConcurrentDirectQuickSelectSketch((DirectQuickSelectSketch)sketch, bSeed,
          bMaxConcurrencyError, dstMem);
    }
    throw new SketchesArgumentException("sketch type not supported.");
  }

  /**
   * Returns a local concurrent UpdateSketch to be used as a per-thread local buffer along with the
   * given concurrent shared UpdateSketch and the current configuration of this Builder
   *
   * <p>The parameters unique to the local concurrent sketch are:
   * <ul>
   * <li>Local Nominal Entries or Local Log Nominal Entries</li>
   * <li>Propagate Ordered Compact flag</li>
   * </ul>
   *
   * @param shared the concurrent shared sketch to be accessed via the concurrent local sketch.
   * @return an UpdateSketch to be used as a per-thread local buffer.
   */
  public UpdateSketch buildLocal(final UpdateSketch shared) {
    if ((shared == null) || !(shared instanceof ConcurrentSharedThetaSketch)) {
      throw new SketchesStateException("The concurrent shared sketch must be built first.");
    }
    return new ConcurrentHeapThetaBuffer(bLocalLgNomLongs, bSeed,
        (ConcurrentSharedThetaSketch) shared, bPropagateOrderedCompact, bMaxNumLocalThreads);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("UpdateSketchBuilder configuration:").append(LS);
    sb.append("LgK:").append(TAB).append(bLgNomLongs).append(LS);
    sb.append("K:").append(TAB).append(1 << bLgNomLongs).append(LS);
    sb.append("LgLocalK:").append(TAB).append(bLocalLgNomLongs).append(LS);
    sb.append("LocalK:").append(TAB).append(1 << bLocalLgNomLongs).append(LS);
    sb.append("Seed:").append(TAB).append(bSeed).append(LS);
    sb.append("p:").append(TAB).append(bP).append(LS);
    sb.append("ResizeFactor:").append(TAB).append(bRF).append(LS);
    sb.append("Family:").append(TAB).append(bFam).append(LS);
    final String mrsStr = bMemReqSvr.getClass().getSimpleName();
    sb.append("MemoryRequestServer:").append(TAB).append(mrsStr).append(LS);
    sb.append("Propagate Ordered Compact").append(TAB).append(bPropagateOrderedCompact).append(LS);
    sb.append("NumPoolThreads").append(TAB).append(bNumPoolThreads).append(LS);
    sb.append("MaxConcurrencyError").append(TAB).append(bMaxConcurrencyError).append(LS);
    sb.append("MaxNumLocalThreads").append(TAB).append(bMaxNumLocalThreads).append(LS);
    return sb.toString();
  }

}
