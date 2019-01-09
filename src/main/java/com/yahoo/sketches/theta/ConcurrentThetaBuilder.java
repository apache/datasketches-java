/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
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
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesStateException;

/**
 * For building concurrent buffers and shared theta sketch
 *
 * @author Lee Rhodes
 */
public class ConcurrentThetaBuilder {
  private int bSharedLgNomLongs;
  private int bLocalLgNomLongs;
  private long bSeed;
  private int bCacheLimit;
  private boolean bPropagateOrderedCompact;
  private ConcurrentSharedThetaSketch bShared;
  private boolean bSharedIsDirect;

  /**
   * Constructor for building concurrent buffers and the shared theta sketch.
   * The shared theta sketch must be built first.
   */
  public ConcurrentThetaBuilder() {
    bSharedLgNomLongs = Integer.numberOfTrailingZeros(DEFAULT_NOMINAL_ENTRIES);
    bLocalLgNomLongs = 4; //default is smallest legal QS sketch
    bSeed = DEFAULT_UPDATE_SEED;
    bCacheLimit = 0;
    bPropagateOrderedCompact = true;
    bShared = null;
    bSharedIsDirect = false;
  }

  /**
   * Returns a ConcurrentHeapThetaBuffer with the current configuration of this Builder,
   * which must include a valid ConcurrentDirectThetaSketch.
   * The relevant parameters are:
   * <ul><li>Local Nominal Entries</li>
   * <li>seed</li>
   * <li>Cache Limit</li>
   * <li>Propagate Compact</li>
   * </ul>
   *
   * @return an ConcurrentHeapThetaBuffer
   */
  public ConcurrentHeapThetaBuffer build() {
    if (bShared == null) {
      throw new SketchesStateException("The ConcurrentDirectThetaSketch must be built first.");
    }
    return new ConcurrentHeapThetaBuffer(bLocalLgNomLongs, bSeed, bCacheLimit, bShared,
        bPropagateOrderedCompact);
  }

  /**
   * Returns a ConcurrentDirectThetaSketch with the current configuration of the Builder
   * and the given destination WritableMemory.
   * The relevant parameters are:
   * <ul><li>Shared Nominal Entries</li>
   * <li>seed</li>
   * <li>Pool Threads</li>
   * <li>Destination Writable Memory</li>
   * </ul>
   *
   * @param dstMem the given WritableMemory
   * @return a ConcurrentDirectThetaSketch with the current configuration of the Builder
   * and the given destination WritableMemory.
   */
  public ConcurrentSharedThetaSketch build(final WritableMemory dstMem) {
    if (bSharedIsDirect) {
      if (dstMem == null) {
        throw new SketchesArgumentException("Destination WritableMemory cannot be null.");
      }
      bShared = new ConcurrentDirectThetaSketch(bSharedLgNomLongs, bSeed, dstMem);
    } else {
      bShared = new ConcurrentHeapQuickSelectSketch(bSharedLgNomLongs, bSeed);
    }
    return bShared;
  }

  /**
   * Sets the Nominal Entries for the concurrent shared sketch. The minimum value is 16 and the
   * maximum value is 67,108,864, which is 2^26. Be aware that sketches as large as this maximum
   * value have not been thoroughly tested or characterized for performance.
   *
   * @param nomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entries</a>
   *                   This will become the ceiling power of 2 if it is not.
   * @return this ConcurrentThetaBuilder
   */
  public ConcurrentThetaBuilder setSharedNominalEntries(final int nomEntries) {
    bSharedLgNomLongs = Integer.numberOfTrailingZeros(ceilingPowerOf2(nomEntries));
    if ((bSharedLgNomLongs > MAX_LG_NOM_LONGS) || (bSharedLgNomLongs < MIN_LG_NOM_LONGS)) {
      throw new SketchesArgumentException(
          "Nominal Entries must be >= 16 and <= 67108864: " + nomEntries);
    }
    return this;
  }

  /**
   * Sets the Log Nominal Entries for the concurrent shared sketch. The minimum value is 4 and the
   * maximum value is 26. Be aware that sketches as large as this maximum
   * value have not been thoroughly tested or characterized for performance.
   *
   * @param lgNomEntries the Log Nominal Entries for the concurrent shared sketch
   * @return this ConcurrentThetaBuilder
   */
  public ConcurrentThetaBuilder setSharedLogNominalEntries(final int lgNomEntries) {
    bSharedLgNomLongs = lgNomEntries;
    if ((bSharedLgNomLongs > MAX_LG_NOM_LONGS) || (bSharedLgNomLongs < MIN_LG_NOM_LONGS)) {
      throw new SketchesArgumentException(
          "Log Nominal Entries must be >= 4 and <= 26: " + lgNomEntries);
    }
    return this;
  }

  /**
   * Returns Log-base 2 Nominal Entries for the shared sketch
   *
   * @return Log-base 2 Nominal Entries for the shared sketch
   */
  public int getSharedLgNominalEntries() {
    return bSharedLgNomLongs;
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
  public ConcurrentThetaBuilder setLocalNominalEntries(final int nomEntries) {
    bLocalLgNomLongs = Integer.numberOfTrailingZeros(ceilingPowerOf2(nomEntries));
    if ((bLocalLgNomLongs > MAX_LG_NOM_LONGS) || (bSharedLgNomLongs < MIN_LG_NOM_LONGS)) {
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
  public ConcurrentThetaBuilder setLocalLogNominalEntries(final int lgNomEntries) {
    bLocalLgNomLongs = lgNomEntries;
    if ((bLocalLgNomLongs > MAX_LG_NOM_LONGS) || (bLocalLgNomLongs < MIN_LG_NOM_LONGS)) {
      throw new SketchesArgumentException(
          "Log Nominal Entries must be >= 4 and <= 26: " + lgNomEntries);
    }
    return this;
  }

  /**
   * Returns Log-base 2 Nominal Entries for the concurrent local sketch
   *
   * @return Log-base 2 Nominal Entries for the concurrent local sketch
   */
  public int getLocalLgNominalEntries() {
    return bLocalLgNomLongs;
  }

  /**
   * Sets the long seed value that is required by the hashing function.
   *
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return this ConcurrentThetaBuilder
   */
  public ConcurrentThetaBuilder setSeed(final long seed) {
    bSeed = seed;
    return this;
  }

  /**
   * Returns the seed
   *
   * @return the seed
   */
  public long getSeed() {
    return bSeed;
  }

  /**
   * Sets the cache limit size for the ConcurrentHeapThetaBuffer.
   *
   * @param cacheLimit the given cacheLimit. The default is zero.
   * @return this ConcurrentThetaBuilder
   */
  public ConcurrentThetaBuilder setCacheLimit(final int cacheLimit) {
    bCacheLimit = cacheLimit;
    return this;
  }

  /**
   * Gets the cache limit size for the ConcurrentHeapThetaBuffer.
   *
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
  public ConcurrentThetaBuilder setPropagateOrderedCompact(final boolean prop) {
    bPropagateOrderedCompact = prop;
    return this;
  }

  /**
   * Gets the Propagate Ordered Compact flag
   *
   * @return the Propagate Ordered Compact flag
   */
  public boolean getPropagateOrderedCompact() {
    return bPropagateOrderedCompact;
  }

  /**
   * Gets the shared ConcurrentDirectThetaSketch or null if not set.
   *
   * @return the shared ConcurrentDirectThetaSketch or null if not set.
   */
  public ConcurrentSharedThetaSketch getSharedSketch() {
    return bShared;
  }

  public ConcurrentThetaBuilder setSharedIsDirect(final boolean isDirect) {
    bSharedIsDirect = isDirect;
    return this;
  }

  @Override public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("ConcurrentThetaBuilder configuration:").append(LS);
    sb.append("LgK:").append(TAB).append(bSharedLgNomLongs).append(LS);
    sb.append("K:").append(TAB).append(1 << bSharedLgNomLongs).append(LS);
    sb.append("Seed:").append(TAB).append(bSeed).append(LS);
    sb.append("Cache Limit:").append(TAB).append(bCacheLimit).append(LS);
    sb.append("Propagate Ordered Compact").append(TAB).append(bPropagateOrderedCompact).append(LS);
    final String str = (bShared != null) ? bShared.getClass().getSimpleName() : "null";
    sb.append("Shared Sketch:").append(TAB).append(str).append(LS);
    sb.append("Shared is direct:").append(TAB).append(bSharedIsDirect).append(LS);
    return sb.toString();
  }

}
