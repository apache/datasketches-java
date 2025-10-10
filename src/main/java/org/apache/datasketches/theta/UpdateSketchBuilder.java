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
import org.apache.datasketches.common.MemorySegmentRequest;
import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesStateException;
import org.apache.datasketches.common.SuppressFBWarnings;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.thetacommon.ThetaUtil;

/**
 * For building a new UpdatableThetaSketch.
 *
 * @author Lee Rhodes
 */
public final class UpdateSketchBuilder {
  private int bLgNomLongs;
  private long bSeed;
  private ResizeFactor bRF;
  private Family bFam;
  private float bP;
  private MemorySegmentRequest bMemorySegmentRequest;

  //Fields for concurrent theta sketch
  private int bNumPoolThreads;
  private int bConCurLgNomLongs;
  private boolean bPropagateOrderedCompact;
  private double bMaxConcurrencyError;
  private int bMaxNumLocalThreads;

  /**
   * Constructor for building a new UpdatableThetaSketch. The default configuration is
   * <ul>
   * <li>Nominal Entries: {@value org.apache.datasketches.thetacommon.ThetaUtil#DEFAULT_NOMINAL_ENTRIES}</li>
   * <li>Seed: {@value org.apache.datasketches.common.Util#DEFAULT_UPDATE_SEED}</li>
   * <li>Resize Factor: The default for sketches on the Java heap is {@link ResizeFactor#X8}.
   * For direct sketches, which are targeted for off-heap, this value will
   * be fixed at either {@link ResizeFactor#X1} or {@link ResizeFactor#X2}.</li>
   * <li>Family: {@link org.apache.datasketches.common.Family#QUICKSELECT}</li>
   * <li>Input Sampling Probability, p: 1.0</li>
   * <li>MemorySegmentRequest implementation: null</li>
   * </ul>
   * Parameters unique to the concurrent sketches only:
   * <ul>
   * <li>Concurrent NumPoolThreads: 3</li>
   * <li>Number of local Nominal Entries: 4</li>
   * <li>Concurrent PropagateOrderedCompact: true</li>
   * <li>Concurrent MaxConcurrencyError: 0</li>
   * <li>Concurrent MaxNumLocalThreads: 1</li>
   * </ul>
   */
  public UpdateSketchBuilder() {
    bLgNomLongs = Integer.numberOfTrailingZeros(ThetaUtil.DEFAULT_NOMINAL_ENTRIES);
    bSeed = Util.DEFAULT_UPDATE_SEED;
    bRF = ResizeFactor.X8;
    bFam = Family.QUICKSELECT;
    bP = (float) 1.0;
    bMemorySegmentRequest = null;

    // Default values for concurrent sketch
    bNumPoolThreads = ConcurrentPropagationService.NUM_POOL_THREADS;
    bConCurLgNomLongs = 4; //default is smallest legal QS sketch
    bPropagateOrderedCompact = true;
    bMaxConcurrencyError = 0;
    bMaxNumLocalThreads = 1;
  }

  /**
   * Sets the local Nominal Entries for this builder.
   * This value is also used for building a shared concurrent sketch.
   * The minimum value is 16 (2^4) and the maximum value is 67,108,864 (2^26).
   * Be aware that sketches as large as this maximum value may not have been
   * thoroughly tested or characterized for performance.
   *
   * @param nomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entries</a>
   * This will become the ceiling power of 2 if the given value is not.
   * @return this UpdatableThetaSketchBuilder
   */
  public UpdateSketchBuilder setNominalEntries(final int nomEntries) {
    bLgNomLongs = ThetaUtil.checkNomLongs(nomEntries);
    return this;
  }

  /**
   * Alternative method of setting the local Nominal Entries for this builder from the log_base2 value.
   * This value is also used for building a shared concurrent sketch.
   * The minimum value is 4 and the maximum value is 26.
   * Be aware that sketches as large as this maximum value may not have been
   * thoroughly characterized for performance.
   *
   * @param lgNomEntries the Log Nominal Entries. Also for the concurrent shared sketch
   * @return this UpdatableThetaSketchBuilder
   */
  public UpdateSketchBuilder setLogNominalEntries(final int lgNomEntries) {
    bLgNomLongs = ThetaUtil.checkNomLongs(1 << lgNomEntries);
    return this;
  }

  /**
   * Alternative method of setting the Nominal Entries for this builder from the log_base2 value, commonly called LgK.
   * This value is also used for building a shared concurrent sketch.
   * The minimum value is 4 and the maximum value is 26.
   * Be aware that sketches as large as 26 may not have been
   * thoroughly characterized for performance.
   *
   * @param lgK the Log Nominal Entries. Also for the concurrent shared sketch.
   * @return this UpdatableThetaSketchBuilder
   */
  public UpdateSketchBuilder setLgK(final int lgK) {
    bLgNomLongs = ThetaUtil.checkNomLongs(1 << lgK);
    return this;
  }

  /**
   * Returns the local Log-base 2 Nominal Entries
   * @return Log-base 2 Nominal Entries
   */
  public int getLgNominalEntries() {
    return bLgNomLongs;
  }

  /**
   * Sets the local (default) Concurrent Nominal Entries for the concurrent local sketch. The minimum value is 16 and the
   * maximum value is 67,108,864, which is 2^26.
   * Be aware that sketches as large as this maximum
   * value have not been thoroughly tested or characterized for performance.
   *
   * @param nomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entries</a>
   *                   This will become the ceiling power of 2 if it is not.
   * @return this UpdatableThetaSketchBuilder
   */
  public UpdateSketchBuilder setConCurNominalEntries(final int nomEntries) {
    bConCurLgNomLongs = Integer.numberOfTrailingZeros(ceilingPowerOf2(nomEntries));
    if (bConCurLgNomLongs > ThetaUtil.MAX_LG_NOM_LONGS || bConCurLgNomLongs < ThetaUtil.MIN_LG_NOM_LONGS) {
      throw new SketchesArgumentException(
          "Nominal Entries must be >= 16 and <= 67108864: " + nomEntries);
    }
    return this;
  }

  /**
   * Alternative method of setting the local (default) Nominal Entries for a local concurrent sketch from the log_base2 value.
   * The minimum value is 4 and the maximum value is 26.
   * Be aware that sketches as large as this maximum
   * value have not been thoroughly tested or characterized for performance.
   *
   * @param lgNomEntries the Log Nominal Entries for a concurrent local sketch
   * @return this UpdatableThetaSketchBuilder
   */
  public UpdateSketchBuilder setConCurLogNominalEntries(final int lgNomEntries) {
    bConCurLgNomLongs = lgNomEntries;
    if (bConCurLgNomLongs > ThetaUtil.MAX_LG_NOM_LONGS || bConCurLgNomLongs < ThetaUtil.MIN_LG_NOM_LONGS) {
      throw new SketchesArgumentException(
          "Log Nominal Entries must be >= 4 and <= 26: " + lgNomEntries);
    }
    return this;
  }

  /**
   * Returns local Log-base 2 Nominal Entries for the concurrent local sketch
   * @return Log-base 2 Nominal Entries for the concurrent local sketch
   */
  public int getConCurLgNominalEntries() {
    return bConCurLgNomLongs;
  }

  /**
   * Sets the local long seed value that is required by the hashing function.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return this UpdatableThetaSketchBuilder
   */
  public UpdateSketchBuilder setSeed(final long seed) {
    bSeed = seed;
    return this;
  }

  /**
   * Returns the local long seed value that is required by the hashing function.
   * @return the seed
   */
  public long getSeed() {
    return bSeed;
  }

  /**
   * Sets the local upfront uniform pre-sampling probability, <i>p</i>
   * @param p <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @return this UpdatableThetaSketchBuilder
   */
  public UpdateSketchBuilder setP(final float p) {
    if (p <= 0.0 || p > 1.0) {
      throw new SketchesArgumentException("p must be > 0 and <= 1.0: " + p);
    }
    bP = p;
    return this;
  }

  /**
   * Returns the local upfront uniform pre-sampling probability <i>p</i>
   * @return the pre-sampling probability <i>p</i>
   */
  public float getP() {
    return bP;
  }

  /**
   * Sets the local cache Resize Factor.
   * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @return this UpdatableThetaSketchBuilder
   */
  public UpdateSketchBuilder setResizeFactor(final ResizeFactor rf) {
    bRF = rf;
    return this;
  }

  /**
   * Returns the local Resize Factor
   * @return the Resize Factor
   */
  public ResizeFactor getResizeFactor() {
    return bRF;
  }

  /**
   * Set the local Family. Choose either Family.ALPHA or Family.QUICKSELECT.
   * @param family the family for this builder
   * @return this UpdatableThetaSketchBuilder
   */
  public UpdateSketchBuilder setFamily(final Family family) {
    bFam = family;
    return this;
  }

  /**
   * Returns the local Family
   * @return the Family
   */
  public Family getFamily() {
    return bFam;
  }

  /**
   * Sets the local MemorySegmentRequest
   * @param mSegReq the given MemorySegmentRequest
   * @return this UpdatableThetaSketchBuilder
   */
  public UpdateSketchBuilder setMemorySegmentRequest(final MemorySegmentRequest mSegReq) {
    bMemorySegmentRequest = mSegReq;
    return this;
  }

  /**
   * Returns the local MemorySegmentRequest
   * @return the local MemorySegmentRequest
   */
  public MemorySegmentRequest getMemorySegmentRequest() {
    return bMemorySegmentRequest;
  }

  //Concurrent related

  /**
   * Sets the local number of pool threads used for background propagation in the concurrent sketches.
   * @param numPoolThreads the given number of pool threads
   */
  public void setNumPoolThreads(final int numPoolThreads) {
    bNumPoolThreads = numPoolThreads;
  }

  /**
   * Gets the local number of background pool threads used for propagation in the concurrent sketches.
   * @return the number of background pool threads
   */
  public int getNumPoolThreads() {
    return bNumPoolThreads;
  }

  /**
   * Sets the local Propagate Ordered Compact flag to the given value. Used with concurrent sketches.
   *
   * @param prop the given value
   * @return this UpdatableThetaSketchBuilder
   */
  public UpdateSketchBuilder setPropagateOrderedCompact(final boolean prop) {
    bPropagateOrderedCompact = prop;
    return this;
  }

  /**
   * Gets the local Propagate Ordered Compact flag used with concurrent sketches.
   * @return the Propagate Ordered Compact flag
   */
  public boolean getPropagateOrderedCompact() {
    return bPropagateOrderedCompact;
  }

  /**
   * Sets the local Maximum Concurrency Error.
   * @param maxConcurrencyError the given Maximum Concurrency Error.
   */
  public void setMaxConcurrencyError(final double maxConcurrencyError) {
    bMaxConcurrencyError = maxConcurrencyError;
  }

  /**
   * Gets the local Maximum Concurrency Error
   * @return the Maximum Concurrency Error
   */
  public double getMaxConcurrencyError() {
    return bMaxConcurrencyError;
  }

  /**
   * Sets the local Maximum Number of Local Threads.
   * This is used to set the size of the local concurrent buffers.
   * @param maxNumLocalThreads the given Maximum Number of Local Threads
   */
  public void setMaxNumLocalThreads(final int maxNumLocalThreads) {
    bMaxNumLocalThreads = maxNumLocalThreads;
  }

  /**
   * Gets the local Maximum Number of Local Threads.
   * @return the Maximum Number of Local Threads.
   */
  public int getMaxNumLocalThreads() {
    return bMaxNumLocalThreads;
  }

  // BUILD FUNCTIONS

  /**
   * Returns an UpdatableThetaSketch with the current configuration of this Builder.
   * @return an UpdatableThetaSketch
   */
  public UpdatableThetaSketch build() {
    return build(null);
  }

  /**
   * Returns an UpdatableThetaSketch with the current configuration of this Builder
   * with the specified backing destination MemorySegment store.
   * Note: this can only be used with the QUICKSELECT Family of sketches
   * and cannot be used with the Alpha Family of sketches.
   * @param dstSeg The destination MemorySegment.
   * @return an UpdatableThetaSketch
   */
  public UpdatableThetaSketch build(final MemorySegment dstSeg) {
    UpdatableThetaSketch sketch = null;
    final boolean unionGadget = false;
    switch (bFam) {
      case ALPHA: {
        if (dstSeg == null) {
          sketch = HeapAlphaSketch.newHeapInstance(bLgNomLongs, bSeed, bP, bRF);
        }
        else {
          throw new SketchesArgumentException("AlphaSketch cannot be backed by a MemorySegment.");
        }
        break;
      }
      case QUICKSELECT: {
        if (dstSeg == null) {
          sketch =  new HeapQuickSelectSketch(bLgNomLongs, bSeed, bP, bRF, unionGadget);
        }
        else {
          sketch = new DirectQuickSelectSketch(bLgNomLongs, bSeed, bP, bRF, dstSeg, bMemorySegmentRequest, unionGadget);
        }
        break;
      }
      default: {
        throw new SketchesArgumentException(
          "Given Family cannot be built as an UpdatableThetaSketch: " + bFam.toString());
      }
    }
    return sketch;
  }

  /**
   * Returns an on-heap concurrent shared UpdatableThetaSketch with the current configuration of the
   * Builder.
   *
   * <p>The parameters unique to the shared concurrent sketch are:
   * <ul>
   * <li>Number of Pool Threads (default is 3)</li>
   * <li>Maximum Concurrency Error</li>
   * </ul>
   *
   * <p>Key parameters that are in common with other <i>ThetaSketches</i>:
   * <ul>
   * <li>Nominal Entries or Log Nominal Entries (for the shared concurrent sketch)</li>
   * </ul>
   *
   * @return an on-heap concurrent UpdatableThetaSketch with the current configuration of the Builder.
   */
  public UpdatableThetaSketch buildShared() {
    return buildShared(null);
  }

  /**
   * Returns a concurrent shared UpdatableThetaSketch with the current
   * configuration of the Builder and the given destination MemorySegment. If the destination
   * MemorySegment is null, this defaults to an on-heap concurrent shared UpdatableThetaSketch.
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
   * <li>Destination MemorySegment (if not null, returned sketch is Direct. Default is null.)</li>
   * </ul>
   *
   * @param dstSeg the given MemorySegment for Direct, otherwise <i>null</i>.
   * @return a concurrent UpdatableThetaSketch with the current configuration of the Builder
   * and the given destination MemorySegment.
   */
  @SuppressFBWarnings(value = "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD",
      justification = "Harmless in Builder, fix later")
  public UpdatableThetaSketch buildShared(final MemorySegment dstSeg) {
    ConcurrentPropagationService.NUM_POOL_THREADS = bNumPoolThreads;
    if (dstSeg == null) {
      return new ConcurrentHeapQuickSelectSketch(bLgNomLongs, bSeed, bMaxConcurrencyError);
    } else {
      return new ConcurrentDirectQuickSelectSketch(bLgNomLongs, bSeed, bMaxConcurrencyError, dstSeg);
    }
  }

  /**
   * Returns a direct (potentially off-heap) concurrent shared UpdatableThetaSketch with the current
   * configuration of the Builder, the data from the given sketch, and the given destination
   * MemorySegment. If the destination MemorySegment is null, this defaults to an on-heap
   * concurrent shared UpdatableThetaSketch.
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
   * <li>Destination MemorySegment (if not null, returned sketch is Direct. Default is null.)</li>
   * </ul>
   *
   * @param sketch a given UpdatableThetaSketch from which the data is used to initialize the returned
   * shared sketch.
   * @param dstSeg the given MemorySegment for Direct, otherwise <i>null</i>.
   * @return a concurrent UpdatableThetaSketch with the current configuration of the Builder
   * and the given destination MemorySegment.
   */
  @SuppressFBWarnings(value = "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD",
      justification = "Harmless in Builder, fix later")
  public UpdatableThetaSketch buildSharedFromSketch(final UpdatableThetaSketch sketch, final MemorySegment dstSeg) {
    ConcurrentPropagationService.NUM_POOL_THREADS = bNumPoolThreads;
    if (dstSeg == null) {
      return new ConcurrentHeapQuickSelectSketch(sketch, bSeed, bMaxConcurrencyError);
    } else {
      return new ConcurrentDirectQuickSelectSketch(sketch, bSeed, bMaxConcurrencyError, dstSeg);
    }
  }

  /**
   * Returns a local, on-heap, concurrent UpdatableThetaSketch to be used as a per-thread local buffer
   * along with the given concurrent shared UpdatableThetaSketch and the current configuration of this
   * Builder.
   *
   * <p>The parameters unique to the local concurrent sketch are:
   * <ul>
   * <li>Local Nominal Entries or Local Log Nominal Entries</li>
   * <li>Propagate Ordered Compact flag</li>
   * </ul>
   *
   * @param shared the concurrent shared sketch to be accessed via the concurrent local sketch.
   * @return an UpdatableThetaSketch to be used as a per-thread local buffer.
   */
  public UpdatableThetaSketch buildLocal(final UpdatableThetaSketch shared) {
    if (shared == null || !(shared instanceof ConcurrentSharedThetaSketch)) {
      throw new SketchesStateException("The concurrent shared sketch must be built first.");
    }
    return new ConcurrentHeapThetaBuffer(bConCurLgNomLongs, bSeed,
        (ConcurrentSharedThetaSketch) shared, bPropagateOrderedCompact, bMaxNumLocalThreads);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("UpdateSketchBuilder configuration:").append(LS);
    sb.append("LgK:").append(TAB).append(bLgNomLongs).append(LS);
    sb.append("K:").append(TAB).append(1 << bLgNomLongs).append(LS);
    sb.append("LgLocalK:").append(TAB).append(bConCurLgNomLongs).append(LS);
    sb.append("LocalK:").append(TAB).append(1 << bConCurLgNomLongs).append(LS);
    sb.append("Seed:").append(TAB).append(bSeed).append(LS);
    sb.append("p:").append(TAB).append(bP).append(LS);
    sb.append("ResizeFactor:").append(TAB).append(bRF).append(LS);
    sb.append("Family:").append(TAB).append(bFam).append(LS);
    sb.append("Propagate Ordered Compact").append(TAB).append(bPropagateOrderedCompact).append(LS);
    sb.append("NumPoolThreads").append(TAB).append(bNumPoolThreads).append(LS);
    sb.append("MaxConcurrencyError").append(TAB).append(bMaxConcurrencyError).append(LS);
    sb.append("MaxNumLocalThreads").append(TAB).append(bMaxNumLocalThreads).append(LS);
    return sb.toString();
  }

}
