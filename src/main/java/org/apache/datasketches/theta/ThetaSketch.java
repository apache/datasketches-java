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

import static org.apache.datasketches.common.Family.idToFamily;
import static org.apache.datasketches.common.Util.LONG_MAX_VALUE_AS_DOUBLE;
import static org.apache.datasketches.common.Util.LS;
import static org.apache.datasketches.common.Util.ceilingPowerOf2;
import static org.apache.datasketches.common.Util.zeroPad;
import static org.apache.datasketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.checkSegPreambleCap;
import static org.apache.datasketches.theta.PreambleUtil.extractCurCount;
import static org.apache.datasketches.theta.PreambleUtil.extractFamilyID;
import static org.apache.datasketches.theta.PreambleUtil.extractFlags;
import static org.apache.datasketches.theta.PreambleUtil.extractSerVer;
import static org.apache.datasketches.theta.PreambleUtil.extractThetaLong;
import static org.apache.datasketches.thetacommon.HashOperations.count;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.MemorySegmentStatus;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.thetacommon.BinomialBoundsN;
import org.apache.datasketches.thetacommon.ThetaUtil;

/**
 * The top-level class for all theta sketches. This class is never constructed directly.
 * Use the UpdatableThetaSketch.builder() methods to create UpdatableThetaSketches.
 *
 * @author Lee Rhodes
 */
public abstract class ThetaSketch implements MemorySegmentStatus {

  ThetaSketch() {}

  //public static factory constructor-type methods

  /**
   * Heapify takes the sketch image in MemorySegment and instantiates an on-heap ThetaSketch.
   *
   * <p>The resulting sketch will not retain any link to the source MemorySegment.</p>
   *
   * <p>For UpdatableThetaSketches this method checks if the
   * <a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">Default Update Seed</a></p>
   * was used to create the source MemorySegment image.
   *
   * @param srcSeg an image of a ThetaSketch.
   *
   * @return a ThetaSketch on the heap.
   */
  public static ThetaSketch heapify(final MemorySegment srcSeg) {
    return heapify(srcSeg, Util.DEFAULT_UPDATE_SEED);
  }

  /**
   * Heapify takes the sketch image in MemorySegment and instantiates an on-heap ThetaSketch.
   *
   * <p>The resulting sketch will not retain any link to the source MemorySegment.</p>
   *
   * <p>For UpdatableThetaSketches this method checks if the expectedSeed
   * was used to create the source MemorySegment image.</p>
   *
   * @param srcSeg an image of a ThetaSketch that was created using the given expectedSeed.
   * @param expectedSeed the seed used to validate the given MemorySegment image.
   *  <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * Compact sketches store a 16-bit hash of the seed, but not the seed itself.
   * @return a ThetaSketch on the heap.
   */
  public static ThetaSketch heapify(final MemorySegment srcSeg, final long expectedSeed) {
    checkSegPreambleCap(srcSeg);
    final int familyID = extractFamilyID(srcSeg);
    if (familyID == Family.COMPACT.getID()) {
      return CompactThetaSketch.heapify(srcSeg, expectedSeed);
    }
    return heapifyUpdateSketchFromMemorySegment(srcSeg, expectedSeed);
  }

  /**
   * Wrap takes the sketch image in the given MemorySegment and refers to it directly.
   * There is no data copying onto the java heap.
   * The wrap operation enables fast read-only merging and access to all the public read-only API.
   *
   * <p>Only sketches that have been explicitly stored as direct sketches can be wrapped.</p>
   *
   * <p>Wrapping any subclass of this class that is empty or contains only a single item will
   * result in on-heap equivalent forms of empty and single item sketch respectively.
   * This is actually faster and consumes less overall space.</p>
   *
   * <p>This method checks if the
   * <a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">Default Update Seed</a>
   * was used to create the source MemorySegment image.</p>
   *
   * @param srcSeg a MemorySegment with an image of a ThetaSketch.
   * @return a read-only ThetaSketch backed by the given MemorySegment
   */
  public static ThetaSketch wrap(final MemorySegment srcSeg) {
    return wrap(srcSeg, Util.DEFAULT_UPDATE_SEED);
  }

  /**
   * Wrap takes the sketch image in the given MemorySegment and refers to it directly.
   * There is no data copying onto the java heap.
   * The wrap operation enables fast read-only merging and access to all the public read-only API.
   *
   * <p>Only sketches that have been explicitly stored as direct sketches can be wrapped.</p>
   *
   * <p>Wrapping any subclass of this class that is empty or contains only a single item will
   * result in on-heap equivalent forms of empty and single item sketch respectively.
   * This is actually faster and consumes less overall space.</p>
   *
   * <p>This method checks if the given expectedSeed
   * was used to create the source MemorySegment image.</p>
   *
   * @param srcSeg a MemorySegment with an image of a ThetaSketch.
   * @param expectedSeed the seed used to validate the given MemorySegment image.
   * <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @return a read-only ThetaSketch backed by the given MemorySegment.
   */
  public static ThetaSketch wrap(final MemorySegment srcSeg, final long expectedSeed) {
    checkSegPreambleCap(srcSeg);
    final int familyID = extractFamilyID(srcSeg);
    if (familyID == Family.QUICKSELECT.getID()) {
      return DirectQuickSelectSketchR.readOnlyWrap(srcSeg, expectedSeed);
    }
    if (familyID == Family.COMPACT.getID()) {
      return CompactThetaSketch.wrap(srcSeg, expectedSeed);
    }
    final Family family = Family.idToFamily(familyID);
    throw new SketchesArgumentException(
        "Cannot wrap family: " + family + " as a ThetaSketch");
  }

  //ThetaSketch interface

  /**
   * Converts this sketch to a ordered CompactThetaSketch.
   *
   * <p>If <i>this.isCompact() == true</i> this method returns <i>this</i>,
   * otherwise, this method is equivalent to
   * {@link #compact(boolean, MemorySegment) compact(true, null)}.
   *
   * <p>A CompactThetaSketch is always immutable.</p>
   *
   * @return this sketch as an ordered CompactThetaSketch.
   */
  public CompactThetaSketch compact() {
    return isCompact() ? (CompactThetaSketch)this : compact(true, null);
  }

  /**
   * Convert this sketch to a <i>CompactThetaSketch</i>.
   *
   * <p>If this sketch is a type of <i>UpdatableThetaSketch</i>, the compacting process converts the hash table
   * of the <i>UpdatableThetaketch</i> to a simple list of the valid hash values.
   * Any hash values of zero or equal-to or greater than theta will be discarded.
   * The number of valid values remaining in the <i>CompactThetaSketch</i> depends on a number of factors,
   * but may be larger or smaller than <i>Nominal Entries</i> (or <i>k</i>).
   * It will never exceed 2<i>k</i>.
   * If it is critical to always limit the size to no more than <i>k</i>,
   * then <i>rebuild()</i> should be called on the <i>UpdatableThetaSketch</i> prior to calling this method.</p>
   *
   * <p>A <i>CompactThetaSketch</i> is always immutable.</p>
   *
   * <p>A new <i>CompactThetaSketch</i> object is created:</p>
   * <ul><li>if <i>dstSeg!= null</i></li>
   * <li>if <i>dstSeg == null</i> and <i>this.hasMemorySegment() == true</i></li>
   * <li>if <i>dstSeg == null</i> and <i>this</i> has more than 1 item and <i>this.isOrdered() == false</i>
   * and <i>dstOrdered == true</i>.</li>
   *</ul>
   *
   * <p>Otherwise, this operation returns <i>this</i>.</p>
   *
   * @param dstOrdered assumed true if this sketch is empty or has only one value
   * <a href="{@docRoot}/resources/dictionary.html#dstOrdered">See Destination Ordered</a>
   *
   * @param dstSeg
   * <a href="{@docRoot}/resources/dictionary.html#dstSeg">See Destination MemorySegment</a>.
   *
   * @return this sketch as a <i>CompactThetaSketch</i>.
   */
  public abstract CompactThetaSketch compact(final boolean dstOrdered, final MemorySegment dstSeg);

  /**
   * Returns the number of storage bytes required for this ThetaSketch if its current state were
   * compacted. It this sketch is already in the compact form this is equivalent to
   * calling {@link #getCurrentBytes()}.
   * @return number of compact bytes
   */
  public abstract int getCompactBytes();

  /**
   * Gets the number of hash values less than the given theta expressed as a long.
   * @param thetaLong the given theta as a long between zero and <i>Long.MAX_VALUE</i>.
   * @return the number of hash values less than the given thetaLong.
   */
  public int getCountLessThanThetaLong(final long thetaLong) {
    return count(getCache(), thetaLong);
  }

  /**
   * Returns the number of storage bytes required for this sketch in its current state.
   *
   * @return the number of storage bytes required for this sketch
   */
  public abstract int getCurrentBytes();

  /**
   * Gets the unique count estimate.
   * @return the sketch's best estimate of the cardinality of the input stream.
   */
  public abstract double getEstimate();

  /**
   * Gets the estimate from the given MemorySegment
   * @param srcSeg the given MemorySegment
   * @return the result estimate
   */
  public static double getEstimate(final MemorySegment srcSeg) {
    checkSegPreambleCap(srcSeg);
    final int familyId = extractFamilyID(srcSeg);
    if (!isValidSketchID(familyId)) {
      throw new SketchesArgumentException("Source MemorySegment is not a valid ThetaSketch Family: "
          + Family.idToFamily(familyId).toString());
    }
    return ThetaSketch.estimate(extractThetaLong(srcSeg), getRetainedEntries(srcSeg));
  }

  /**
   * Returns the Family that this sketch belongs to
   * @return the Family that this sketch belongs to
   */
  public abstract Family getFamily();

  /**
   * Gets the approximate lower error bound given the specified number of Standard Deviations.
   * This will return getEstimate() if isEmpty() is true.
   *
   * @param numStdDev
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @return the lower bound.
   */
  public double getLowerBound(final int numStdDev) {
    return isEstimationMode()
        ? lowerBound(getRetainedEntries(true), getThetaLong(), numStdDev, isEmpty())
        : getRetainedEntries(true);
  }

  /**
   * Returns the maximum number of storage bytes required for a CompactThetaSketch with the given
   * number of actual entries.
   * @param numberOfEntries the actual number of retained entries stored in the sketch.
   * @return the maximum number of storage bytes required for a CompactThetaSketch with the given number
   * of retained entries.
   */
  public static int getMaxCompactSketchBytes(final int numberOfEntries) {
    if (numberOfEntries == 0) { return 8; }
    if (numberOfEntries == 1) { return 16; }
    return (numberOfEntries << 3) + 24;
  }

  /**
   * Returns the maximum number of storage bytes required for a CompactThetaSketch given the configured
   * log_base2 of the number of nominal entries, which is a power of 2.
   * @param lgNomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entries</a>
   * @return the maximum number of storage bytes required for a CompactThetaSketch with the given
   * lgNomEntries.
   */
  public static int getCompactSketchMaxBytes(final int lgNomEntries) {
    return (int)((2 << lgNomEntries) * ThetaUtil.REBUILD_THRESHOLD
        + Family.QUICKSELECT.getMaxPreLongs()) * Long.BYTES;
  }

  /**
   * Returns the maximum number of storage bytes required for an UpdatableThetaSketch with the given
   * number of nominal entries (power of 2).
   * @param nomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entries</a>
   * This will become the ceiling power of 2 if it is not.
   * @return the maximum number of storage bytes required for a UpdatableThetaSketch with the given
   * nomEntries
   */
  public static int getMaxUpdateSketchBytes(final int nomEntries) {
    final int nomEnt = ceilingPowerOf2(nomEntries);
    return (nomEnt << 4) + (Family.QUICKSELECT.getMaxPreLongs() << 3);
  }

  /**
   * Returns the maximum number of storage bytes required for an UpdatableThetaSketch with the given
   * log_base2 of the nominal entries.
   * @param lgNomEntries log_base2 of <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entries</a>
   * @return the maximum number of storage bytes required for a UpdatableThetaSketch with the given lgNomEntries
   */
  public static int getUpdateSketchMaxBytes(final int lgNomEntries) {
    return (16 << lgNomEntries) + (Family.QUICKSELECT.getMaxPreLongs() << 3);
  }

  /**
   * Returns the number of valid entries that have been retained by the sketch.
   * For the AlphaSketch this returns only valid entries.
   * @return the number of valid retained entries.
   */
  public int getRetainedEntries() {
    return getRetainedEntries(true);
  }

  /**
   * Returns the number of entries that have been retained by the sketch.
   * @param valid This parameter is only relevant for the AlphaSketch.
   * if true, returns the number of valid entries, which are less than theta and used
   * for estimation. Otherwise, return the number of all entries, valid or not, that are currently in the
   * internal sketch cache.
   * @return the number of retained entries
   */
  public abstract int getRetainedEntries(final boolean valid);

  /**
   * Returns the number of valid entries that have been retained by the sketch from the given MemorySegment
   * @param srcSeg the given MemorySegment that has an image of a ThetaSketch
   * @return the number of valid retained entries
   */
  public static int getRetainedEntries(final MemorySegment srcSeg) {
    final int preLongs = checkSegPreambleCap(srcSeg);
    final boolean empty = (extractFlags(srcSeg) & EMPTY_FLAG_MASK) != 0;
    return (preLongs == 1) ? (empty ? 0 : 1) : extractCurCount(srcSeg);
  }

  /**
   * Returns the serialization version from the given MemorySegment
   * @param seg the sketch MemorySegment
   * @return the serialization version from the MemorySegment
   */
  public static int getSerializationVersion(final MemorySegment seg) {
    checkSegPreambleCap(seg);
    return extractSerVer(seg);
  }

  /**
   * Gets the value of theta as a double with a value between zero and one
   * @return the value of theta as a double
   */
  public double getTheta() {
    return getThetaLong() / LONG_MAX_VALUE_AS_DOUBLE;
  }

  /**
   * Gets the value of theta as a long
   * @return the value of theta as a long
   */
  public abstract long getThetaLong();

  /**
   * Gets the approximate upper error bound given the specified number of Standard Deviations.
   * This will return getEstimate() if isEmpty() is true.
   *
   * @param numStdDev
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @return the upper bound.
   */
  public double getUpperBound(final int numStdDev) {
    return isEstimationMode()
        ? upperBound(getRetainedEntries(true), getThetaLong(), numStdDev, isEmpty())
        : getRetainedEntries(true);
  }

  /**
   * Returns true if this sketch is in compact form.
   * @return true if this sketch is in compact form.
   */
  public abstract boolean isCompact();

  /**
   * <a href="{@docRoot}/resources/dictionary.html#empty">See Empty</a>
   * @return true if empty.
   */
  public abstract boolean isEmpty();

  /**
   * Returns true if the sketch is Estimation Mode (as opposed to Exact Mode).
   * This is true if theta &lt; 1.0 AND isEmpty() is false.
   * @return true if the sketch is in estimation mode.
   */
  public boolean isEstimationMode() {
    return getThetaLong() < Long.MAX_VALUE && !isEmpty();
  }

  /**
   * Returns true if internal cache is ordered
   * @return true if internal cache is ordered
   */
  public abstract boolean isOrdered();

  /**
   * Returns a HashIterator that can be used to iterate over the retained hash values of the
   * Theta sketch.
   * @return a HashIterator that can be used to iterate over the retained hash values of the
   * Theta sketch.
   */
  public abstract HashIterator iterator();

  /**
   * Serialize this sketch to a byte array form.
   * @return byte array of this sketch
   */
  public abstract byte[] toByteArray();

  /**
   * Returns a human readable summary of the sketch.  This method is equivalent to the parameterized
   * call:<br>
   * <i>ThetaSketch.toString(ThetaSketch, true, false, 8, true);</i>
   * @return summary
   */
  @Override
  public String toString() {
    return toString(true, false, 8, true);
  }

  /**
   * Gets a human readable listing of contents and summary of the given sketch.
   * This can be a very long string.  If this sketch is in a "dirty" state there
   * may be values in the dataDetail view that are &ge; theta.
   *
   * @param sketchSummary If true the sketch summary will be output at the end.
   * @param dataDetail If true, includes all valid hash values in the sketch.
   * @param width The number of columns of hash values. Default is 8.
   * @param hexMode If true, hashes will be output in hex.
   * @return The result string, which can be very long.
   */
  public String toString(
      final boolean sketchSummary,
      final boolean dataDetail,
      final int width,
      final boolean hexMode) {
    final StringBuilder sb = new StringBuilder();

    int nomLongs = 0;
    int arrLongs = 0;
    float p = 0;
    int rf = 0;
    final boolean updateSketch = this instanceof UpdatableThetaSketch;

    final long thetaLong = getThetaLong();
    final int curCount = this.getRetainedEntries(true);

    if (updateSketch) {
      final UpdatableThetaSketch uis = (UpdatableThetaSketch)this;
      nomLongs = 1 << uis.getLgNomLongs();
      arrLongs = 1 << uis.getLgArrLongs();
      p = uis.getP();
      rf = uis.getResizeFactor().getValue();
    }

    if (dataDetail) {
      final int w = width > 0 ? width : 8; // default is 8 wide
      if (curCount > 0) {
        sb.append("### SKETCH DATA DETAIL");
        final HashIterator it = iterator();
        int j = 0;
        while (it.next()) {
          final long h = it.get();
          if (j % w == 0) {
            sb.append(LS).append(String.format("   %6d", j + 1));
          }
          if (hexMode) {
            sb.append(" " + zeroPad(Long.toHexString(h), 16) + ",");
          }
          else {
            sb.append(String.format(" %20d,", h));
          }
          j++ ;
        }
        sb.append(LS).append("### END DATA DETAIL").append(LS + LS);
      }
    }

    if (sketchSummary) {
      final double thetaDbl = thetaLong / LONG_MAX_VALUE_AS_DOUBLE;
      final String thetaHex = zeroPad(Long.toHexString(thetaLong), 16);
      final String thisSimpleName = this.getClass().getSimpleName();
      final int seedHash = Short.toUnsignedInt(getSeedHash());

      sb.append(LS);
      sb.append("### ").append(thisSimpleName).append(" SUMMARY: ").append(LS);
      if (updateSketch) {
        sb.append("   Nominal Entries (k)     : ").append(nomLongs).append(LS);
      }
      sb.append("   Estimate                : ").append(getEstimate()).append(LS);
      sb.append("   Upper Bound, 95% conf   : ").append(getUpperBound(2)).append(LS);
      sb.append("   Lower Bound, 95% conf   : ").append(getLowerBound(2)).append(LS);
      if (updateSketch) {
        sb.append("   p                       : ").append(p).append(LS);
      }
      sb.append("   Theta (double)          : ").append(thetaDbl).append(LS);
      sb.append("   Theta (long)            : ").append(thetaLong).append(LS);
      sb.append("   Theta (long) hex        : ").append(thetaHex).append(LS);
      sb.append("   EstMode?                : ").append(isEstimationMode()).append(LS);
      sb.append("   Empty?                  : ").append(isEmpty()).append(LS);
      sb.append("   Ordered?                : ").append(isOrdered()).append(LS);
      if (updateSketch) {
        sb.append("   Resize Factor           : ").append(rf).append(LS);
        sb.append("   Array Size Entries      : ").append(arrLongs).append(LS);
      }
      sb.append("   Retained Entries        : ").append(curCount).append(LS);
      sb.append("   Seed Hash               : ").append(Integer.toHexString(seedHash))
        .append(" | ").append(seedHash).append(LS);
      sb.append("### END SKETCH SUMMARY").append(LS);

    }
    return sb.toString();
  }

  /**
   * Returns a human readable string of the preamble of a byte array image of a ThetaSketch.
   * @param byteArr the given byte array
   * @return a human readable string of the preamble of a byte array image of a ThetaSketch.
   */
  public static String toString(final byte[] byteArr) {
    return PreambleUtil.preambleToString(byteArr);
  }

  /**
   * Returns a human readable string of the preamble of a MemorySegment image of a ThetaSketch.
   * @param seg the given MemorySegment object
   * @return a human readable string of the preamble of a MemorySegment image of a ThetaSketch.
   */
  public static String toString(final MemorySegment seg) {
    return PreambleUtil.preambleToString(seg);
  }

  //Restricted methods

  /**
   * Gets the internal cache array. For on-heap sketches this will return a reference to the actual
   * cache array. For MemorySegment-based sketches this returns a copy.
   *
   * <p>This can be an expensive operation and is intended for diagnostic & test applications.
   * Use {@link #iterator() iterator()} instead.</p>
   * @return the internal cache array.
   */
  abstract long[] getCache();

  /**
   * Gets preamble longs if stored in compact form. If this sketch is already in compact form,
   * this is identical to the call {@link #getCurrentPreambleLongs()}.
   * @return preamble longs if stored in compact form.
   */
  abstract int getCompactPreambleLongs();

  /**
   * Gets the number of data longs if stored in current state.
   * @return the number of data longs if stored in current state.
   */
  abstract int getCurrentDataLongs();

  /**
   * Returns preamble longs if stored in current state.
   * @return number of preamble longs if stored.
   */
  abstract int getCurrentPreambleLongs();

  /**
   * Returns the backing MemorySegment object if it exists, otherwise null.
   * This is overridden where relevant.
   * @return the backing MemorySegment object if it exists, otherwise null.
   */
  MemorySegment getMemorySegment() { return null; }

  /**
   * Gets the 16-bit seed hash
   * @return the seed hash
   */
  abstract short getSeedHash();

  static boolean getEmpty(final MemorySegment srcSeg) {
    checkSegPreambleCap(srcSeg);
    final int serVer = extractSerVer(srcSeg);
    if (serVer == 1) {
      return getThetaLong(srcSeg) == Long.MAX_VALUE && getRetainedEntries(srcSeg) == 0;
    }
    return (extractFlags(srcSeg) & EMPTY_FLAG_MASK) != 0;
  }

  static int getPreambleLongs(final MemorySegment srcSeg) {
    return checkSegPreambleCap(srcSeg);
  }

  static long getThetaLong(final MemorySegment srcSeg) {
    final int preLongs = checkSegPreambleCap(srcSeg);
    return preLongs < 3 ? Long.MAX_VALUE : extractThetaLong(srcSeg);
  }

  /**
   * Returns true if given Family id is one of the theta sketches
   * @param id the given Family id
   * @return true if given Family id is one of the theta sketches
   */
  static final boolean isValidSketchID(final int id) {
    return id == Family.ALPHA.getID()
        || id == Family.QUICKSELECT.getID()
        || id == Family.COMPACT.getID();
  }

  static final double estimate(final long thetaLong, final int curCount) {
    return curCount * (LONG_MAX_VALUE_AS_DOUBLE / thetaLong);
  }

  /**
   * Gets the approximate lower error bound from a valid MemorySegment image of a ThetaSketch
   * given the specified number of Standard Deviations.
   * This will return getEstimate() if isEmpty() is true.
   *
   * @param numStdDev
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @param srcSeg the source MemorySegment
   * @return the lower bound.
   */
  public static double getLowerBound(final int numStdDev, final MemorySegment srcSeg) {
    return lowerBound(getRetainedEntries(srcSeg), ThetaSketch.getThetaLong(srcSeg), numStdDev, ThetaSketch.getEmpty(srcSeg));
  }

  static final double lowerBound(final int curCount, final long thetaLong, final int numStdDev, final boolean empty) {
    final double theta = thetaLong / LONG_MAX_VALUE_AS_DOUBLE;
    return BinomialBoundsN.getLowerBound(curCount, theta, numStdDev, empty);
  }

  /**
   * Gets the approximate upper error bound from a valid MemorySegment image of a ThetaSketch
   * given the specified number of Standard Deviations.
   * This will return getEstimate() if isEmpty() is true.
   *
   * @param numStdDev
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @param srcSeg the source MemorySegment
   * @return the upper bound.
   */
  public static double getUpperBound(final int numStdDev, final MemorySegment srcSeg) {
    return upperBound(getRetainedEntries(srcSeg), ThetaSketch.getThetaLong(srcSeg), numStdDev, ThetaSketch.getEmpty(srcSeg));
  }

  static final double upperBound(final int curCount, final long thetaLong, final int numStdDev,
      final boolean empty) {
    final double theta = thetaLong / LONG_MAX_VALUE_AS_DOUBLE;
    return BinomialBoundsN.getUpperBound(curCount, theta, numStdDev, empty);
  }

  /**
   * Instantiates a Heap UpdatableThetaSketch from MemorySegment.
   * @param srcSeg the source MemorySegment
   * @param expectedSeed the seed used to validate the given MemorySegment image.
   * <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @return a ThetaSketch
   */
  private static final ThetaSketch heapifyUpdateSketchFromMemorySegment(final MemorySegment srcSeg, final long expectedSeed) {
    final Family family = idToFamily(extractFamilyID(srcSeg));

    if (family == Family.ALPHA) {
      final int flags = extractFlags(srcSeg);
      final boolean compactFlag = (flags & COMPACT_FLAG_MASK) != 0;
      if (compactFlag) {
        throw new SketchesArgumentException(
            "Corrupted: An ALPHA family image cannot be compact");
      }
      return HeapAlphaSketch.heapifyInstance(srcSeg, expectedSeed);
    }
    if (family == Family.QUICKSELECT) {
      return HeapQuickSelectSketch.heapifyInstance(srcSeg, expectedSeed);
    }
    throw new SketchesArgumentException(
        "Cannot heapify family: " + family + " as a ThetaSketch");
  }

}
