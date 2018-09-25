/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import java.util.HashMap;
import java.util.Map;

/**
 * Defines the various families of sketch and set operation classes.  A family defines a set of
 * classes that share fundamental algorithms and behaviors.  The classes within a family may
 * still differ by how they are stored and accessed. For example, internally there are separate
 * classes for the QuickSelect sketch algorithm that operate on the Java heap and off-heap.
 * Not all of these families have parallel forms on and off-heap but are included for completeness.
 *
 * @author Lee Rhodes
 */
public enum Family {
  /**
   * The Alpha Sketch family is a member of the Theta Sketch Framework of sketches and is best
   * suited for real-time processes where both the updating of the sketch and getting the estimate
   * is performed directly on the sketch.  In this situation the AlphaSketch has roughly a
   * 30% improvement (~1/sqrt(2*k)) in its error distribution as compared to the QuickSelect
   * (or similar KMV-derived) sketches.
   *
   * <p>If the AlphaSketch is fed into any SetOperation, the error distribution reverts back to the
   * normal QuickSelect/KMV error distribution (~1/sqrt(k)).  For this reason, the AlphaSketch
   * does not have a sister class for off-heap operation. The Alpha Sketch has a roughly 30% faster
   * overall update time as compared to the QuickSelect sketch family.</p>
   *
   * <p>The Alpha Sketch is created using the UpdateSketch.builder().
   * <a href="{@docRoot}/resources/dictionary.html#alphaTCF">See Alpha TCF</a> and
   * <a href="{@docRoot}/resources/dictionary.html#thetaSketch">Theta Sketch Framework</a>
   */
  ALPHA(1, "Alpha", 3, 3),

  /**
   * The QuickSelect Sketch family is a member of the Theta Sketch Framework of sketches and
   * is the workhorse of the Theta Sketch Families and can be constructed for either on-heap or
   * off-heap operation.
   * The QuickSelect Sketch is created using the UpdateSketch.builder().
   * <a href="{@docRoot}/resources/dictionary.html#quickSelectTCF">See Quick Select TCF</a>
   */
  QUICKSELECT(2, "QuickSelect", 3, 3),

  /**
   * The Compact Sketch family is a member of the Theta Sketch Framework of sketches.
   * The are read-only and cannot be updated, but can participate in any of the Set Operations.
   * The compact sketches are never created directly with a constructor or Builder.
   * Instead they are created as a result of the compact()
   * method of an UpdateSketch or as a result of a getSketchSamples() of a SetOperation.
   */
  COMPACT(3, "Compact", 1, 3),

  /**
   * The Union family is an operation for the Theta Sketch Framework of sketches.
   * The Union is constructed using the SetOperation.builder().
   */
  UNION(4, "Union", 4, 4),

  /**
   * The Intersection family is an operation for the Theta Sketch Framework of sketches.
   * The Intersection is constructed using the SetOperation.builder().
   */
  INTERSECTION(5, "Intersection", 3, 3),

  /**
   * The A and not B family is an operation for the Theta Sketch Framework of sketches.
   * The AnotB operation is constructed using the SetOperation.builder().
   */
  A_NOT_B(6, "AnotB", 3, 3),

  /**
   * The HLL family of sketches. (Not part of TSF.)
   */
  HLL(7, "HLL", 1, 1),

  /**
   * The Quantiles family of sketches. (Not part of TSF.)
   */
  QUANTILES(8, "QUANTILES", 1, 2),

  /**
   * The Tuple family of sketches is a large family of sketches that are extensions of the
   * Theta Sketch Framework.
   */
  TUPLE(9, "TUPLE", 1, 1),

  /**
   * The Frequency family of sketches. (Not part of TSF.)
   */
  FREQUENCY(10, "FREQUENCY", 1, 4),

  /**
   * The Reservoir family of sketches. (Not part of TSF.)
   */
  RESERVOIR(11, "RESERVOIR", 1, 2),

  /**
   * The reservoir sampling family of Union operations. (Not part of TSF.)
   */
  RESERVOIR_UNION(12, "RESERVOIR_UNION", 1, 1),

  /**
   * The VarOpt family of sketches. (Not part of TSF.)
   */
  VAROPT(13, "VAROPT", 1, 4),

  /**
   * The VarOpt family of sketches. (Not part of TSF.)
   */
  VAROPT_UNION(14, "VAROPT_UNION", 1, 4),

  /**
   * KLL quanliles sketch
   */
  KLL(15, "KLL", 1, 2),

  /**
   * Compressed Probabilistic Counting (CPC) Sketch
   */
  CPC(16, "CPC", 1, 5);

  private static final Map<Integer, Family> lookupID = new HashMap<>();
  private static final Map<String, Family> lookupFamName = new HashMap<>();
  private int id_;
  private String famName_;
  private int minPreLongs_;
  private int maxPreLongs_;

  static {
    for (Family f : values()) {
      lookupID.put(f.getID(), f);
      lookupFamName.put(f.getFamilyName().toUpperCase(), f);
    }
  }

  private Family(final int id, final String famName, final int minPreLongs, final int maxPreLongs) {
    id_ = id;
    famName_ = famName.toUpperCase();
    minPreLongs_ = minPreLongs;
    maxPreLongs_ = maxPreLongs;
  }

  /**
   * Returns the byte ID for this family
   * @return the byte ID for this family
   */
  public int getID() {
    return id_;
  }

  /**
   *
   * @param id the given id, a value &lt; 128.
   */
  public void checkFamilyID(final int id) {
    if (id != id_) {
      throw new SketchesArgumentException(
          "Possible Corruption: This Family " + toString()
            + " does not match the ID of the given Family: " + idToFamily(id).toString());
    }
  }

  /**
   * Returns the name for this family
   * @return the name for this family
   */
  public String getFamilyName() {
    return famName_;
  }

  /**
   * Returns the minimum preamble size for this family in longs
   * @return the minimum preamble size for this family in longs
   */
  public int getMinPreLongs() {
    return minPreLongs_;
  }

  /**
   * Returns the maximum preamble size for this family in longs
   * @return the maximum preamble size for this family in longs
   */
  public int getMaxPreLongs() {
    return maxPreLongs_;
  }

  @Override
  public String toString() {
    return famName_;
  }

  /**
   * Returns the Family given the ID
   * @param id the given ID
   * @return the Family given the ID
   */
  public static Family idToFamily(final int id) {
    final Family f = lookupID.get(id);
    if (f == null) {
      throw new SketchesArgumentException("Possible Corruption: Illegal Family ID: " + id);
    }
    return f;
  }

  /**
   * Returns the Family given the family name
   * @param famName the family name
   * @return the Family given the family name
   */
  public static Family stringToFamily(final String famName) {
    final Family f = lookupFamName.get(famName.toUpperCase());
    if (f == null) {
      throw new SketchesArgumentException("Possible Corruption: Illegal Family Name: " + famName);
    }
    return f;
  }

}
