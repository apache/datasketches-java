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

package org.apache.datasketches.fdt2;

/**
 * Defines a Group from a Frequent Distinct Tuple query. This class is called internally during
 * post processing and is not intended to be called by the user.
 * @author Lee Rhodes
 */
public class Group implements Comparable<Group> {
  private int count = 0;
  private double est = 0;
  private double ub = 0;
  private double lb = 0;
  private double fraction = 0;
  private double rse = 0;
  private String priKey = null;
  private final static String fmt =
      "%,12d" + "%,15.2f" + "%,15.2f" + "%,15.2f" + "%12.6f" + "%12.6f" + " %s";
  private final static String hfmt =
      "%12s"  + "%15s"    + "%15s"    + "%15s"    + "%12s"   + "%12s"   + " %s";

  /**
   * Construct an empty Group
   */
  public Group() { }

  /**
   * Specifies the parameters to be listed as columns
   * @param priKey the primary key of the FDT sketch
   * @param count the number of retained rows associated with this group
   * @param estimate the estimate of the original population associated with this group
   * @param ub the upper bound of the estimate
   * @param lb the lower bound of the estimate
   * @param fraction the fraction of all retained rows of the sketch associated with this group
   * @param rse the estimated Relative Standard Error for this group.
   * @return return this
   */
  public Group init(final String priKey, final int count, final double estimate, final double ub,
      final double lb, final double fraction, final double rse) {
    this.count = count;
    est = estimate;
    this.ub = ub;
    this.lb = lb;
    this.fraction = fraction;
    this.rse = rse;
    this.priKey = priKey;
    return this;
  }

  /**
   * Gets the primary key of type String
   * @return priKey of type String
   */
  public String getPrimaryKey() { return priKey; }

  /**
   * Returns the count
   * @return the count
   */
  public int getCount() { return count; }

  /**
   * Returns the estimate
   * @return the estimate
   */
  public double getEstimate() { return est; }

  /**
   * Returns the upper bound
   * @return the upper bound
   */
  public double getUpperBound() { return ub; }

  /**
   * Returns the lower bound
   * @return the lower bound
   */
  public double getLowerBound() { return lb; }

  /**
   * Returns the fraction for this group
   * @return the fraction for this group
   */
  public double getFraction() { return fraction; }

  /**
   * Returns the RSE
   * @return the RSE
   */
  public double getRse() { return rse; }

  /**
   * Returns the descriptive header
   * @return the descriptive header
   */
  public String getHeader() {
    return String.format(hfmt,"Count", "Est", "UB", "LB", "Fraction", "RSE", "PriKey");
  }

  @Override
  public String toString() {
    return String.format(fmt, count, est, ub, lb, fraction, rse, priKey);
  }

  @Override
  public int compareTo(final Group that) {
    return that.count - count; //decreasing
  }

  @Override
  public boolean equals(final Object that) {
    if (this == that) { return true; }
    if (!(that instanceof Group)) { return false; }
    return ((Group)that).count == count;
  }

  @Override
  public int hashCode() {
    return Integer.MAX_VALUE - count; //MAX_VALUE is a Double Mersenne Prime = 2^31 - 1 = M_M_5
  }

}
