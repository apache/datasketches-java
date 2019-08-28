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

package org.apache.datasketches.fdt;

/**
 * Defines a Group from a Frequent Distinct Tuple query. This class is called internally during
 * post processing and is not inteded to be called by the user.
 * Note: this class has a natural ordering that is inconsistent with equals.
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
   * @param lb the lower bound of the extimate
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
   * @return priKey of type T
   */
  public String getPrimaryKey() { return priKey; }

  /**
   * @return the count
   */
  public int getCount() { return count; }

  /**
   * @return the estimate
   */
  public double getEstimate() { return est; }

  /**
   * @return the upper bound
   */
  public double getUpperBound() { return ub; }

  /**
   * @return the lower bound
   */
  public double getLowerBound() { return lb; }

  /**
   * @return the fraction for this group
   */
  public double getFraction() { return fraction; }

  /**
   * @return the RSE
   */
  public double getRse() { return rse; }

  /**
   * @return the descriptive header
   */
  public String getHeader() {
    return String.format(hfmt,"Count", "Est", "UB", "LB", "Fraction", "RSE", "PriKey");
  }

  @Override
  public String toString() {
    return String.format(fmt, count, est, ub, lb, fraction, rse, priKey);
  }

  /**
   * Note: this class has a natural ordering that is inconsistent with equals.
   * Ignore FindBugs EQ_COMPARETO_USE_OBJECT_EQUALS warning.
   * @param that The Group to compare to
   */
  @Override
  public int compareTo(final Group that) {
    return that.count - count; //decreasing
  }

}

