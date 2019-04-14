/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.fdt;

/**
 * Row class that defines the return values from a Frequent Distinct Tuple query.
 * @param <T> type of priKey
 *
 * @author Lee Rhodes
 */
public class Row<T> implements Comparable<Row<T>> {
  private final int count;
  private final double est;
  private final double ub;
  private final double lb;
  private final T priKey;
  private static final String FMT =  "%,12d" + "%,20.2f" + "%,20.2f" + "%,20.2f" + " %s";
  private static final String HFMT = "%12s"  + "%20s"    + "%20s"    + "%20s"    + " %s";

  Row(final T priKey, final int count, final double estimate, final double ub, final double lb) {
    this.count = count;
    this.est = estimate;
    this.ub = ub;
    this.lb = lb;
    this.priKey = priKey;
  }

  /**
   * @return priKey of type T
   */
  public T getPrimaryKey() { return priKey; }

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
   * @return return the lower bound
   */
  public double getLowerBound() { return lb; }

  /**
   * @return the descriptive row header
   */
  public static String getRowHeader() {
    return String.format(HFMT,"Count", "Est", "UB", "LB", "Item");
  }

  @Override
  public String toString() {
    return String.format(FMT, count, est, ub, lb, priKey.toString());
  }

  /**
   * This compareTo is strictly limited to the Row.getCount() value and does not imply any
   * ordering whatsoever to the other elements of the row: priKey and upper and lower bounds.
   * Defined this way, this compareTo will be consistent with hashCode() and equals(Object).
   * @param that the other row to compare to.
   * @return a negative integer, zero, or a positive integer as this.getCount() is less than,
   * equal to, or greater than that.getCount().
   */
  @Override
  public int compareTo(final Row<T> that) {
    return (that.count - this.count);
  }

} //End of class Row<T>

