/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.fdt;

/**
 * Row class that defines the return values from a Frequent Distinct Tuple query.
 * @param <T> type of item
 *
 * @author Lee Rhodes
 */
public class Row<T> implements Comparable<Row<T>> {
  private final T item;
  private final long est;
  private final double ub;
  private final double lb;
  private static final String FMT =  "%,17d%,20.2f%,20.2f %s";
  private static final String HFMT = "%17s%20s%20s %s";

  Row(final T item, final long estimate, final double ub, final double lb) {
    this.item = item;
    this.est = estimate;
    this.ub = ub;
    this.lb = lb;
  }

  /**
   * @return item of type T
   */
  public T getItem() { return item; }

  /**
   * @return the estimate
   */
  public long getEstimate() { return est; }

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
    return String.format(HFMT,"Est", "UB", "LB", "Item");
  }

  @Override
  public String toString() {
    return String.format(FMT, est, ub, lb, item.toString());
  }

  /**
   * This compareTo is strictly limited to the Row.getEstimate() value and does not imply any
   * ordering whatsoever to the other elements of the row: item and upper and lower bounds.
   * Defined this way, this compareTo will be consistent with hashCode() and equals(Object).
   * @param that the other row to compare to.
   * @return a negative integer, zero, or a positive integer as this.getEstimate() is less than,
   * equal to, or greater than that.getEstimate().
   */
  @Override
  public int compareTo(final Row<T> that) {
    return (this.est < that.est) ? -1 : (this.est > that.est) ? 1 : 0;
  }

} //End of class Row<T>

