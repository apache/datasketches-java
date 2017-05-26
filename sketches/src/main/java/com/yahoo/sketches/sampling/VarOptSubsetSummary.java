package com.yahoo.sketches.sampling;

/**
 * A simple object o capture the results of a subset sum query on a varopt sketch.
 *
 * @author Jon Malkin
 */
public class VarOptSubsetSummary {
  private double lowerBound;
  private double estimate;
  private double upperBound;
  private double totalSketchWeight;

  VarOptSubsetSummary(final double lowerBound,
                      final double estimate,
                      final double upperBound,
                      final double totalSketchWeight) {
    this.lowerBound        = lowerBound;
    this.estimate          = estimate;
    this.upperBound        = upperBound;
    this.totalSketchWeight = totalSketchWeight;
  }

  public double getLowerBound() {
    return lowerBound;
  }

  public double getTotalSketchWeight() {
    return totalSketchWeight;
  }

  public double getUpperBound() {
    return upperBound;
  }

  public double getEstimate() {
    return estimate;
  }
}
