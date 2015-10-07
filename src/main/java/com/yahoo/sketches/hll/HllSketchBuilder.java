package com.yahoo.sketches.hll;

public class HllSketchBuilder
{
  private Preamble preamble;
  private boolean dense = false;
  private boolean hipEstimation = false;

  public HllSketchBuilder copy() {
    HllSketchBuilder retVal = new HllSketchBuilder();

    retVal.preamble = preamble;
    retVal.dense = dense;
    retVal.hipEstimation = hipEstimation;

    return retVal;
  }

  public HllSketchBuilder setLogBuckets(int k) {
    this.preamble = Preamble.createSharedPreamble((byte) k);
    return this;
  }

  public HllSketchBuilder setPreamble(Preamble preamble) {
    this.preamble = preamble;
    return this;
  }

  public HllSketchBuilder asDense() {
    this.dense = true;
    return this;
  }

  public HllSketchBuilder usingHipEstimator() {
    this.hipEstimation = true;
    return this;
  }

  public HllSketch build() {
    final Fields fields;
    if (dense) {
      fields = new OnHeapFields(preamble);
    } else {
      fields = new OnHeapHashFields(preamble);
    }

    if (hipEstimation) {
      return new HipHllSketch(fields);
    } else {
      return new HllSketch(fields);
    }
  }
}
