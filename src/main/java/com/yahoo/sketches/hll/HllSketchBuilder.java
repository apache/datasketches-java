package com.yahoo.sketches.hll;

public class HllSketchBuilder
{
  private Preamble preamble;
  private boolean dense = false;

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

  public HllSketch build() {
    if (dense) {
      return new HllSketch(new OnHeapFields(preamble));
    }
    return new HllSketch(new OnHeapHashFields(preamble));
  }
}
