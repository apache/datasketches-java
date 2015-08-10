package com.yahoo.sketches.hll;

/**
 */
public class HipHllSketch extends HllSketch
{
  // derived using some formulas in Ting's paper
  private static final double HIP_REL_ERROR_NUMER = 0.836083874576235;

  private double invPow2Sum;
  private double hipEstAccum;

  public HipHllSketch(final Fields fields)
  {
    super(fields);

    this.invPow2Sum = numBuckets();
    this.hipEstAccum = 0d;

    setUpdateCallback(
        new Fields.UpdateCallback()
        {
          private int numBuckets = fields.getPreamble().getConfigK();

          @Override
          public void bucketUpdated(int bucket, byte oldVal, byte newVal)
          {
            double oneOverQ = oneOverQ();
            hipEstAccum += oneOverQ;
            // subtraction before addition is intentional, in order to avoid overflow (?)
            invPow2Sum -= HllUtils.invPow2Table[oldVal];
            invPow2Sum += HllUtils.invPow2Table[newVal];
          }

          private double oneOverQ()
          {
            return numBuckets / invPow2Sum;
          }
        }
    );
  }

  @Override
  public HllSketch union(HllSketch that)
  {
    throw new UnsupportedOperationException("HipHllSketches cannot handle merges, use a normal HllSketch");
  }

  @Override
  public double getUpperBound(double numStdDevs)
  {
    return hipEstAccum / (1.0 - eps(numStdDevs));
  }

  @Override
  public double getLowerBound(double numStdDevs)
  {
    double lowerBound = hipEstAccum / (1.0 + eps(numStdDevs));
    int numBuckets = numBuckets();
    if (lowerBound < numBuckets) {
      double numNonZeros = numBuckets - numBucketsAtZero();
      if (lowerBound < numNonZeros) {
        return numNonZeros;
      }
    }
    return lowerBound;
  }

  @Override
  public double getEstimate()
  {
    return hipEstAccum;
  }

  @Override
  protected double inversePowerOf2Sum()
  {
    return invPow2Sum;
  }

  private double eps(double numStdDevs)
  {
    return numStdDevs * HIP_REL_ERROR_NUMER / Math.sqrt(numBuckets());
  }
}
