package com.yahoo.sketches.hll;

import com.yahoo.sketches.SketchesArgumentException;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class CubicInterpolation {

  /**
   * Cubic interpolation using interpolation X and Y tables.
   *
   * @param xArr xArr
   * @param yArr yArr
   * @param x x
   * @return cubic interpolation
   */
  //Used by AbstractCoupons
  //In C: again-two-registers cubic_interpolate_using_table L1377
  static double usingXAndYTables(final double[] xArr, final double[] yArr,
      final double x) {
    assert (xArr.length >= 4) && (xArr.length == yArr.length);
    if ((x < xArr[0]) || (x > xArr[xArr.length - 1])) {
      throw new SketchesArgumentException("X value out of range: " + x);
    }
    if (x == xArr[xArr.length - 1]) {
      return yArr[yArr.length - 1]; // corner case
    }
    final int offset = findStraddle(xArr, x); //uses recursion
    assert (offset >= 0) && (offset <= (xArr.length - 2));
    if (offset == 0) {
      return interpolateUsingXAndYTables(xArr, yArr, offset, x); // corner case
    }
    if (offset == (xArr.length - 2)) {
      return interpolateUsingXAndYTables(xArr, yArr, offset - 2, x); // corner case
    }
    return interpolateUsingXAndYTables(xArr, yArr, offset - 1, x);
  }

  // In C: again-two-registers cubic_interpolate_aux L1368
  private static double interpolateUsingXAndYTables(final double[] xArr, final double[] yArr,
      final int offset, final double x) {
    return cubicInterpolate(xArr[offset], yArr[offset], xArr[offset + 1], yArr[offset + 1],
        xArr[offset + 2], yArr[offset + 2], xArr[offset + 3], yArr[offset + 3], x);
  }

  //Interpolate using X table and Y stride

  /**
   * Cubic interpolation using interpolation X table and Y stride.
   *
   * @param xArr The x array
   * @param yStride The y stride
   * @param x The value x
   * @return cubic interpolation
   */
  //In C: again-two-registers cubic_interpolate_with_x_arr_and_y_stride L1411
  // Used by HllEstimators
  static double usingXArrAndYStride(
      final double[] xArr, final double yStride, final double x) {
    final int xArrLen = xArr.length;
    final int xArrLenM1 = xArrLen - 1;

    final int offset;
    assert ((xArrLen >= 4) && (x >= xArr[0]) && (x <= xArr[xArrLenM1]));

    if (x ==  xArr[xArrLenM1]) { /* corner case */
      return (yStride * (xArrLenM1));
    }

    offset = findStraddle(xArr, x); //uses recursion
    final int xArrLenM2 = xArrLen - 2;
    assert ((offset >= 0) && (offset <= (xArrLenM2)));

    if (offset == 0) { /* corner case */
      return (interpolateUsingXArrAndYStride(xArr, yStride, (offset - 0), x));
    }
    else if (offset == xArrLenM2) { /* corner case */
      return (interpolateUsingXArrAndYStride(xArr, yStride, (offset - 2), x));
    }
    /* main case */
    return (interpolateUsingXArrAndYStride(xArr, yStride, (offset - 1), x));
  }

  //In C: again-two-registers cubic_interpolate_with_x_arr_and_y_stride_aux L1402
  private static double interpolateUsingXArrAndYStride(final double[] xArr, final double yStride,
      final int offset, final double x) {
    return cubicInterpolate(
        xArr[offset + 0], yStride * (offset + 0),
        xArr[offset + 1], yStride * (offset + 1),
        xArr[offset + 2], yStride * (offset + 2),
        xArr[offset + 3], yStride * (offset + 3),
        x);
  }

  //Cubic Interpolation used by both methods

  // Interpolate using the cubic curve that passes through the four given points, using the
  // Lagrange interpolation formula.
  // In C: again-two-registers cubic_interpolate_aux_aux L1346
  private static double cubicInterpolate(final double x0, final double y0, final double x1,
      final double y1, final double x2, final double y2, final double x3, final double y3,
      final double x) {
    final double l0Numer = (x - x1) * (x - x2) * (x - x3);
    final double l1Numer = (x - x0) * (x - x2) * (x - x3);
    final double l2Numer = (x - x0) * (x - x1) * (x - x3);
    final double l3Numer = (x - x0) * (x - x1) * (x - x2);

    final double l0Denom = (x0 - x1) * (x0 - x2) * (x0 - x3);
    final double l1Denom = (x1 - x0) * (x1 - x2) * (x1 - x3);
    final double l2Denom = (x2 - x0) * (x2 - x1) * (x2 - x3);
    final double l3Denom = (x3 - x0) * (x3 - x1) * (x3 - x2);

    final double term0 = (y0 * l0Numer) / l0Denom;
    final double term1 = (y1 * l1Numer) / l1Denom;
    final double term2 = (y2 * l2Numer) / l2Denom;
    final double term3 = (y3 * l3Numer) / l3Denom;

    return term0 + term1 + term2 + term3;
  }

  //In C: again-two-registers.c find_straddle L1335
  private static int findStraddle(final double[] xArr, final double x) {
    assert ((xArr.length >= 2) && (x >= xArr[0]) && (x <= xArr[xArr.length - 1]));
    return (recursiveFindStraddle(xArr, 0, xArr.length - 1, x));
  }

  //In C: again-two-registers.c find_straddle_aux L1322
  private static int recursiveFindStraddle(final double[] xArr, final int left, final int right,
      final double x) {
    final int middle;
    assert (left < right);
    assert ((xArr[left] <= x) && (x < xArr[right])); /* the invariant */
    if ((left + 1) == right) {
      return (left);
    }
    middle = left + ((right - left) / 2);
    if (xArr[middle] <= x) {
      return (recursiveFindStraddle(xArr, middle, right, x));
    } else {
      return (recursiveFindStraddle(xArr, left, middle, x));
    }
  }

}
