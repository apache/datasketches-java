/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class RelativeErrorTables {

  /**
   * Return Relative Error for UB or LB for HIP or Non-HIP as a function of numStdDev.
   * @param upperBound true if for upper bound
   * @param oooFlag true if for Non-HIP
   * @param lgK must be between 4 and 12 inclusive
   * @param stdDev must be between 1 and 3 inclusive
   * @return Relative Error for UB or LB for HIP or Non-HIP as a function of numStdDev.
   */
  static double getRelErr(final boolean upperBound, final boolean oooFlag,
      final int lgK, final int stdDev) {
    final int idx = ((lgK - 4) * 3) + (stdDev - 1);
    final int sw = (oooFlag ? 2 : 0) | (upperBound ? 1 : 0);
    double f = 0;
    switch (sw) {
      case 0 : { //HIP, LB
        f = HIP_LB[idx];
        break;
      }
      case 1 : { //HIP, UB
        f = HIP_UB[idx];
        break;
      }
      case 2 : { //NON_HIP, LB
        f = NON_HIP_LB[idx];
        break;
      }
      case 3 : { //NON_HIP, UB
        f = NON_HIP_UB[idx];
        break;
      }
    }
    return f;
  }

  //case 0
  private static double[] HIP_LB = //sd 1, 2, 3
    { //Q(.84134), Q(.97725), Q(.99865) respectively
      0.207316195, 0.502865572, 0.882303765, //4
      0.146981579, 0.335426881, 0.557052,    //5
      0.104026721, 0.227683872, 0.365888317, //6
      0.073614601, 0.156781585, 0.245740374, //7
      0.05205248,  0.108783763, 0.168030442, //8
      0.036770852, 0.075727545, 0.11593785,  //9
      0.025990219, 0.053145536, 0.080772263, //10
      0.018373987, 0.037266176, 0.056271814, //11
      0.012936253, 0.02613829,  0.039387631, //12
    };

  //case 1
  private static double[] HIP_UB = //sd 1, 2, 3
    { //Q(.15866), Q(.02275), Q(.00135) respectively
      -0.207805347, -0.355574279, -0.475535095, //4
      -0.146988328, -0.262390832, -0.360864026, //5
      -0.103877775, -0.191503663, -0.269311582, //6
      -0.073452978, -0.138513438, -0.198487447, //7
      -0.051982806, -0.099703123, -0.144128618, //8
      -0.036768609, -0.07138158,  -0.104430324, //9
      -0.025991325, -0.050854296, -0.0748143,   //10
      -0.01834533,  -0.036121138, -0.05327616,  //11
      -0.012920332, -0.025572893, -0.037896952, //12
    };

  //case 2
  private static double[] NON_HIP_LB = //sd 1, 2, 3
    { //Q(.84134), Q(.97725), Q(.99865) respectively
      0.254409839, 0.682266712, 1.304022158, //4
      0.181817353, 0.443389054, 0.778776219, //5
      0.129432281, 0.295782195, 0.49252279,  //6
      0.091640655, 0.201175925, 0.323664385, //7
      0.064858051, 0.138523393, 0.218805328, //8
      0.045851855, 0.095925072, 0.148635751, //9
      0.032454144, 0.067009668, 0.102660669, //10
      0.022921382, 0.046868565, 0.071307398, //11
      0.016155679, 0.032825719, 0.049677541  //12
    };

  //case 3
  private static double[] NON_HIP_UB = //sd 1, 2, 3
    { //Q(.15866), Q(.02275), Q(.00135) respectively
      -0.256980172, -0.411905944, -0.52651057,  //4
      -0.182332109, -0.310275547, -0.412660505, //5
      -0.129314228, -0.230142294, -0.315636197, //6
      -0.091584836, -0.16834013,  -0.236346847, //7
      -0.06487411,  -0.122045231, -0.174112107, //8
      -0.04591465,  -0.08784505,  -0.126917615, //9
      -0.032433119, -0.062897613, -0.091862929, //10
      -0.022960633, -0.044875401, -0.065736049, //11
      -0.016186662, -0.031827816, -0.046973459  //12
    };

}
