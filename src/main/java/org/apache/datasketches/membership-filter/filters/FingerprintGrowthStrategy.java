/*
 * Copyright 2024 Niv Dayan

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package filters;

public class FingerprintGrowthStrategy {

	public enum FalsePositiveRateExpansion {
		UNIFORM,
		POLYNOMIAL,
		POLYNOMIAL_SHRINK,
		TRIANGULAR,
		GEOMETRIC,
	}
	
	// determines what is the new length of fingerprints to be used for entries inserted in the current generation
	static int  get_new_fingerprint_size(int original_fingerprint_size, int num_expansions, long max_num_expansions_est, FalsePositiveRateExpansion fprStyle) {
		
		double original_FPR = Math.pow(2, -original_fingerprint_size);
		double new_filter_FPR = 0;
		if (fprStyle == FalsePositiveRateExpansion.GEOMETRIC) {
			double factor = 1.0 / Math.pow(2, num_expansions);
			new_filter_FPR = factor * original_FPR; 
		}
		else if (fprStyle == FalsePositiveRateExpansion.POLYNOMIAL) {
			double factor = 1.0 / Math.pow(num_expansions + 1, 2);
			new_filter_FPR = factor * original_FPR; 
		}
		else if (fprStyle == FalsePositiveRateExpansion.POLYNOMIAL_SHRINK) {
			
			/*double additional_init_FP = Math.ceil( Math.log(Math.pow(max_num_expansions_est, 2)) / Math.log(2) );
			double target_fp_size = original_fingerprint_size - additional_init_FP;
			double target_FPR = Math.pow(2, -target_fp_size);
			
			double current = max_num_expansions_est - num_expansions + 1;
			//System.out.println(current);
			current = current == 0 ? 1 : current;
			current = Math.abs(current);
			double factor = 1.0 / Math.pow(current, 2);
			new_filter_FPR = factor * target_FPR; */
			
			long abs = Math.abs(max_num_expansions_est - 1 - num_expansions);
			long max = Math.max(abs, 1);
			double log = Math.log(max) / Math.log(2);
			int ceil = 2 * (int)Math.round(log);
			int FP_size = original_fingerprint_size + ceil;
			//System.out.println(num_expansions + " \t " + max_num_expansions_est + " \t " + FP_size);
			return FP_size;
			
		}
		else if (fprStyle == FalsePositiveRateExpansion.TRIANGULAR) {
			double factor = 1.0 / (num_expansions * (num_expansions + 1));
			new_filter_FPR = factor * original_FPR; 
		}
		else if (fprStyle == FalsePositiveRateExpansion.UNIFORM) {
			new_filter_FPR = original_FPR; 
		}		
		double fingerprint_size = Math.ceil( Math.log(1.0/new_filter_FPR) / Math.log(2) );
		int fingerprint_size_int = (int) fingerprint_size;
		return fingerprint_size_int;
	}
	
	
}
