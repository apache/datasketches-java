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

import infiniFilter_experiments.Experiment1;
import infiniFilter_experiments.Experiment2;
import infiniFilter_experiments.Experiment3;
import infiniFilter_experiments.Experiment4;
import infiniFilter_experiments.ExperimentsBase;

public class Client {

	static void run_tests() {
		Tests.test1(); // example from wikipedia
		Tests.test2(); // example from quotient filter paper
		Tests.test3(); // ensuring no false negatives
		Tests.test4(); // overflow test
		Tests.test5(); // deletion test 
		Tests.test6(); // iteration test 1
		Tests.test7(); // iteration test 2
		Tests.test8(); // expansion test for FingerprintShrinkingQF
		Tests.test9(); // expansion test for MultiplyingQF
		Tests.test10(); // testing InfiniFilter
		Tests.test12(); // testing InfiniFilter - chained
		Tests.test13(); // testing InfiniFilter - rejuvenation 
		Tests.test14(); // InfiniFilter deleting largest matching fingerprint 
		Tests.test15(); // testing deletes
		Tests.test16(); // testing deletes 
		Tests.test17(); // testing deletes 
		Tests.test18(); // testing deletes & rejuv operations
		Tests.test19(); // testing xxhash 
		Tests.test20(1000000); //testing xxhash(ByteBuffer)==xxhash(long)
		Tests.test21(1000000); // testing insert,search an delete of types int,long,String,byte[] 
		Tests.test22(); // testing no false negatives for bloom filter 
		Tests.test23(); // no false negatives for cuckoo filter
		Tests.test24(); // testing false positive rate for quotient filter  
		Tests.test25(); // testing false positive rate for cuckoo filter 
		Tests.test26(); // testing false positive rate for bloom filter 
		Tests.test27(); // exceeding the bound of the quotient filter 
		
		System.out.println("all tests passed");
	}
	
	

	static public  void main(String[] args) {		
		run_tests();
		Tests.measure_cluster_length_distribution();
		aleph_tests.run_tests();
		
		System.out.println("success");
		
	}


}
