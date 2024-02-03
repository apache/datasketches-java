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

package infiniFilter_experiments;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Calendar;

import filters.ChainedInfiniFilter;
import filters.FingerprintGrowthStrategy;
import filters.BasicInfiniFilter;
import filters.Chaining;

public class Experiment3 extends ExperimentsBase {

	public static void main(String[] args)  {

		parse_arguments(args);

		BasicInfiniFilter qf1 = new ChainedInfiniFilter(num_entries_power, bits_per_entry);
		qf1.set_fpr_style(FingerprintGrowthStrategy.FalsePositiveRateExpansion.UNIFORM);
		qf1.set_expand_autonomously(true); 
		baseline res1 = new baseline();
		long starting_index = 0;
		long end_key = qf1.get_max_entries_before_expansion() - 1;
		for (int i = num_entries_power; i <= num_cycles; i++ ) {
			Experiment1.scalability_experiment(qf1, starting_index, end_key, res1);
			starting_index = end_key;
			end_key = qf1.get_max_entries_before_expansion() * 2 - 1;
			System.out.println("infinifilter uniform FPR " + i);
		}
		
		BasicInfiniFilter qf2 = new ChainedInfiniFilter(num_entries_power, bits_per_entry);
		qf2.set_fpr_style(FingerprintGrowthStrategy.FalsePositiveRateExpansion.POLYNOMIAL);
		qf2.set_expand_autonomously(true); 
		baseline res2 = new baseline();
		starting_index = 0;
		end_key = qf2.get_max_entries_before_expansion() - 1;
		for (int i = num_entries_power; i <= num_cycles; i++ ) {
			Experiment1.scalability_experiment(qf2, starting_index, end_key, res2);
			starting_index = end_key;
			end_key = qf2.get_max_entries_before_expansion() * 2 - 1;
			System.out.println("infinifilter polynomial FPR " + i);
		}

		Chaining qf3 = new Chaining(num_entries_power, bits_per_entry);
		qf3.set_growth_style(Chaining.SizeExpansion.GEOMETRIC);
		qf3.set_fpr_style(FingerprintGrowthStrategy.FalsePositiveRateExpansion.POLYNOMIAL);
		qf3.set_expand_autonomously(true); 
		baseline res3 = new baseline();
		starting_index = 0;
		end_key = qf3.get_max_entries_before_expansion() - 1;
		for (int i = num_entries_power; i <= num_cycles - 1; i++ ) {
			Experiment1.scalability_experiment(qf3, starting_index, end_key, res3);
			starting_index = end_key;
			end_key = (long)(qf3.get_max_entries_before_expansion() * 2 + starting_index - 1);
			System.out.println("geometric chaining polynomial FPR " + i);
		}
		
		BasicInfiniFilter qf4 = new ChainedInfiniFilter(num_entries_power, bits_per_entry);
		qf4.set_fpr_style(FingerprintGrowthStrategy.FalsePositiveRateExpansion.TRIANGULAR);
		qf4.set_expand_autonomously(true); 
		baseline res4 = new baseline();
		starting_index = 0;
		end_key = qf4.get_max_entries_before_expansion() - 1;
		for (int i = num_entries_power; i <= num_cycles; i++ ) {
			Experiment1.scalability_experiment(qf4, starting_index, end_key, res4);
			starting_index = end_key;
			end_key = qf4.get_max_entries_before_expansion() * 2 - 1;
			System.out.println("infinifilter triangular FPR " + i);
		}

		int commas_before = 1;
		int commas_after = 3;
		res1.print("num_entries", "insertion_time", commas_before++, commas_after--);
		res2.print("num_entries", "insertion_time", commas_before++, commas_after--);
		res3.print("num_entries", "insertion_time", commas_before++, commas_after--);
		res4.print("num_entries", "insertion_time", commas_before++, commas_after--);

		
		System.out.println();

		commas_before = 1;
		commas_after = 2;
		res1.print("num_entries", "query_time", commas_before++, commas_after--);
		res2.print("num_entries", "query_time", commas_before++, commas_after--);
		res3.print("num_entries", "query_time", commas_before++, commas_after--);
		res4.print("num_entries", "query_time", commas_before++, commas_after--);

		System.out.println();

		commas_before = 1;
		commas_after = 2;
		res1.print("num_entries", "FPR", commas_before++, commas_after--);
		res2.print("num_entries", "FPR", commas_before++, commas_after--);
		res3.print("num_entries", "FPR", commas_before++, commas_after--);
		res4.print("num_entries", "FPR", commas_before++, commas_after--);

		System.out.println();

		commas_before = 1;
		commas_after = 2;
		res1.print("num_entries", "memory", commas_before++, commas_after--);
		res2.print("num_entries", "memory", commas_before++, commas_after--);
		res3.print("num_entries", "memory", commas_before++, commas_after--);
		res4.print("num_entries", "memory", commas_before++, commas_after--);

		
		String timeStamp = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(Calendar.getInstance().getTime());

		LocalDate ld = java.time.LocalDate.now();
		String dir_name = "Exp3_" + bits_per_entry + "_bytes_" +  timeStamp.toString();
	    Path path = Paths.get(dir_name);

		try {
			Files.createDirectories(path);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		String write_latency_file_name = dir_name + "/writes_speed.txt";
		String read_latency_file_name  = dir_name + "/read_speed.txt";
		String FPR_file_name  = dir_name + "/false_positive_rate.txt";
		String memory_file_name  = dir_name + "/memory.txt";
		String all_file_name  = dir_name + "/all.txt";
		
		create_file(write_latency_file_name);
		create_file(read_latency_file_name);
		create_file(FPR_file_name);
		create_file(memory_file_name);
		create_file(all_file_name);
		
	    try {
	        FileWriter insertion_writer = new FileWriter(write_latency_file_name);
	        
			commas_before = 1;
			commas_after = 2;
			res1.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, insertion_writer);
			res2.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, insertion_writer);
			res3.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, insertion_writer);
			res4.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, insertion_writer);

			//System.out.println();
			insertion_writer.close();
	        FileWriter reads_writer = new FileWriter(read_latency_file_name);

			commas_before = 1;
			commas_after = 2;
			res1.print_to_file("num_entries", "query_time", commas_before++, commas_after--, reads_writer);
			res2.print_to_file("num_entries", "query_time", commas_before++, commas_after--, reads_writer);
			res3.print_to_file("num_entries", "query_time", commas_before++, commas_after--, reads_writer);
			res4.print_to_file("num_entries", "query_time", commas_before++, commas_after--, reads_writer);

			//System.out.println();
			reads_writer.close();
	        FileWriter FPR_writer = new FileWriter(FPR_file_name);

			commas_before = 1;
			commas_after = 2;
			res1.print_to_file("num_entries", "FPR", commas_before++, commas_after--, FPR_writer);
			res2.print_to_file("num_entries", "FPR", commas_before++, commas_after--, FPR_writer);
			res3.print_to_file("num_entries", "FPR", commas_before++, commas_after--, FPR_writer);
			res4.print_to_file("num_entries", "FPR", commas_before++, commas_after--, FPR_writer);

			FPR_writer.close();
			FileWriter mem_writer = new FileWriter(memory_file_name);
			
			//System.out.println();

			commas_before = 1;
			commas_after = 2;
			res1.print_to_file("num_entries", "memory", commas_before++, commas_after--, mem_writer);
			res2.print_to_file("num_entries", "memory", commas_before++, commas_after--, mem_writer);
			res3.print_to_file("num_entries", "memory", commas_before++, commas_after--, mem_writer);
			res4.print_to_file("num_entries", "memory", commas_before++, commas_after--, mem_writer);

			mem_writer.close();
						
	    	FileWriter all_writer = new FileWriter(all_file_name);

			commas_before = 1;
			commas_after = 2;
			res1.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, all_writer);
			res2.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, all_writer);
			res3.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, all_writer);
			res4.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, all_writer);

			all_writer.write("\n");
			
			commas_before = 1;
			commas_after = 2;
			res1.print_to_file("num_entries", "query_time", commas_before++, commas_after--, all_writer);
			res2.print_to_file("num_entries", "query_time", commas_before++, commas_after--, all_writer);
			res3.print_to_file("num_entries", "query_time", commas_before++, commas_after--, all_writer);
			res4.print_to_file("num_entries", "query_time", commas_before++, commas_after--, all_writer);

			all_writer.write("\n");
			
			commas_before = 1;
			commas_after = 2;
			res1.print_to_file("num_entries", "FPR", commas_before++, commas_after--, all_writer);
			res2.print_to_file("num_entries", "FPR", commas_before++, commas_after--, all_writer);
			res3.print_to_file("num_entries", "FPR", commas_before++, commas_after--, all_writer);
			res4.print_to_file("num_entries", "FPR", commas_before++, commas_after--, all_writer);

			all_writer.write("\n");
			
			commas_before = 1;
			commas_after = 2;
			res1.print_to_file("num_entries", "memory", commas_before++, commas_after--, all_writer);
			res2.print_to_file("num_entries", "memory", commas_before++, commas_after--, all_writer);
			res3.print_to_file("num_entries", "memory", commas_before++, commas_after--, all_writer);
			res4.print_to_file("num_entries", "memory", commas_before++, commas_after--, all_writer);

			all_writer.close();
	    	
	        System.out.println("Successfully wrote to the files.");
	      } catch (IOException e) {
	        System.out.println("An error occurred.");
	        e.printStackTrace();
	      }
		
	}
	
}
