/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.fs.dummy.examples;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.net.URI;

/**
 * Word Count example for testing plugin mechanism.
 * dummy://localhost/words and anotherDummy://localhost/words could be supported together.
 * Different filesystem schema should have different classloader.
 * Different filesystem can not see any classes from the others.
 */
public class WordCount {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		// get input data
		if (!params.has("input1") || !params.has("input2")) {
			throw new IllegalArgumentException("--input1 and --input2 should be specified.");
		}
		String uriOfInput1 = params.get("input1");
		String uriOfInput2 = params.get("input2");
		DataSet<String> text1 = env.readTextFile(uriOfInput1);
		DataSet<String> text2 = env.readTextFile(uriOfInput2);

		DataSet<Tuple2<String, Integer>> counts =
				// split up the lines in pairs (2-tuples) containing: (word,1)
				text1.union(text2).flatMap(new Tokenizer(uriOfInput1, uriOfInput2))
				// group by the tuple field "0" and sum up tuple field "1"
				.groupBy(0)
				.sum(1);

		// emit result
		if (params.has("output")) {
			counts.writeAsCsv(params.get("output"), "\n", ",");
			// execute program
			env.execute("Two input WordCount Example for plugin mechanism.");
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			counts.print();
		}

	}

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into
	 * multiple pairs in the form of "(word,1)" ({@code Tuple2<String, Integer>}).
	 */
	@SuppressWarnings("serial")
	static final class Tokenizer extends RichFlatMapFunction<String, Tuple2<String, Integer>> {

		private final String input1;
		private final String input2;

		Tokenizer(String input1, String input2) {
			this.input1 = input1;
			this.input2 = input2;
		}

		@Override
		public void open(Configuration conf) throws IOException {
			checkPluginsIsolation();
		}

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<>(token, 1));
				}
			}
		}

		private void checkPluginsIsolation() throws IOException {

			// Use getUnguardedFileSystem to get the classloader of the real filesystem, not the wrapper filesystem
			FileSystem fileSystem1 = FileSystem.getUnguardedFileSystem(URI.create(input1));
			FileSystem fileSystem2 = FileSystem.getUnguardedFileSystem(URI.create(input2));

			if (!fileSystem1.getUri().getScheme().equals(fileSystem2.getUri().getScheme())) {
				// Different filesystem schema should have different classloader.
				Preconditions.checkState(!fileSystem1.getClass().getClassLoader().equals(
					fileSystem2.getClass().getClassLoader()), "{} and {} should have different classloader.",
					input1, input2);

				// Different filesystem can not see any classes from the others.
				checkVisibility(fileSystem1, fileSystem2);
				checkVisibility(fileSystem2, fileSystem1);
			}
		}

		private void checkVisibility(FileSystem fileSystem, FileSystem anotherFileSystem) {
			boolean visible = true;
			try {
				fileSystem.getClass().getClassLoader().loadClass(anotherFileSystem.getClass().getCanonicalName());
			} catch (ClassNotFoundException e) {
				visible = false;
			}
			Preconditions.checkState(!visible, "%s should not be visible for filesystem %s.",
				anotherFileSystem.getClass().getCanonicalName(),
				fileSystem.getClass().getCanonicalName());
		}
	}

}
