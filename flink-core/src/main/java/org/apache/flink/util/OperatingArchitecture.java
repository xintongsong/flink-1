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

package org.apache.flink.util;

import org.apache.flink.annotation.Internal;

/**
 * An enumeration indicating the operating architecture that the JVM runs on.
 */
@Internal
public enum OperatingArchitecture {

	AMD64,
	ARM64,
	UNKNOWN;

	// ------------------------------------------------------------------------
	/**
	 * Gets the operating system that the JVM runs on from the java system properties.
	 * this method returns <tt>UNKNOWN</tt>, if the operating system was not successfully determined.
	 *
	 * @return The enum constant for the operating system, or <tt>UNKNOWN</tt>, if it was not possible to determine.
		*/
	public static OperatingArchitecture getCurrentOperatingArchitecture() {
		return arch;
	}

	/**
	 * Checks whether the operating system this JVM runs on is Windows.
	 *
	 * @return <code>true</code> if the operating system this JVM runs on is
	 *         AMD64, <code>false</code> otherwise
	 */
	public static boolean isAMD64() {
		return getCurrentOperatingArchitecture() == AMD64;
	}

	/**
	 * Checks whether the operating system this JVM runs on is Linux.
	 *
	 * @return <code>true</code> if the operating system this JVM runs on is
	 *         ARM64, <code>false</code> otherwise
	 */
	public static boolean isARM64() {
		return getCurrentOperatingArchitecture() == ARM64;
	}

	/**
	 * The enum constant for the operating system.
	 */
	private static final OperatingArchitecture arch = readArchitectureFromSystemProperties();

	/**
	 * Parses the operating system that the JVM runs on from the java system properties.
	 * If the operating system was not successfully determined, this method returns {@code UNKNOWN}.
	 *
	 * @return The enum constant for the operating system, or {@code UNKNOWN}, if it was not possible to determine.
	 */
	private static OperatingArchitecture readArchitectureFromSystemProperties() {
		String archName = System.getProperty(OS_KEY);

		if (archName.startsWith(AMD64_ARCH_PREFIX)) {
			return AMD64;
		}
		if (archName.startsWith(ARM64_ARCH_PREFIX)) {
			return ARM64;
		}

		return UNKNOWN;
	}

	// --------------------------------------------------------------------------------------------
	//  Constants to extract the Architecture type from the java environment
	// --------------------------------------------------------------------------------------------

	/**
	 * The key to extract the operating system architecture from the system properties.
	 */
	private static final String OS_KEY = "os.arch";

	/**
	 * The expected prefix for AMD64 operating architecture.
	 */
	private static final String AMD64_ARCH_PREFIX = "adm64";

	/**
	 * The expected prefix for ARM64 operating architecture.
	 */
	private static final String ARM64_ARCH_PREFIX = "aarch64";
}
