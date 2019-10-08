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

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;

import java.math.BigDecimal;
import java.util.Locale;

import static org.apache.flink.configuration.MemorySize.MemoryUnit.BYTES;
import static org.apache.flink.configuration.MemorySize.MemoryUnit.GIGA_BYTES;
import static org.apache.flink.configuration.MemorySize.MemoryUnit.KILO_BYTES;
import static org.apache.flink.configuration.MemorySize.MemoryUnit.MEGA_BYTES;
import static org.apache.flink.configuration.MemorySize.MemoryUnit.TERA_BYTES;
import static org.apache.flink.configuration.MemorySize.MemoryUnit.hasUnit;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * MemorySize is a representation of a number of bytes, viewable in different units.
 *
 * <h2>Parsing</h2>
 *
 * <p>The size can be parsed from a text expression. If the expression is a pure number,
 * the value will be interpreted as bytes.
 *
 */
@PublicEvolving
public class MemorySize implements java.io.Serializable {

	private static final long serialVersionUID = 1L;

	public static final MemorySize ZERO = new MemorySize(0L);

	public static final MemorySize MAX_VALUE = new MemorySize(Long.MAX_VALUE);

	// ------------------------------------------------------------------------

	/** The memory size, in bytes. */
	private final long bytes;

	/**
	 * Constructs a new MemorySize.
	 *
	 * @param bytes The size, in bytes. Must be zero or larger.
	 */
	public MemorySize(long bytes) {
		checkArgument(bytes >= 0, "bytes must be >= 0");
		this.bytes = bytes;
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets the memory size in bytes.
	 */
	public long getBytes() {
		return bytes;
	}

	/**
	 * Gets the memory size in Kibibytes (= 1024 bytes).
	 */
	public long getKibiBytes() {
		return bytes >> 10;
	}

	/**
	 * Gets the memory size in Mebibytes (= 1024 Kibibytes).
	 */
	public int getMebiBytes() {
		return (int) (bytes >> 20);
	}

	/**
	 * Gets the memory size in Gibibytes (= 1024 Mebibytes).
	 */
	public long getGibiBytes() {
		return bytes >> 30;
	}

	/**
	 * Gets the memory size in Tebibytes (= 1024 Gibibytes).
	 */
	public long getTebiBytes() {
		return bytes >> 40;
	}

	// ------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return (int) (bytes ^ (bytes >>> 32));
	}

	@Override
	public boolean equals(Object obj) {
		return obj == this ||
				(obj != null && obj.getClass() == this.getClass() && ((MemorySize) obj).bytes == this.bytes);
	}

	@Override
	public String toString() {
		return bytes + " bytes";
	}

	// ------------------------------------------------------------------------
	//  Calculations
	// ------------------------------------------------------------------------

	public MemorySize add(MemorySize that) {
		return new MemorySize(Math.addExact(this.bytes, that.bytes));
	}

	public MemorySize subtract(MemorySize that) {
		return new MemorySize(Math.subtractExact(this.bytes, that.bytes));
	}

	public MemorySize multiply(double multiplier) {
		checkArgument(multiplier >= 0, "multiplier must be >= 0");

		BigDecimal product = BigDecimal.valueOf(this.bytes).multiply(BigDecimal.valueOf(multiplier));
		if (product.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) > 0) {
			throw new ArithmeticException("long overflow");
		}
		return new MemorySize(product.longValue());
	}

	// ------------------------------------------------------------------------
	//  Parsing
	// ------------------------------------------------------------------------

	/**
	 * Parses the given string as as MemorySize.
	 *
	 * @param text The string to parse
	 * @return The parsed MemorySize
	 *
	 * @throws IllegalArgumentException Thrown, if the expression cannot be parsed.
	 */
	public static MemorySize parse(String text) throws IllegalArgumentException {
		return new MemorySize(parseBytes(text));
	}

	/**
	 * Parses the given string with a default unit.
	 *
	 * @param text The string to parse.
	 * @param defaultUnit specify the default unit.
	 * @return The parsed MemorySize.
	 *
	 * @throws IllegalArgumentException Thrown, if the expression cannot be parsed.
	 */
	public static MemorySize parse(String text, MemoryUnit defaultUnit) throws IllegalArgumentException {
		if (!hasUnit(text)) {
			return parse(text + defaultUnit.getUnits()[0]);
		}

		return parse(text);
	}

	/**
	 * Parses the given string as bytes.
	 * The supported expressions are listed under {@link MemorySize}.
	 *
	 * @param text The string to parse
	 * @return The parsed size, in bytes.
	 *
	 * @throws IllegalArgumentException Thrown, if the expression cannot be parsed.
	 */
	public static long parseBytes(String text) throws IllegalArgumentException {
		checkNotNull(text, "text");

		final String trimmed = text.trim();
		checkArgument(!trimmed.isEmpty(), "argument is an empty- or whitespace-only string");

		final int len = trimmed.length();
		int pos = 0;

		char current;
		while (pos < len && (current = trimmed.charAt(pos)) >= '0' && current <= '9') {
			pos++;
		}

		final String number = trimmed.substring(0, pos);
		final String unit = trimmed.substring(pos).trim().toLowerCase(Locale.US);

		if (number.isEmpty()) {
			throw new NumberFormatException("text does not start with a number");
		}

		final long value;
		try {
			value = Long.parseLong(number); // this throws a NumberFormatException on overflow
		}
		catch (NumberFormatException e) {
			throw new IllegalArgumentException("The value '" + number +
					"' cannot be re represented as 64bit number (numeric overflow).");
		}

		final long multiplier;
		if (unit.isEmpty()) {
			multiplier = 1L;
		}
		else {
			if (matchesAny(unit, BYTES)) {
				multiplier = 1L;
			}
			else if (matchesAny(unit, KILO_BYTES)) {
				multiplier = 1024L;
			}
			else if (matchesAny(unit, MEGA_BYTES)) {
				multiplier = 1024L * 1024L;
			}
			else if (matchesAny(unit, GIGA_BYTES)) {
				multiplier = 1024L * 1024L * 1024L;
			}
			else if (matchesAny(unit, TERA_BYTES)) {
				multiplier = 1024L * 1024L * 1024L * 1024L;
			}
			else {
				throw new IllegalArgumentException("Memory size unit '" + unit +
						"' does not match any of the recognized units: " + MemoryUnit.getAllUnits());
			}
		}

		final long result = value * multiplier;

		// check for overflow
		if (result / multiplier != value) {
			throw new IllegalArgumentException("The value '" + text +
					"' cannot be re represented as 64bit number of bytes (numeric overflow).");
		}

		return result;
	}

	private static boolean matchesAny(String str, MemoryUnit unit) {
		for (String s : unit.getUnits()) {
			if (s.equals(str)) {
				return true;
			}
		}
		return false;
	}

	/**
	 *  Enum which defines memory unit, mostly used to parse value from configuration file.
	 *
	 * <p>To make larger values more compact, the common size suffixes are supported:
	 *
	 * <ul>
	 *     <li>q or 1b or 1bytes (bytes)
	 *     <li>1k or 1kb or 1kibibytes (interpreted as kibibytes = 1024 bytes)
	 *     <li>1m or 1mb or 1mebibytes (interpreted as mebibytes = 1024 kibibytes)
	 *     <li>1g or 1gb or 1gibibytes (interpreted as gibibytes = 1024 mebibytes)
	 *     <li>1t or 1tb or 1tebibytes (interpreted as tebibytes = 1024 gibibytes)
	 * </ul>
	 *
	 */
	public enum MemoryUnit {

		BYTES(new String[] { "b", "bytes" }),
		KILO_BYTES(new String[] { "k", "kb", "kibibytes" }),
		MEGA_BYTES(new String[] { "m", "mb", "mebibytes" }),
		GIGA_BYTES(new String[] { "g", "gb", "gibibytes" }),
		TERA_BYTES(new String[] { "t", "tb", "tebibytes" });

		private String[] units;

		MemoryUnit(String[] units) {
			this.units = units;
		}

		public String[] getUnits() {
			return units;
		}

		public static String getAllUnits() {
			return concatenateUnits(BYTES.getUnits(), KILO_BYTES.getUnits(), MEGA_BYTES.getUnits(), GIGA_BYTES.getUnits(), TERA_BYTES.getUnits());
		}

		public static boolean hasUnit(String text) {
			checkNotNull(text, "text");

			final String trimmed = text.trim();
			checkArgument(!trimmed.isEmpty(), "argument is an empty- or whitespace-only string");

			final int len = trimmed.length();
			int pos = 0;

			char current;
			while (pos < len && (current = trimmed.charAt(pos)) >= '0' && current <= '9') {
				pos++;
			}

			final String unit = trimmed.substring(pos).trim().toLowerCase(Locale.US);

			return unit.length() > 0;
		}

		private static String concatenateUnits(final String[]... allUnits) {
			final StringBuilder builder = new StringBuilder(128);

			for (String[] units : allUnits) {
				builder.append('(');

				for (String unit : units) {
					builder.append(unit);
					builder.append(" | ");
				}

				builder.setLength(builder.length() - 3);
				builder.append(") / ");
			}

			builder.setLength(builder.length() - 3);
			return builder.toString();
		}

	}
}
