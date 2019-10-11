package org.apache.flink.formats.csv;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.formats.csv.RowCsvInputFormatTest.PATH;
import static org.apache.flink.formats.csv.RowCsvInputFormatTest.createTempFile;
import static org.junit.Assert.assertEquals;

/**
 * Test split logic for {@link RowCsvInputFormat}.
 */
public class RowCsvInputFormatSplitTest {

	@Test
	public void readAll() throws Exception {
		String content = "11$\n1,222\n" + "22$2,333\n";
		List<Row> rows = testEscapeLineDelimiterInField(content, 0, -1, '$');
		assertEquals(Arrays.asList(Row.of("11\n1", "222"), Row.of("222", "333")), rows);
	}

	@Test
	public void readStartOffset() throws Exception {
		String content = "11$\n1,222\n" + "22$2,333\n";
		List<Row> rows = testEscapeLineDelimiterInField(content, 1, -1, '$');
		assertEquals(Collections.singletonList(Row.of("222", "333")), rows);
	}

	@Test
	public void readStartOffsetWithSeparator() throws Exception {
		String content = "11$\n1,222\n" + "22$2,333\n";
		List<Row> rows = testEscapeLineDelimiterInField(content, 3, -1, '$');
		assertEquals(Collections.singletonList(Row.of("222", "333")), rows);
	}

	@Test
	public void readLengthWithSeparator() throws Exception {
		String content = "11$\n1,222\n" + "22$\n2,333\n";
		List<Row> rows = testEscapeLineDelimiterInField(content, 0, 13, '$');
		assertEquals(Arrays.asList(Row.of("11\n1", "222"), Row.of("22\n2", "333")), rows);
	}

	@Test
	public void readLengthWithMultiBytesEscapeChar() throws Exception {
		String content = "11好\n1,222\n" + "22好\n2,333\n";
		List<Row> rows = testEscapeLineDelimiterInField(content, 0, 13, '好');
		assertEquals(Arrays.asList(Row.of("11\n1", "222"), Row.of("22\n2", "333")), rows);
	}

	@Test
	public void readLengthWithMultiBytesEscapeChar2() throws Exception {
		String content = "11好\n1,222\n" + "22好\n2,333\n";
		List<Row> rows = testEscapeLineDelimiterInField(content, 0, 16, '好');
		assertEquals(Arrays.asList(Row.of("11\n1", "222"), Row.of("22\n2", "333")), rows);
	}

	@Test
	public void readLengthWithMultiBytesEscapeChar3() throws Exception {
		String content = "11好\n1,222\n" + "22好\n2,333\n";
		List<Row> rows = testEscapeLineDelimiterInField(content, 0, 18, '好');
		assertEquals(Arrays.asList(Row.of("11\n1", "222"), Row.of("22\n2", "333")), rows);
	}

	@Test
	public void readStartOffsetAndLength() throws Exception {
		String content = "11好\n1,222\n" + "22好\n2,333\n";
		List<Row> rows = testEscapeLineDelimiterInField(content, 3, 18, '好');
		assertEquals(Collections.singletonList(Row.of("22\n2", "333")), rows);
	}

	@Test
	public void readMultiLineSeparator() throws Exception {
		String content = "111,222\r\n" + "222,333\r\n";
		List<Row> rows = testEscapeLineDelimiterInField(content, 3, 18, '好');
		assertEquals(Collections.singletonList(Row.of("222", "333")), rows);
	}

	@Test
	public void readRLineSeparator() throws Exception {
		String content = "111,222\r" + "222,333\r";
		List<Row> rows = testEscapeLineDelimiterInField(content, 3, 18, '好');
		assertEquals(Collections.singletonList(Row.of("222", "333")), rows);
	}

	private List<Row> testEscapeLineDelimiterInField(String content, long offset, long length, char escapeChar) throws Exception {
		FileInputSplit split = createTempFile(content, offset, length);

		TypeInformation[] fieldTypes = new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO};
		RowCsvInputFormat.Builder builder = RowCsvInputFormat.builder(new RowTypeInfo(fieldTypes), PATH)
				.setEscapeCharacter(escapeChar);

		RowCsvInputFormat format = builder.build();
		format.configure(new Configuration());
		format.open(split);

		List<Row> rows = new ArrayList<>();
		while (!format.reachedEnd()) {
			Row result = new Row(3);
			result = format.nextRecord(result);
			if (result == null) {
				break;
			} else {
				rows.add(result);
			}
		}
		return rows;
	}
}
