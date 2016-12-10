package cff.bench.convert.csv;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.tools.read.SimpleReadSupport;
import org.apache.parquet.tools.read.SimpleRecord;
import org.apache.parquet.tools.read.SimpleRecord.NameValue;
import org.junit.Test;

public class ConvertToParquestTest {

	@Test
	public void testConvertWorks() throws IOException {
		ConvertUtils.convertCsvToParquet(new File("src/test/resources/testFile.csv"), new File("tmp"), "|", true);

		try (ParquetReader<SimpleRecord> reader = ParquetReader.builder(new SimpleReadSupport(), new Path("tmp"))
				.build();) {
			final List<NameValue> value = reader.read().getValues();
			assertEquals("C1", value.get(0).getName().toString());
			assertEquals("C2", value.get(1).getName().toString());
			assertEquals("C3", value.get(2).getName().toString());
		}
		new File("tmp").delete();
	}

}

