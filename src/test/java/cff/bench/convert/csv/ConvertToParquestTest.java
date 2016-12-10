package cff.bench.convert.csv;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

public class ConvertToParquestTest {

	@Test
	public void testConvertWorks() throws IOException{
		ConvertUtils.convertCsvToParquet(new File("src/test/resources/testFile.csv"), new File(
				"csv.parquet"), true);
	}
	
}
