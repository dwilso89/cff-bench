package cff.bench.mr;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Test;

public class TestParquetMR {

	@Test
	public void testSchemaConversion() throws IOException {

		AvroSchemaConverter schemaConverter = new AvroSchemaConverter();
		Schema schema = schemaConverter.convert(MessageTypeParser.parseMessageType(
				IOUtils.toString(new FileInputStream(new File("src/test/resources/noaa_parquet.schema")))));
		
		System.out.println(schema);
	}

}
