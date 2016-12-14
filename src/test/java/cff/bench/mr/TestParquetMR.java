package cff.bench.mr;

import static org.junit.Assert.assertEquals;

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

		assertEquals(schema.toString(),
				"{\"type\":\"record\",\"name\":\"reading\",\"fields\":[{\"name\":\"station\",\"type\":\"int\"},{\"name\":\"wban\",\"type\":\"int\"},{\"name\":\"year_moda\",\"type\":\"int\"},{\"name\":\"temperature\",\"type\":[\"null\",\"float\"],\"default\":null},{\"name\":\"temperature_count\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"dew_point\",\"type\":[\"null\",\"float\"],\"default\":null},{\"name\":\"dew_point_count\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"sea_level_pressure\",\"type\":[\"null\",\"float\"],\"default\":null},{\"name\":\"sea_level_pressure_count\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"station_pressure\",\"type\":[\"null\",\"float\"],\"default\":null},{\"name\":\"station_pressure_count\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"visibility\",\"type\":[\"null\",\"float\"],\"default\":null},{\"name\":\"visibility_count\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"wind_speed\",\"type\":[\"null\",\"float\"],\"default\":null},{\"name\":\"wind_speed_count\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"sustained_wind_speed\",\"type\":[\"null\",\"float\"],\"default\":null},{\"name\":\"max_wind_gust\",\"type\":[\"null\",\"float\"],\"default\":null},{\"name\":\"max_temperature\",\"type\":[\"null\",\"float\"],\"default\":null},{\"name\":\"max_temperature_flag\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"min_temperature\",\"type\":[\"null\",\"float\"],\"default\":null},{\"name\":\"min_temperature_flag\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"precipitation_amount\",\"type\":[\"null\",\"float\"],\"default\":null},{\"name\":\"precipitation_flag\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"snow_depth\",\"type\":[\"null\",\"float\"],\"default\":null},{\"name\":\"indicators\",\"type\":[\"null\",\"int\"],\"default\":null}]}");
	}

}
