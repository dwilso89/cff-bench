/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cff.bench.convert.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import com.google.common.io.Closeables;

public class ConvertUtils {

	public static final String CSV_DELIMITER = "|";

	private static String readFile(String path) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(path));
		StringBuilder stringBuilder = new StringBuilder();

		try {
			String line = null;
			String ls = System.getProperty("line.separator");

			while ((line = reader.readLine()) != null) {
				stringBuilder.append(line);
				stringBuilder.append(ls);
			}
		} finally {
			Closeables.closeQuietly(reader);
		}

		return stringBuilder.toString();
	}

	public static String getSchema(File csvFile) throws IOException {
		String fileName = csvFile.getName().substring(0, csvFile.getName().length() - ".csv".length()) + ".schema";
		File schemaFile = new File(csvFile.getParentFile(), fileName);
		return readFile(schemaFile.getAbsolutePath());
	}

	public static void convertCsvToParquet(File csvFile, File outputParquetFile, final String delimiter)
			throws IOException {
		convertCsvToParquet(csvFile, outputParquetFile, delimiter, false);
	}

	public static void convertCsvToParquet(File csvFile, File outputParquetFile, final String delimiter,
			boolean enableDictionary) throws IOException {
		String rawSchema = getSchema(csvFile);
		if (outputParquetFile.exists()) {
			throw new IOException("Output file " + outputParquetFile.getAbsolutePath() + " already exists");
		}

		Path path = new Path(outputParquetFile.toURI());

		MessageType schema = MessageTypeParser.parseMessageType(rawSchema);
		CsvParquetWriter writer = new CsvParquetWriter(path, schema, enableDictionary);

		BufferedReader br = new BufferedReader(new FileReader(csvFile));
		String line;
		try {
			while ((line = br.readLine()) != null) {
				String[] fields = line.split(Pattern.quote(delimiter));
				writer.write(Arrays.asList(fields));
			}

			writer.close();
		} finally {
			Closeables.closeQuietly(br);
		}
	}

	public static void convertParquetToCSV(File parquetFile, File csvOutputFile) throws IOException {
		Path parquetFilePath = new Path(parquetFile.toURI());

		Configuration configuration = new Configuration(true);

		org.apache.parquet.hadoop.example.GroupReadSupport readSupport = new GroupReadSupport();
		ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, parquetFilePath);
		MessageType schema = readFooter.getFileMetaData().getSchema();

		readSupport.init(configuration, null, schema);
		BufferedWriter w = new BufferedWriter(new FileWriter(csvOutputFile));
		ParquetReader<Group> reader = new ParquetReader<Group>(parquetFilePath, readSupport);
		try {
			Group g = null;
			while ((g = reader.read()) != null) {
				writeGroup(w, g, schema);
			}
			reader.close();
		} finally {
			w.close();
		}
	}

	private static void writeGroup(BufferedWriter w, Group g, MessageType schema) throws IOException {
		for (int j = 0; j < schema.getFieldCount(); j++) {
			if (j > 0) {
				w.write(CSV_DELIMITER);
			}
			String valueToString = g.getValueToString(j, 0);
			w.write(valueToString);
		}
		w.write('\n');
	}

	@Deprecated
	public static void convertParquetToCSVEx(File parquetFile, File csvOutputFile) throws IOException {
		Path parquetFilePath = new Path(parquetFile.toURI());

		Configuration configuration = new Configuration(true);

		// TODO Following can be changed by using ParquetReader instead of
		// ParquetFileReader
		ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, parquetFilePath);
		MessageType schema = readFooter.getFileMetaData().getSchema();
		ParquetFileReader parquetFileReader = new ParquetFileReader(configuration, parquetFilePath,
				readFooter.getBlocks(), schema.getColumns());
		BufferedWriter w = new BufferedWriter(new FileWriter(csvOutputFile));
		PageReadStore pages = null;
		try {
			while (null != (pages = parquetFileReader.readNextRowGroup())) {
				final long rows = pages.getRowCount();

				final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
				final RecordReader<Group> recordReader = columnIO.getRecordReader(pages,
						new GroupRecordConverter(schema));
				for (int i = 0; i < rows; i++) {
					final Group g = recordReader.read();
					writeGroup(w, g, schema);
				}
			}
		} finally {
			parquetFileReader.close();
			w.close();
		}
	}

}