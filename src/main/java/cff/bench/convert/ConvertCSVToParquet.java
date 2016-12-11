package cff.bench.convert;

import java.io.File;
import java.io.IOException;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import cff.bench.convert.csv.ConvertUtils;

public class ConvertCSVToParquet {

	public class Commands {
		@Parameter(names = { "-csvFile" }, description = "File to convert")
		private String csvFile;
		@Parameter(names = { "-delimiter" }, description = "CSV delimiter")
		private String delimiter = ",";
		@Parameter(names = { "-schemaFile" }, description = "Schema file")
		private String schemaFile;
		@Parameter(names = { "-parquetFile" }, description = "Output file")
		private String parquetFile;
		@Parameter(names = { "-dict" }, description = "enable dictionary", required = true)
		private boolean enableDictionary = false;
	}

	public void convert(String[] args) throws IOException {
		final Commands commands = new Commands();
		new JCommander(commands, args);

		ConvertUtils.convertCsvToParquet(new File(commands.csvFile), new File(commands.schemaFile),
				new File(commands.parquetFile), commands.delimiter, commands.enableDictionary);
	}

	public static void main(String[] args) throws Exception {
		new ConvertCSVToParquet().convert(args);
	}
}