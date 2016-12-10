package cff.bench.convert;

import java.io.File;
import java.io.IOException;

import cff.bench.convert.csv.ConvertUtils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class ConvertCSVToParquet {

	public class Commands {
		@Parameter(names = { "-csvFile" }, description = "File to convert")
		private String csvFile;
		@Parameter(names = { "-schemaFile" }, description = "Schema file")
		private String schemaFile;
		@Parameter(names = { "-parquetFile" }, description = "Output file")
		private String parquetFile;
		@Parameter(names = { "-dict" }, description = "enable dictionary")
		private boolean enableDictionary = false;
	}

	public void doSomething(String[] args) throws IOException {
		final Commands commands = new Commands();
		new JCommander(commands, args);

		ConvertUtils.convertCsvToParquet(new File(commands.csvFile), new File(
				commands.parquetFile), commands.enableDictionary);
	}

	public static void main(String[] args) throws Exception {
		new ConvertCSVToParquet().doSomething(args);
	}
}