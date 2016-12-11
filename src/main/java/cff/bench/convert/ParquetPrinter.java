package cff.bench.convert;

import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.tools.read.SimpleReadSupport;
import org.apache.parquet.tools.read.SimpleRecord;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class ParquetPrinter {

	public class Commands {
		@Parameter(names = { "-parquetFile" }, description = "Input file")
		private String parquetFile;
	}

	private void printParquet(String[] args) throws IllegalArgumentException, IOException {
		final Commands commands = new Commands();
		new JCommander(commands, args);

		try (ParquetReader<SimpleRecord> reader = ParquetReader
				.builder(new SimpleReadSupport(), new Path(commands.parquetFile)).build();
				final PrintWriter writer = new PrintWriter(System.out, true);) {
			for (SimpleRecord value = reader.read(); value != null; value = reader.read()) {
				value.prettyPrint(writer);
				writer.println();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		new ParquetPrinter().printParquet(args);
	}

}
