package cff.bench.convert;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.tools.read.SimpleReadSupport;
import org.apache.parquet.tools.read.SimpleRecord;
import org.apache.parquet.tools.read.SimpleRecord.NameValue;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class FlatParquetPrinter {

	public class Commands {
		@Parameter(names = { "-parquetFile" }, description = "Input file")
		private String parquetFile;
	}

	private void printFlatParquet(String[] args) throws IllegalArgumentException, IOException {
		final Commands commands = new Commands();
		new JCommander(commands, args);
		
		try (ParquetReader<SimpleRecord> reader = ParquetReader.builder(new SimpleReadSupport(), new Path(commands.parquetFile))
				.build();) {
			final StringBuilder sb = new StringBuilder();
			sb.setLength(0);
			for (SimpleRecord value = reader.read(); value != null; value = reader.read()) {
				sb.append("Row\n");
				for (NameValue v : value.getValues()) {
					sb.append("\t");
					sb.append(v.getName());
					sb.append(" = ");
					if (v.getValue() instanceof byte[]) {
						sb.append(new String((byte[]) v.getValue()));
					} else {
						sb.append(v.getValue());
					}
					sb.append("\n");
				}
				System.out.print(sb.toString());
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		new FlatParquetPrinter().printFlatParquet(args);
	}

}
