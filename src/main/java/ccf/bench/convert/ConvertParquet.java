package ccf.bench.convert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class ConvertParquet {

	public static class Commands {
		@Parameter
		private List<String> parameters = new ArrayList<>();
	}

	public static void main(String[] args) throws IOException {
		Commands commands = new Commands();
		new JCommander(commands, args);
		System.out.println(commands.parameters);

		// CsvParquetWriter writer = new CsvParquetWriter("", new
		// MessageType());
	}

}