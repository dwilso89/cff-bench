package cff.bench.convert;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class ConvertCSVToORC {

	public class Commands {
		@Parameter(names = { "-csvFile" }, description = "File to convert")
		private String csvFile;
		@Parameter(names = { "-delimiter" }, description = "CSV delimiter")
		private String delimiter = ",";
		@Parameter(names = { "-schemaFile" }, description = "Schema file")
		private String schemaFile;
		@Parameter(names = { "-orcFile" }, description = "Output file")
		private String orcFile;
	}

	public void convertToORC(String[] args) throws IllegalArgumentException, IOException {
		final Commands commands = new Commands();
		new JCommander(commands, args);

		convertToORC(commands.csvFile, IOUtils.toString(new FileInputStream(new File(commands.schemaFile))),
				commands.orcFile, commands.delimiter);
	}

	public void convertToORC(String in, String schemaString, String out, String delim)
			throws FileNotFoundException, IOException {
		final TypeDescription schema = TypeDescription
				.fromString(IOUtils.toString(new FileInputStream(new File(schemaString))));
		final Writer writer = OrcFile.createWriter(new Path(out),
				OrcFile.writerOptions(new Configuration()).setSchema(schema));
		final VectorizedRowBatch batch = schema.createRowBatch();

		try (BufferedReader br = new BufferedReader(new FileReader(new File(in)));) {
			final String d = Pattern.quote(delim);
			int rowNum = 0;
			String line = "";
			while ((line = br.readLine()) != null) {
				String[] row = line.split(d);
				for (int col = 0; col < row.length; col++) {
					writeToColumn(schema, batch, col, rowNum, row[col]);
				}
				rowNum++;
				batch.size = rowNum;
				if (batch.size == batch.getMaxSize()) {
					writer.addRowBatch(batch);
					batch.reset();
				}
			}
		}
		writer.addRowBatch(batch);
		writer.close();

	}

	private void writeToColumn(TypeDescription schema, VectorizedRowBatch batch, int column, int row, String value) {
		switch (schema.getChildren().get(column).getCategory()) {
		case BINARY:
		case CHAR:
		case STRING:
			BytesColumnVector b = (BytesColumnVector) batch.cols[column];
			b.setVal(row, value.getBytes());
			break;
		case DECIMAL:
		case DOUBLE:
		case FLOAT:
			DoubleColumnVector d = (DoubleColumnVector) batch.cols[column];
			d.vector[row] = Double.parseDouble(value);
			break;
		case INT:
		case LONG:
		case SHORT:
			LongColumnVector l = (LongColumnVector) batch.cols[column];
			l.vector[row] = Long.parseLong(value);
			break;
		case BOOLEAN:
		case BYTE:
		case DATE:
		case LIST:
		case MAP:
		case STRUCT:
		case TIMESTAMP:
		case UNION:
		case VARCHAR:
		default:
			throw new UnsupportedOperationException("EXCEPTION!!!");
		}
	}

	public static void main(String[] args) {

	}

}
