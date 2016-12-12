package cff.bench.convert.csv;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.junit.Test;

import cff.bench.convert.ConvertCSVToORC;

public class ConvertToOrcTest {

	@Test
	public void testConvertWorks() throws IOException {
		new ConvertCSVToORC().convertToORC("src/test/resources/testFile.csv", "src/test/resources/testFile_orc.schema",
				"out", "|");

		Reader reader = OrcFile.createReader(new Path("out"), OrcFile.readerOptions(new Configuration()));
		RecordReader rows = reader.rows();
		VectorizedRowBatch batch = reader.getSchema().createRowBatch();

		int rowTotal = 0;

		StringBuilder sb = new StringBuilder();
		while (rows.nextBatch(batch)) {
//			System.out.println("Batch size:" + batch.size);
			for (int r = 0; r < batch.size; ++r) {
				sb.setLength(0);
				sb.append("Row: ");
				for (int i = 0; i < batch.cols.length; i++) {
					ColumnVector cv = batch.cols[i];
					switch (reader.getSchema().getChildren().get(i).getCategory()) {
					case DECIMAL:
					case DOUBLE:
					case FLOAT:
						DoubleColumnVector dcv = ((DoubleColumnVector) cv);
						dcv.stringifyValue(sb, r);
						break;
					case LIST:
						break;
					case INT:
					case SHORT:
					case LONG:
						LongColumnVector lcv = ((LongColumnVector) cv);
						lcv.stringifyValue(sb, r);
						break;
					case MAP:
						break;
					case STRING:
					case BINARY:
					case CHAR:
						BytesColumnVector bcv = ((BytesColumnVector) cv);
						sb.append(bcv.toString(r));
						break;
					case BOOLEAN:
					case BYTE:
					case DATE:
					case STRUCT:
					case TIMESTAMP:
					case UNION:
					case VARCHAR:
					default:
						throw new UnsupportedOperationException(
								"Does not support: " + reader.getSchema().getChildren().get(i).getCategory());
					}

					sb.append(" ");
				}
//				System.out.println(sb.toString());

			}
			rowTotal = batch.size;
		}
		rows.close();

		assertEquals(10, rowTotal);

		new File("out").delete();
	}

}
