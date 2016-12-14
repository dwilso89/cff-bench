package cff.bench.print;


import org.apache.hadoop.conf.Configuration;
import org.apache.orc.tools.FileDump;

public class ORCPrinter {

//	public class Commands {
//		@Parameter(names = { "-orcFile" }, description = "Input file")
//		private String orcFile;
//	}
//
//	private void printORC(String[] args) throws Exception {
//		final Commands commands = new Commands();
//		new JCommander(commands, args);
//
//		FileDump.main(new Configuration(), args);
//		
//		Reader reader = OrcFile.createReader(new Path(commands.orcFile), OrcFile.readerOptions(new Configuration()));
//		RecordReader rows = reader.rows();
//		VectorizedRowBatch batch = reader.getSchema().createRowBatch();
//
//		StringBuilder sb = new StringBuilder();
//		while (rows.nextBatch(batch)) {
//			for (int r = 0; r < batch.size; ++r) {
//				sb.setLength(0);
//				sb.append("Row: ");
//				for (int i = 0; i < batch.cols.length; i++) {
//					ColumnVector cv = batch.cols[i];
//					switch (reader.getSchema().getChildren().get(i).getCategory()) {
//					case DECIMAL:
//						DecimalColumnVector decCV = (DecimalColumnVector) cv;
//						decCV.stringifyValue(sb, r);
//						break;
//					case DOUBLE:
//					case FLOAT:
//						DoubleColumnVector dcv = ((DoubleColumnVector) cv);
//						dcv.stringifyValue(sb, r);
//						break;
//					case LIST:
//						break;
//					case INT:
//					case SHORT:
//					case LONG:
//						LongColumnVector lcv = ((LongColumnVector) cv);
//						lcv.stringifyValue(sb, r);
//						break;
//					case MAP:
//						break;
//					case STRING:
//					case CHAR:
//						BytesColumnVector sCV = ((BytesColumnVector) cv);
//						sb.append(sCV.toString(r));
//						break;
//					case BINARY:
//						BytesColumnVector bcv = ((BytesColumnVector) cv);
//						bcv.stringifyValue(sb, r);
//						break;
//					case BOOLEAN:
//					case BYTE:
//					case DATE:
//					case STRUCT:
//					case TIMESTAMP:
//					case UNION:
//					case VARCHAR:
//					default:
//						throw new UnsupportedOperationException(
//								"Does not support: " + reader.getSchema().getChildren().get(i).getCategory());
//					}
//					sb.append(" ");
//				}
//				System.out.println(sb.toString());
//			}
//		}
//		rows.close();
//	}

	public static void main(String[] args) throws Exception {
		FileDump.main(new Configuration(), args);
	}

}
