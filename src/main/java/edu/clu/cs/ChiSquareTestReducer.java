package edu.clu.cs;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChiSquareTestReducer extends
		Reducer<Text, ChiSquareTestTuple, Text, FloatWritable> {
	private FloatWritable result = new FloatWritable();
	private ChiSquareTestTuple chiSquare = new ChiSquareTestTuple();

	@Override
	public void reduce(Text key, Iterable<ChiSquareTestTuple> values, Context context)
			throws IOException, InterruptedException {
		for (ChiSquareTestTuple partialSummary : values) {
			chiSquare.putPartialResult(partialSummary);
		}
		result.set(chiSquare.getResult());
		context.write(key, result);
	}
}
