package edu.clu.cs;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NumericalSummaryReducer extends
		Reducer<Text, NumericalSummaryTuple, Text, NumericalSummaryTuple> {
	private NumericalSummaryTuple result = new NumericalSummaryTuple();

	public void reduce(Text key, Iterable<NumericalSummaryTuple> values, Context context)
			throws IOException, InterruptedException {
		result.clear();
		for (NumericalSummaryTuple partialSummary : values) {
			result.putPartialSummary(partialSummary);
		}
		context.write(key, result);
	}
}
