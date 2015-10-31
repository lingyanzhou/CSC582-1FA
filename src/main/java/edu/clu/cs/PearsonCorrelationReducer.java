package edu.clu.cs;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PearsonCorrelationReducer extends
		Reducer<Text, PearsonCorrelationTuple, Text, PearsonCorrelationTuple> {
	private PearsonCorrelationTuple result = new PearsonCorrelationTuple();

	@Override
	public void reduce(Text key, Iterable<PearsonCorrelationTuple> values, Context context)
			throws IOException, InterruptedException {
		result.clear();
		for (PearsonCorrelationTuple partialSummary : values) {
			result.putPartialResult(partialSummary);
		}
		context.write(key, result);
	}
}
