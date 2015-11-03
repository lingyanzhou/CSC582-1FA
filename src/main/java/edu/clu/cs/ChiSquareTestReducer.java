package edu.clu.cs;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChiSquareTestReducer extends
		Reducer<Text, ChiSquareTestTuple, Text, ChiSquareTestTuple> {
	private FloatWritable m_result = new FloatWritable();
	private ChiSquareTestTuple m_chiSquare = new ChiSquareTestTuple();

	@Override
	public void reduce(Text key, Iterable<ChiSquareTestTuple> values, Context context)
			throws IOException, InterruptedException {
		String[] varNames = key.toString().split(",");
		Factor f1 = getFactor(varNames[0]);
		Factor f2 = getFactor(varNames[1]);
		m_chiSquare.init(f1, f2);
		for (ChiSquareTestTuple partialSummary : values) {
			m_chiSquare.putPartialResult(partialSummary);
		}
		m_result.set(m_chiSquare.getResult());
		context.write(key, m_chiSquare);
	}
	
	private Factor getFactor(String name) {
		if (name.equals("vender_id")) {
			return TripDataFormatHelper.s_venderIdFactor;
		} else if (name.equals("payment_type")) {
			return TripDataFormatHelper.s_paymentTypeFactor;
		} else if (name.equals("store_fwd_flag")) {
			return TripDataFormatHelper.s_storeFwdFlagFactor;
		} else if  (name.equals("rate_code")) {
			return TripDataFormatHelper.s_rateCodeFactor;
		}
		return null;
	}
}
