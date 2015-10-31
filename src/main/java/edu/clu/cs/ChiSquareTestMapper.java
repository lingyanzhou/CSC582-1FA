package edu.clu.cs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ChiSquareTestMapper extends
		Mapper<LongWritable, TripDataTuple, Text, ChiSquareTestTuple> {

	private static String[] s_colNames = { "vender_id", "payment_type",
			"store_fwd_flag", "rate_code" };
	private static Factor[] s_factors = {
			TripDataFormatHelper.s_venderIdFactor,
			TripDataFormatHelper.s_paymentTypeFactor,
			TripDataFormatHelper.s_storeFwdFlagFactor,
			TripDataFormatHelper.s_rateCodeFactor };

	private ChiSquareTestTuple[] m_vals = new ChiSquareTestTuple[6];
	private static Text[] s_keys = new Text[6];

	static {
		int ind = 0;
		for (int i = 0; i < 3; ++i) {
			for (int j = i + 1; j < 4; ++j) {
				s_keys[ind] = new Text(s_colNames[i] + "_X_" + s_colNames[j]);
				ind += 1;
			}
		}
	}
//
//	private static int getIndex(int i, int j) {
//		return j - i - 1 + (7 - i) * i / 2;
//	}

	public ChiSquareTestMapper() {
		for (int i = 0; i < 6; ++i) {
			m_vals[i] = new ChiSquareTestTuple();
		}
		int ind = 0;
		for (int i = 0; i < 3; ++i) {
			for (int j = i + 1; j < 4; ++j) {
				m_vals[ind].init(s_factors[i], s_factors[j]);
				ind += 1;
			}
		}
	}

	@Override
	public void setup(Context context) {
//		for (int i = 0; i < 6; ++i) {
//			m_vals[i].clear();
//		}
	}

	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {

		for (int i = 0; i < 6; ++i) {
			context.write(s_keys[i], m_vals[i]);
		}
	}

	@Override
	public void map(LongWritable key, TripDataTuple value, Context context)
			throws IOException, InterruptedException {
		
		float[] vals = { value.getVendorId(), value.getPaymentType(),
				value.getStoreAndFwdFlag(), value.getRateCode() };

		int ind = 0;
		for (int i = 0; i < 3; ++i) {
			for (int j = i + 1; j < 4; ++j) {
				m_vals[ind].putRecord(vals[i], vals[j]);
				ind+=1;
			}
		}

	}
}
