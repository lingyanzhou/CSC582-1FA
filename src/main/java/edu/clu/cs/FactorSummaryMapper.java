package edu.clu.cs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FactorSummaryMapper extends
		Mapper<LongWritable, TripDataTuple, Text, IntWritable> {
	private Text m_key = new Text();
	private IntWritable m_value = new IntWritable();
	private Map<Float, Integer> m_venderIdCounter = new HashMap<Float, Integer>();
	private Map<Float, Integer> m_paymentTypeCounter = new HashMap<Float, Integer>();
	private Map<Float, Integer> m_rateCodeCounter = new HashMap<Float, Integer>();
	private Map<Float, Integer> m_storeAndFwdFlagCounter = new HashMap<Float, Integer>();

	@Override
	public void map(LongWritable key, TripDataTuple value, Context context)
			throws IOException, InterruptedException {
		float vId = value.getVendorId();
		if (m_venderIdCounter.containsKey(vId)) {
			m_venderIdCounter.put(vId, 1 + m_venderIdCounter.get(vId));
		} else {
			m_venderIdCounter.put(vId, 1);
		}

		float pType = value.getPaymentType();
		if (m_paymentTypeCounter.containsKey(pType)) {
			m_paymentTypeCounter
					.put(pType, 1 + m_paymentTypeCounter.get(pType));
		} else {
			m_paymentTypeCounter.put(pType, 1);
		}

		float rCode = value.getRateCode();
		if (m_rateCodeCounter.containsKey(rCode)) {
			m_rateCodeCounter.put(rCode, 1 + m_rateCodeCounter.get(rCode));
		} else {
			m_rateCodeCounter.put(rCode, 1);
		}

		float sfFlag = value.getStoreAndFwdFlag();
		if (m_storeAndFwdFlagCounter.containsKey(sfFlag)) {
			m_storeAndFwdFlagCounter.put(sfFlag,
					1 + m_storeAndFwdFlagCounter.get(sfFlag));
		} else {
			m_storeAndFwdFlagCounter.put(sfFlag, 1);
		}
	}

	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {
		for (Float k : m_venderIdCounter.keySet()) {
			m_key.set("VenderId,"
					+ TripDataFormatHelper.s_venderIdFactor.convertToString(k
							.floatValue()));
			m_value.set(m_venderIdCounter.get(k));
			context.write(m_key, m_value);
		}
		for (Float k : m_paymentTypeCounter.keySet()) {
			m_key.set("PaymentType,"
					+ TripDataFormatHelper.s_paymentTypeFactor.convertToString(k
							.floatValue()));
			m_value.set(m_paymentTypeCounter.get(k));
			context.write(m_key, m_value);
		}
		for (Float k : m_rateCodeCounter.keySet()) {
			m_key.set("RateCode,"
					+ TripDataFormatHelper.s_rateCodeFactor.convertToString(k
							.floatValue()));
			m_value.set(m_rateCodeCounter.get(k));
			context.write(m_key, m_value);
		}
		for (Float k : m_storeAndFwdFlagCounter.keySet()) {
			m_key.set("StoreAndFwdFlag,"
					+ TripDataFormatHelper.s_storeFwdFlagFactor.convertToString(k
							.floatValue()));
			m_value.set(m_storeAndFwdFlagCounter.get(k));
			context.write(m_key, m_value);
		}
	}

}
