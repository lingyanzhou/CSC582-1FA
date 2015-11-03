package edu.clu.cs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class KNNTipAmountPredictorMapper extends
		Mapper<LongWritable, TripDataTuple, Text, FloatWritable> {
	private static final String s_configVendorId = "KNNTipAmountPredictorMapper_vendorId";
	private static final String s_configRateCode = "KNNTipAmountPredictorMapper_rateCode";
	private static final String s_configPickupHour = "KNNTipAmountPredictorMapper_pickupHour";
	private static final String s_configPickupDayOfWeek = "KNNTipAmountPredictorMapper_pickupDayOfWeek";
	private static final String s_configPickupLongitude = "KNNTipAmountPredictorMapper_pickupLongitude";
	private static final String s_configPickupLatitude = "KNNTipAmountPredictorMapper_pickupLatitude";
	private static final String s_configDropoffLongitude = "KNNTipAmountPredictorMapper_dropoffLongitude";
	private static final String s_configDropoffLatitude = "KNNTipAmountPredictorMapper_dropoffLatitude";
	private FloatWritable m_value = new FloatWritable();
	private TripDataForKNNTuple m_target = new TripDataForKNNTuple();
	private TripDataForKNNTuple m_tmpTuple = new TripDataForKNNTuple();
	private Text m_key = new Text();

	public static void setTarget(Configuration conf, String venderId,
			String rateCode, int hour, int dayOfWeek, float pickupLat,
			float pickupLon, float dropoffLat, float dropoffLon) {
		conf.set(s_configVendorId, venderId);
		conf.set(s_configRateCode, rateCode);
		conf.setInt(s_configPickupHour, hour);
		conf.setInt(s_configPickupDayOfWeek, dayOfWeek);
		conf.setFloat(s_configPickupLongitude, pickupLon);
		conf.setFloat(s_configPickupLatitude, pickupLat);
		conf.setFloat(s_configDropoffLongitude, dropoffLon);
		conf.setFloat(s_configDropoffLatitude, dropoffLat);
	}

	@Override
	public void setup(Context context) {
		m_target.setVendorId(TripDataFormatHelper.s_venderIdFactor
				.convertToFloat(context.getConfiguration()
						.get(s_configVendorId, "VTS")));
		m_target.setRateCode(TripDataFormatHelper.s_rateCodeFactor
				.convertToFloat(context.getConfiguration()
						.get(s_configRateCode)));
		m_target.setPickupHour(context.getConfiguration()
						.getInt(s_configPickupHour, 1));
		m_target.setPickupDayOfWeek(context.getConfiguration()
				.getInt(s_configPickupDayOfWeek, 1));
		m_target.setPickupLongitude(context.getConfiguration()
				.getFloat(s_configPickupLongitude, 1));
		m_target.setPickupLatitude(context.getConfiguration()
				.getFloat(s_configPickupLatitude, 1));
		m_target.setDropoffLongitude(context.getConfiguration()
				.getFloat(s_configDropoffLongitude, 1));
		m_target.setDropoffLatitude(context.getConfiguration()
				.getFloat(s_configDropoffLatitude, 1));
	}

	@Override
	public void map(LongWritable key, TripDataTuple value, Context context)
			throws IOException, InterruptedException {
		m_tmpTuple.clear();
		m_tmpTuple.setFromTripDataTuple(value);
		if (!m_tmpTuple.containsNA()) {
			m_value.set(m_target.getDistance(m_tmpTuple));
			m_key.set(Long.toString(key.get()));
			context.write(m_key, m_value);
		}
	}

}
