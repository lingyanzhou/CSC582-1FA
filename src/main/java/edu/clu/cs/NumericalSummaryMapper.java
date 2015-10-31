package edu.clu.cs;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NumericalSummaryMapper extends
		Mapper<LongWritable, TripDataTuple, Text, NumericalSummaryTuple> {
	private NumericalSummaryTuple m_passengerCountVal = new NumericalSummaryTuple();
	private NumericalSummaryTuple m_tripTimeInSecsVal = new NumericalSummaryTuple();
	private NumericalSummaryTuple m_tripDistanceVal = new NumericalSummaryTuple();
	private NumericalSummaryTuple m_pickupLongitudeVal = new NumericalSummaryTuple();
	private NumericalSummaryTuple m_pickupLatitudeVal = new NumericalSummaryTuple();
	private NumericalSummaryTuple m_dropoffLongitudeVal = new NumericalSummaryTuple();
	private NumericalSummaryTuple m_dropoffLatitudeVal = new NumericalSummaryTuple();
	private NumericalSummaryTuple m_fareAmountVal = new NumericalSummaryTuple();
	private NumericalSummaryTuple m_surchargeVal = new NumericalSummaryTuple();
	private NumericalSummaryTuple m_mtaTaxVal = new NumericalSummaryTuple();
	private NumericalSummaryTuple m_tipAmountVal = new NumericalSummaryTuple();
	private NumericalSummaryTuple m_tollsAmountVal = new NumericalSummaryTuple();
	private NumericalSummaryTuple m_totalAmountVal = new NumericalSummaryTuple();
	private static Text m_passengerCountKey = new Text("passenger_count");
	private static Text m_tripTimeInSecsKey = new Text("trip_time_in_secs");
	private static Text m_tripDistanceKey = new Text("trip_distance");
	private static Text m_pickupLongitudeKey = new Text("pickup_longitude");
	private static Text m_pickupLatitudeKey = new Text("pickup_latitude");
	private static Text m_dropoffLongitudeKey = new Text("dropoff_longitude");
	private static Text m_dropoffLatitudeKey = new Text("dropoff_latitude");
	private static Text m_fareAmountKey = new Text("fare_amount");
	private static Text m_surchargeKey = new Text("surcharge");
	private static Text m_mtaTaxKey = new Text("mta_tax");
	private static Text m_tipAmountKey = new Text("tip_amount");
	private static Text m_tollsAmountKey = new Text("tolls_amount");
	private static Text m_totalAmountKey = new Text("total_amount");
	
	@Override
	public void setup(Context context) {
		m_passengerCountVal.clear();
		m_tripTimeInSecsVal.clear();
		m_tripDistanceVal.clear();
		m_pickupLongitudeVal.clear();
		m_pickupLatitudeVal.clear();
		m_dropoffLongitudeVal.clear();
		m_dropoffLatitudeVal.clear();
		m_fareAmountVal.clear();
		m_surchargeVal.clear();
		m_mtaTaxVal.clear();
		m_tipAmountVal.clear();
		m_tollsAmountVal.clear();
		m_totalAmountVal.clear();
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		context.write(m_passengerCountKey, m_passengerCountVal);
		context.write(m_tripTimeInSecsKey, m_tripTimeInSecsVal);
		context.write(m_tripDistanceKey, m_tripDistanceVal);
		context.write(m_pickupLongitudeKey, m_pickupLongitudeVal);
		context.write(m_pickupLatitudeKey, m_pickupLatitudeVal);
		context.write(m_dropoffLongitudeKey, m_dropoffLongitudeVal);
		context.write(m_dropoffLatitudeKey, m_dropoffLatitudeVal);
		context.write(m_fareAmountKey, m_fareAmountVal);
		context.write(m_surchargeKey, m_surchargeVal);
		context.write(m_mtaTaxKey, m_mtaTaxVal);
		context.write(m_tipAmountKey, m_tipAmountVal);
		context.write(m_tollsAmountKey, m_tollsAmountVal);
		context.write(m_totalAmountKey, m_totalAmountVal);
	}

	@Override
	public void map(LongWritable key, TripDataTuple value, Context context)
			throws IOException, InterruptedException {
		m_passengerCountVal.putRecord(value.getPassengerCount());
		m_tripTimeInSecsVal.putRecord(value.getTripTimeInSecs());
		m_tripDistanceVal.putRecord(value.getTripDistance());
		m_pickupLongitudeVal.putRecord(value.getPickupLongitude());
		m_pickupLatitudeVal.putRecord(value.getPickupLatitude());
		m_dropoffLongitudeVal.putRecord(value.getDropoffLongitude());
		m_dropoffLatitudeVal.putRecord(value.getDropoffLatitude());
		m_fareAmountVal.putRecord(value.getFareAmount());
		m_surchargeVal.putRecord(value.getSurcharge());
		m_mtaTaxVal.putRecord(value.getMtaTax());
		m_tipAmountVal.putRecord(value.getTipAmount());
		m_tollsAmountVal.putRecord(value.getTollsAmount());
		m_totalAmountVal.putRecord(value.getTotalAmount());
		
	}
}
