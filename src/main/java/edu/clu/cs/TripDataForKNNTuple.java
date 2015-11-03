package edu.clu.cs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.io.Writable;

public class TripDataForKNNTuple implements Writable {
	private float m_vendorId = Float.NaN;
	private float m_rateCode = Float.NaN;
	private float m_pickupHour = Float.NaN;
	private float m_pickupDayOfWeek = Float.NaN;
	private float m_pickupLongitude = Float.NaN;
	private float m_pickupLatitude = Float.NaN;
	private float m_dropoffLongitude = Float.NaN;
	private float m_dropoffLatitude = Float.NaN;

	public float getDistance(TripDataForKNNTuple another) {
		float sqSum = 0;
		if (another.m_vendorId != m_vendorId) {
			sqSum += 1;
		}
		if (another.m_rateCode != m_rateCode) {
			sqSum += 1;
		}
		if (another.m_pickupHour != m_pickupHour) {
			sqSum += 1;
		}
		if (another.m_pickupDayOfWeek != m_pickupDayOfWeek) {
			sqSum += 1;
		}
		sqSum += Math.pow(
				(m_pickupLongitude - another.m_pickupLongitude) / 0.1, 2);
		sqSum += Math.pow((m_pickupLatitude - another.m_pickupLatitude) / 0.08,
				2);
		sqSum += Math.pow(
				(m_dropoffLongitude - another.m_dropoffLongitude) / 0.1, 2);
		sqSum += Math.pow(
				(m_dropoffLatitude - another.m_dropoffLatitude) / 0.08, 2);

		return (float) Math.sqrt(sqSum);
	}

	public void clear() {
		m_vendorId = Float.NaN;
		m_rateCode = Float.NaN;
		m_pickupHour = Float.NaN;
		m_pickupDayOfWeek = Float.NaN;
		m_pickupLongitude = Float.NaN;
		m_pickupLatitude = Float.NaN;
		m_dropoffLongitude = Float.NaN;
		m_dropoffLatitude = Float.NaN;
	}

	public boolean containsNA() {
		return (Float.isNaN(m_rateCode) || Float.isNaN(m_vendorId)
				|| Float.isNaN(m_pickupHour) || Float.isNaN(m_pickupDayOfWeek)
				|| Float.isNaN(m_pickupLongitude)
				|| Float.isNaN(m_pickupLatitude)
				|| Float.isNaN(m_dropoffLongitude) || Float
					.isNaN(m_dropoffLatitude));
	}

	public void readFields(DataInput in) throws IOException {
		m_vendorId = in.readFloat();
		m_rateCode = in.readFloat();
		m_pickupHour = in.readFloat();
		m_pickupDayOfWeek = in.readFloat();
		m_pickupLongitude = in.readFloat();
		m_pickupLatitude = in.readFloat();
		m_dropoffLongitude = in.readFloat();
		m_dropoffLatitude = in.readFloat();
	}

	public void write(DataOutput out) throws IOException {
		out.writeFloat(m_vendorId);
		out.writeFloat(m_rateCode);
		out.writeFloat(m_pickupHour);
		out.writeFloat(m_pickupDayOfWeek);
		out.writeFloat(m_pickupLongitude);
		out.writeFloat(m_pickupLatitude);
		out.writeFloat(m_dropoffLongitude);
		out.writeFloat(m_dropoffLatitude);
	}

	public void setFromTripDataTuple(TripDataTuple tdt) {
		if (tdt.getPaymentType() != TripDataFormatHelper.s_paymentTypeFactor
				.convertToFloat("CSH")) {
			m_vendorId = tdt.getVendorId();
			m_rateCode = tdt.getRateCode();
			Calendar pickUpTime = tdt.getPickupDatetime();
			if (null != pickUpTime) {
				m_pickupHour = pickUpTime.get(Calendar.HOUR_OF_DAY);
				m_pickupDayOfWeek = pickUpTime.get(Calendar.DAY_OF_WEEK);
			}
			m_pickupLongitude = tdt.getPickupLongitude();
			m_pickupLatitude = tdt.getPickupLatitude();
			m_dropoffLongitude = tdt.getDropoffLongitude();
			m_dropoffLatitude = tdt.getDropoffLatitude();
		}
	}

	public void setVendorId(float vendorId) {
		this.m_vendorId = vendorId;
	}

	public void setRateCode(float rateCode) {
		this.m_rateCode = rateCode;
	}

	public void setPickupHour(float pickupHour) {
		this.m_pickupHour = pickupHour;
	}

	public void setPickupDayOfWeek(float pickupDayOfWeek) {
		this.m_pickupDayOfWeek = pickupDayOfWeek;
	}

	public void setPickupLongitude(float pickupLongitude) {
		this.m_pickupLongitude = pickupLongitude;
	}

	public void setPickupLatitude(float pickupLatitude) {
		this.m_pickupLatitude = pickupLatitude;
	}

	public void setDropoffLongitude(float dropoffLongitude) {
		this.m_dropoffLongitude = dropoffLongitude;
	}

	public void setDropoffLatitude(float dropoffLatitude) {
		this.m_dropoffLatitude = dropoffLatitude;
	}
}
