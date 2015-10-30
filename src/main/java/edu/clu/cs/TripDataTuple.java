package edu.clu.cs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;

import org.apache.hadoop.io.Writable;

public class TripDataTuple implements Writable {
	private String m_medallion = null;
	private String m_hackLicense = null;
	private float m_vendorId = Float.NaN;
	private float m_rateCode = Float.NaN;
	private float m_storeAndFwdFlag = Float.NaN;
	private Calendar m_pickupDatetime = null;
	private Calendar m_dropoffDatetime = null;
	private float m_passengerCount = Float.NaN;
	private float m_tripTimeInSecs = Float.NaN;
	private float m_tripDistance = Float.NaN;
	private float m_pickupLongitude = Float.NaN;
	private float m_pickupLatitude = Float.NaN;
	private float m_dropoffLongitude = Float.NaN;
	private float m_dropoffLatitude = Float.NaN;
	private float m_paymentType = Float.NaN;
	private float m_fareAmount = Float.NaN;
	private float m_surcharge = Float.NaN;
	private float m_mtaTax = Float.NaN;
	private float m_tipAmount = Float.NaN;
	private float m_tollsAmount = Float.NaN;
	private float m_totalAmount = Float.NaN;

	public boolean containsNA() {
		return (null == m_medallion || null == m_hackLicense
				|| Float.isNaN(m_rateCode) 
				|| null == m_pickupDatetime || null == m_dropoffDatetime
				|| Float.isNaN(m_passengerCount)
				|| Float.isNaN(m_tripTimeInSecs) || Float.isNaN(m_tripDistance)
				|| Float.isNaN(m_pickupLongitude)
				|| Float.isNaN(m_pickupLatitude)
				|| Float.isNaN(m_dropoffLongitude)
				|| Float.isNaN(m_dropoffLatitude) || Float.isNaN(m_paymentType)
				|| Float.isNaN(m_fareAmount) || Float.isNaN(m_surcharge)
				|| Float.isNaN(m_mtaTax) || Float.isNaN(m_tipAmount)
				|| Float.isNaN(m_tollsAmount) || Float.isNaN(m_totalAmount));
	}

	public void clear() {
		m_medallion = null;
		m_hackLicense = null;
		m_vendorId = Float.NaN;
		m_rateCode = Float.NaN;
		m_storeAndFwdFlag = Float.NaN;
		m_pickupDatetime = null;
		m_dropoffDatetime = null;
		m_passengerCount = Float.NaN;
		m_tripTimeInSecs = Float.NaN;
		m_tripDistance = Float.NaN;
		m_pickupLongitude = Float.NaN;
		m_pickupLatitude = Float.NaN;
		m_dropoffLongitude = Float.NaN;
		m_dropoffLatitude = Float.NaN;
		m_paymentType = Float.NaN;
		m_fareAmount = Float.NaN;
		m_surcharge = Float.NaN;
		m_mtaTax = Float.NaN;
		m_tipAmount = Float.NaN;
		m_tollsAmount = Float.NaN;
		m_totalAmount = Float.NaN;
	}

	public void readFields(DataInput in) throws IOException {
		if (in.readBoolean()) {
			m_medallion = in.readUTF();
		}
		if (in.readBoolean()) {
			m_hackLicense = in.readUTF();
		}
		m_vendorId = in.readFloat();
		m_rateCode = in.readFloat();
		m_storeAndFwdFlag = in.readInt();
		if (in.readBoolean()) {
			if (null == m_pickupDatetime) {
				m_pickupDatetime = new GregorianCalendar();
			}
			m_pickupDatetime.setTimeInMillis(in.readLong());
		}
		if (in.readBoolean()) {
			if (null == m_dropoffDatetime) {
				m_dropoffDatetime = new GregorianCalendar();
			}
			m_dropoffDatetime.setTimeInMillis(in.readLong());
		}
		m_passengerCount = in.readFloat();
		m_tripTimeInSecs = in.readFloat();
		m_tripDistance = in.readFloat();
		m_pickupLongitude = in.readFloat();
		m_pickupLatitude = in.readFloat();
		m_dropoffLongitude = in.readFloat();
		m_dropoffLatitude = in.readFloat();
		m_paymentType = in.readFloat();
		m_fareAmount = in.readFloat();
		m_surcharge = in.readFloat();
		m_mtaTax = in.readFloat();
		m_tipAmount = in.readFloat();
		m_tollsAmount = in.readFloat();
		m_totalAmount = in.readFloat();
	}

	public void write(DataOutput out) throws IOException {
		if (null == m_medallion) {
			out.writeBoolean(false);
		} else {
			out.writeBoolean(true);
			out.writeUTF(m_medallion);
		}
		if (null == m_hackLicense) {
			out.writeBoolean(false);
		} else {
			out.writeBoolean(true);
			out.writeUTF(m_hackLicense);
		}
		out.writeFloat(m_vendorId);
		out.writeFloat(m_rateCode);
		out.writeFloat(m_storeAndFwdFlag);

		if (null == m_pickupDatetime) {
			out.writeBoolean(false);
		} else {
			out.writeBoolean(true);
			out.writeLong(m_pickupDatetime.getTimeInMillis());
		}

		if (null == m_dropoffDatetime) {
			out.writeBoolean(false);
		} else {
			out.writeBoolean(true);
			out.writeLong(m_dropoffDatetime.getTimeInMillis());
		}
		out.writeFloat(m_passengerCount);
		out.writeFloat(m_tripTimeInSecs);
		out.writeFloat(m_tripDistance);
		out.writeFloat(m_pickupLongitude);
		out.writeFloat(m_pickupLatitude);
		out.writeFloat(m_dropoffLongitude);
		out.writeFloat(m_dropoffLatitude);
		out.writeFloat(m_paymentType);
		out.writeFloat(m_fareAmount);
		out.writeFloat(m_surcharge);
		out.writeFloat(m_mtaTax);
		out.writeFloat(m_tipAmount);
		out.writeFloat(m_tollsAmount);
		out.writeFloat(m_totalAmount);
	}

	public String getMedallion() {
		return m_medallion;
	}

	public void setMedallion(String medallion) {
		this.m_medallion = medallion;
	}

	public String getHacklicense() {
		return m_hackLicense;
	}

	public void setHackLicense(String hackLicense) {
		this.m_hackLicense = hackLicense;
	}

	public float getVendorId() {
		return m_vendorId;
	}

	public void setVendorId(float f) {
		this.m_vendorId = f;
	}

	public float getRateCode() {
		return m_rateCode;
	}

	public void setRateCode(float rateCode) {
		this.m_rateCode = rateCode;
	}

	public float getStoreAndFwdFlag() {
		return m_storeAndFwdFlag;
	}

	public void setStoreAndFwdFlag(float f) {
		this.m_storeAndFwdFlag = f;
	}

	public Calendar getPickupDatetime() {
		return m_pickupDatetime;
	}

	public void setPickupDatetime(long millis) {
		if (null == m_pickupDatetime) {
			m_pickupDatetime = new GregorianCalendar();
		}
		this.m_pickupDatetime.setTimeInMillis(millis);
	}

	public Calendar getDropoffDatetime() {
		return m_dropoffDatetime;
	}

	public void setDropoffDatetime(long millis) {
		if (null == m_dropoffDatetime) {
			m_dropoffDatetime = new GregorianCalendar();
		}
		this.m_dropoffDatetime.setTimeInMillis(millis);
	}

	public float getPassengerCount() {
		return m_passengerCount;
	}

	public void setPassengerCount(float val) {
		this.m_passengerCount = val;
	}

	public float getTripTimeInSecs() {
		return m_tripTimeInSecs;
	}

	public void setTripTimeInSecs(float tripTimeInSecs) {
		this.m_tripTimeInSecs = tripTimeInSecs;
	}

	public float getTripDistance() {
		return m_tripDistance;
	}

	public void setTripDistance(float tripDistance) {
		this.m_tripDistance = tripDistance;
	}

	public float getPickupLongitude() {
		return m_pickupLongitude;
	}

	public void setPickupLongitude(float m_pickupLongitude) {
		this.m_pickupLongitude = m_pickupLongitude;
	}

	public float getPickupLatitude() {
		return m_pickupLatitude;
	}

	public void setPickupLatitude(float pickupLatitude) {
		this.m_pickupLatitude = pickupLatitude;
	}

	public float getDropoffLongitude() {
		return m_dropoffLongitude;
	}

	public void setDropoffLongitude(float dropoffLongitude) {
		this.m_dropoffLongitude = dropoffLongitude;
	}

	public float getDropoffLatitude() {
		return m_dropoffLatitude;
	}

	public void setDropoffLatitude(float dropoffLatitude) {
		this.m_dropoffLatitude = dropoffLatitude;
	}

	public float getPaymentType() {
		return m_paymentType;
	}

	public void setPaymentType(float f) {
		this.m_paymentType = f;
	}

	public float getFareAmount() {
		return m_fareAmount;
	}

	public void setFareAmount(float fareAmount) {
		this.m_fareAmount = fareAmount;
	}

	public float getSurcharge() {
		return m_surcharge;
	}

	public void setSurcharge(float surcharge) {
		this.m_surcharge = surcharge;
	}

	public float getMtaTax() {
		return m_mtaTax;
	}

	public void setMtaTax(float mtaTax) {
		this.m_mtaTax = mtaTax;
	}

	public float getTipAmount() {
		return m_tipAmount;
	}

	public void setTipAmount(float tipAmount) {
		this.m_tipAmount = tipAmount;
	}

	public float getTollsAmount() {
		return m_tollsAmount;
	}

	public void setTollsAmount(float tollsAmount) {
		this.m_tollsAmount = tollsAmount;
	}

	public float getTotalAmount() {
		return m_totalAmount;
	}

	public void setTotalAmount(float totalAmount) {
		this.m_totalAmount = totalAmount;
	}
}
