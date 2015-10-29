
BasePath="/user/mgroup3"
DataPath="data2"
OutputPath="sample_results"
AllSummaryName="AllNumerical"
OutputSurfix="_summary"
FieldNames=(
	"rate_code"
	"passenger_count"
	"trip_time_in_secs"
	"trip_distance"
	"pickup_longitude"
	"pickup_latitude"
	"dropoff_longitude"
	"dropoff_latitude"
	"fare_amount"
	"surcharge"
	"mta_tax"
	"tip_amount"
	"tolls_amount"
	"total_amount"
 ) 


hadoop fs -cat ${BasePath}/${OutputPath}/${1}${OutputSurfix}/*




