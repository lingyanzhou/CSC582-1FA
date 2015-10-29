
BasePath="/user/mgroup3"
DataPath="data"
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

function join { local IFS="$1"; shift; echo "$*"; }

hadoop fs -rmr ${BasePath}/${OutputPath}/${AllSummaryName}${OutputSurfix}

if [ $1 == 'all' ]; then
    hadoop jar ../target/NumericalSummary-0.0.1.jar edu.clu.cs.NumericalSummaryDriver -c $(join , "${FieldNames[@]}") -i ${BasePath}/${DataPath} -o ${BasePath}/${OutputPath}/${AllSummaryName}${OutputSurfix}

    exit
fi

for field in "${FieldNames[@]}"
do
    hadoop fs -rmr ${BasePath}/${OutputPath}/${field}${OutputSurfix}
    hadoop jar ../target/NumericalSummary-0.0.1.jar edu.clu.cs.NumericalSummaryDriver -c ${field} -i ${BasePath}/${DataPath} -o ${BasePath}/${OutputPath}/${field}${OutputSurfix}
done




