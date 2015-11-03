BasePath="/user/mgroup3"
BaseOutputPath="results"
SubOutputPath="AutoCorrelation"

hadoop fs -cat ${BasePath}/${BaseOutputPath}/${SubOutputPath}/HourlyTipSum/*

hadoop fs -cat ${BasePath}/${BaseOutputPath}/${SubOutputPath}/HourlyPickupCount/*




