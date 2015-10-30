
BasePath="/user/mgroup3"
DataPath="data"
BaseOutputPath="results"
SubOutputPath="WeeklyEnsembleNS"
 

hadoop fs -rmr ${BasePath}/${BaseOutputPath}/${SubOutputPath}

hadoop jar ../../target/CSC582Project-1.jar edu.clu.cs.WeeklyEnsembleNSHourlyBinDriver -i ${BasePath}/${DataPath} -o ${BasePath}/${BaseOutputPath}/${SubOutputPath}
