
BasePath="/user/mgroup3"
DataPath="data"
BaseOutputPath="results"
SubOutputPath="KNNTipAmountPredictor"
 

hadoop fs -rmr ${BasePath}/${BaseOutputPath}/${SubOutputPath}

hadoop jar ../../target/CSC582Project-1.jar edu.clu.cs.KNNTipAmountPredictorDriver -i ${BasePath}/${DataPath} -o ${BasePath}/${BaseOutputPath}/${SubOutputPath} -d 1 -H 1 -k 100 -r 2 -u -73.79 -v 40.65  -V VTS -x -74.034 -y 40.724
