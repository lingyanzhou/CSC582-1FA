
BasePath="/user/mgroup3"
DataPath="data2"
BaseOutputPath="sample_results"
SubOutputPath="LatLonBinnedAverageTip"
 

hadoop fs -rmr ${BasePath}/${BaseOutputPath}/${SubOutputPath}

hadoop jar ../../target/CSC582Project-1.jar edu.clu.cs.LatLonBinnedAverageTipDriver -i ${BasePath}/${DataPath} -o ${BasePath}/${BaseOutputPath}/${SubOutputPath}
