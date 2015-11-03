
BasePath="/user/mgroup3"
DataPath="results/LatLonBinnedAverageTip"
BaseOutputPath="results"
SubOutputPath="TopKRegionWithMostAverageTip"
 

hadoop fs -rmr ${BasePath}/${BaseOutputPath}/${SubOutputPath}

hadoop jar ../../target/CSC582Project-1.jar edu.clu.cs.TopKItemsDriver -k 500 -i ${BasePath}/${DataPath} -o ${BasePath}/${BaseOutputPath}/${SubOutputPath}
