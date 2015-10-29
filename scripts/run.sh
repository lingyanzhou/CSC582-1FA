
BasePath="/user/mgroup3"
DataPath="data"
BaseOutputPath="results"
SubOutputPath="NumericalSummary"
 

hadoop fs -rmr ${BasePath}/${BaseOutputPath}/${SubOutputPath}

if [ $1 == 'all' ]; then
    hadoop jar ../target/CSC582Project-1.jar edu.clu.cs.NumericalSummaryDriver -i ${BasePath}/${DataPath} -o ${BasePath}/${BaseOutputPath}/${SubOutputPath}

    exit
fi
