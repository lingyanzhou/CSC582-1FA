BasePath="/user/mgroup3"
BaseOutputPath="sample_results"
SubOutputPath="AutoCorrelation"

hadoop fs -cat ${BasePath}/${BaseOutputPath}/${SubOutputPath}/DailyTipSum/*

hadoop fs -cat ${BasePath}/${BaseOutputPath}/${SubOutputPath}/DailyTipAverage/*
