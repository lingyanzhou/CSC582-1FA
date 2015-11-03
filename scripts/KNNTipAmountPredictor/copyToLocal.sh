BasePath="/user/mgroup3"
BaseOutputPath="results"
SubOutputPath="KNNTipAmountPredictor"

hadoop fs -copyToLocal ${BasePath}/${BaseOutputPath}/${SubOutputPath}/part-r-00000 KNNtip.tsv

