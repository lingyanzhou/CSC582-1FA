sum = 0;
count = 0;
with open("KNNtip.tsv") as f:
    for line in f:
        val = float(line.split("\t")[1])
        sum+=val
        count+=1
    f.close()
print "Predicted Tip Amount: ", sum/count
