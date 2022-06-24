from __future__ import print_function
from audioop import avg
from curses import raw

import sys
from pyspark.sql import SparkSession


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: average_series_rating.py <filein> <fileout>", file=sys.stderr)
        sys.exit(-1)


    filein = sys.argv[1] #"hdfs://cm:9000/uhadoop/shared/imdb/imdb-ratings-test.tsv"
    fileout = sys.argv[2] #"hdfs://cm:9000/uhadoop2021/<user>/chess-avg/"

    spark = SparkSession.builder.appName("chessProject").getOrCreate()

    inputRDD = spark.read.text(filein).rdd.map(lambda r: r[0])


    '''
    0 id
    1 Event          object
    2 White          object
    3 Black          object
    4 Result         object
    5 WhiteElo        int64
    6 BlackElo        int64
    7 ECO            object 
    8 Opening        object
    9 TimeControl    object
    10 Termination    object
    11 AN             object
    '''

    lines = inputRDD.map(lambda line: line.split(","))

    '''
    Chess:
    0 Event
    1 White#Black
    2 Result
    3 WhiteElo
    4 BlackElo
    5 Eco#Opening
    6 TimeControl
    7 Temination 
    8 AN
    '''

    chess = lines.map(lambda line: (str(line[1]).strip(), line[2]+'#'+line[3], str(line[4]), line[5], line[6], \
        line[7]+'#'+str(line[8]).strip(), line[9], line[10], line[11]))

    #(EcoOpening, Result)
    ecoResult = chess.map(lambda line: (line[5], line[2]))
    ecoResult_cached = ecoResult.cache()

    #(EcoOpening, 1)
    ecoResult1 = ecoResult_cached.map(lambda line: (line[0], 1)) 

    #(bEcoOpening, sum)
    ecoResultSum = ecoResult1.reduceByKey(lambda b1, b2 : b1+b2)

    def aux(tup):
        if tup == '1/2-1/2':
            return 1
        return 0

    #Filtramos partidas donde ganaron negras (bEcoOpening, 0/1) 
    blackWins = ecoResult_cached.map(lambda line: (line[0], aux(line[1]))) 

    #(bEcoOpening, sum)
    blackWinsSum = blackWins.reduceByKey(lambda b1, b2 : b1+b2)

    #join blackWinsSum ecoResultSum
    #(EcoOpening, sumBWin, sumT)
    mergedEcoOp = blackWinsSum.join(ecoResultSum)

    #(EcoOpening, perBlackWins)
    ecoOpBlackWinsPer = mergedEcoOp.mapValues(lambda tup: (tup[0]/tup[1])*100)

    ecoOpBlackWinsPer.saveAsTextFile(fileout)

    spark.stop()




