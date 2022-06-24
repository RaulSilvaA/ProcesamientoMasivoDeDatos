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
    5 Eco
    6 Opening
    7 TimeControl
    8 Temination 
    9 AN
    10 nComidas
    11 nMovimientos
    '''


    chess = lines.map(lambda line: (str(line[1]).strip(), line[2]+'#'+line[3], line[4], line[5], line[6], \
         line[7], line[8], line[9], line[10], line[11], line[11].count('x'), line[11].count('.')))
    

    #(Event, n_piezas_comidas)
    comLength =  chess.map(lambda line: (line[11], line[10]))

    #(Event, n_piezas_comidas)
    pComidasLength = comLength.aggregateByKey((0.0, 0), \
        lambda sumCount, nCom: (sumCount[0] + nCom, sumCount[1] + 1), \
        lambda sumCountA, sumCountB: (sumCountA[0] + sumCountB[0], sumCountA[1] + sumCountB[1])
        )
    
    #(Event, avg_piezas_comidas)
    avgComLenght = pComidasLength.mapValues(lambda tup2n: tup2n[0]/tup2n[1])

    cAvgComLenght = avgComLenght.coalesce(1)

    sAvgComLenght = cAvgComLenght.sortByKey()

    sAvgComLenght.saveAsTextFile(fileout)


    spark.stop()