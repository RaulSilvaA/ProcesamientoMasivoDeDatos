from __future__ import print_function
from curses import raw

import sys
from unittest.mock import seal
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: average_series_rating.py <filein> <fileout>", file=sys.stderr)
        sys.exit(-1)

    filein = sys.argv[1] #"hdfs://cm:9000/uhadoop/shared/imdb/imdb-ratings-test.tsv"
    fileout = sys.argv[2] #"hdfs://cm:9000/uhadoop2021/<user>/chess-avg/"

    spark = SparkSession.builder.appName("chessProject").getOrCreate()

    inputRDD = spark.read.text(filein).rdd.map(lambda r: r[0])

    lines = inputRDD.map(lambda line: line.split(","))

    '''
    0 Event          object
    1 White          object
    2 Black          object
    3 Result         object
    4 WhiteElo        int64
    5 BlackElo        int64
    6 ECO            object 
    7 Opening        object
    8 TimeControl    object
    9 Termination    object
    10 AN             object
    '''

    #queries
    #rawChess = lines.filter(lambda line: ("TV_SERIES" == line[6]) and not ('null' == line[7]))

    #(Event, AN)
    movEvent = lines.map(lambda tup: (tup[0], tup[10]))

    #(Event, numero_de_movimientos)
    nMovementsEvent = movEvent(lambda line: (line[0], len(line[1].split('.'))-1))

    #(Event, cantidad_promedio_de_movimientos)
    pMovementsEvent = nMovementsEvent.aggregateByKey((0.0, 0), \
        lambda sumCount, nMov: (sumCount[0] + nMov, sumCount[1] + 1), \
        lambda sumCountA, sumCountB: (sumCountA[0] + sumCountB[0], sumCountA[1] + sumCountB[1])
        )

    #(event, avg_mov_x_event)
    avgMovementEvent = pMovementsEvent.mapValues(lambda tup2n: tup2n[0]/tup2n[1])

    avgMovementEvent.saveAsTextFile(fileout)

    spark.stop()

