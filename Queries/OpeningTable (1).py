from __future__ import print_function
from audioop import avg
from curses import raw

import sys
from pyspark.sql import SparkSession


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: average_series_rating.py <filein> <fileout>", file=sys.stderr)
        sys.exit(-1)


    #quizas mas archivos de salida
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
    10 empateBool
    '''

    def jaqueMate(mov):
      mov = str(mov)
      nJaque = mov.count('+')
      if mov.count('++') != 0:
          nJaque = nJaque - 1
      return nJaque

    def nTablas(r):
      if r == "1-0":
          return 1
      else:
          return 0

    chess = lines.map(lambda line: (str(line[1]).strip(), line[2]+'#'+line[3], line[4], line[5], line[6], \
        line[7], line[8], line[9], line[10], line[11], \
        nTablas(line[4])))   
    
    
   
    openingEvent = chess.map(lambda line: (line[5]+'#'+str(line[6]).strip(), line[10]))


    #(cantidad de tablas, cantidad de partidas de tipo Opening#Event)
    nTablasEvent = openingEvent.aggregateByKey((0.0, 0), \
        lambda sumCount, nTablas: (sumCount[0] + nTablas, sumCount[1] + 1), \
        lambda sumCountA, sumCountB: (sumCountA[0] + sumCountB[0], sumCountA[1] + sumCountB[1])
        )

    #(Opening#Event, avg_tablas)
    percentageTablasEvent = nTablasEvent.mapValues(lambda tup2n: tup2n[0]/tup2n[1]*100)
    df = spark.createDataFrame(percentageTablasEvent, ['eco#opening', 'winrate'])
    percentageTablasEventSorted = df.sort(['winrate'],ascending = [False])
    top10Tablas = percentageTablasEventSorted.limit(50)

    top10Tablas.show(50, truncate=False)
    spark.stop()