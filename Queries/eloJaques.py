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
    10 nJaques
    11 EloCode
    '''

    def eloDefiner(wElo, bElo):
        avgElo = 0
        if type(wElo) != int:
            avgElo = bElo
        else:
            avgElo = (wElo + bElo)/2


        eloNamesU2000 = ['']*2+['class D']*2+['class C']*2+['class B']*2+['class A']*2
        eloNamesO2000 =  ['CM']*2+['CM-NM','FM-IM','IM-GM']+ ['GM']*2

        if avgElo < 1200:
            return 'novice'
        elif avgElo < 2700:
            limit = int(avgElo/1000)
            index = int((avgElo-(limit*1000))/100)
            return eloNamesU2000[index] if limit < 2 else eloNamesO2000[index]
        else: # >= 2700
            return 'SGM'

    def jaqueMate(mov):
      mov = str(mov)
      nJaque = mov.count('+')
      if mov.count('++') != 0:
          nJaque = nJaque - 1
      return nJaque

    chess = lines.map(lambda line: (line[1], line[2]+'#'+line[3], line[4], line[5], line[6], \
        line[7], line[8], line[9], line[10], line[11], \
        jaqueMate(line[11]), eloDefiner(line[5], line[6])))
 
 
 ######SEGUN ELO
    #(EloBlock, cantidad de jaques)
    nJaquesElo = chess.map(lambda line: (line[11], line[10]))

    #(cantidad de jaques, cantidad de partidas de cierto ELO)
    nJaquesElo = nJaquesElo.aggregateByKey((0.0, 0), \
        lambda sumCount, nJaques: (sumCount[0] + nJaques, sumCount[1] + 1), \
        lambda sumCountA, sumCountB: (sumCountA[0] + sumCountB[0], sumCountA[1] + sumCountB[1])
        )
    
    #(ELO, avg_jaques)
    avgJaquesElo = nJaquesElo.mapValues(lambda tup2n: tup2n[0]/tup2n[1])

    cAvgJaquesElo = avgJaquesElo.coalesce(1)
    cAvgJaquesElo.saveAsTextFile(fileout)

    spark.stop()