from __future__ import print_function
from audioop import avg
from curses import raw

import sys
import string
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
    Chess:
    0 id
    1 Event
    2 White#Black
    3 Result
    4 WhiteElo
    5 BlackElo
    6 pEloBlock
    7 Eco
    8 Opening
    9 TimeControl
    10 Temination 
    11 AN
    12 nMovimientos
    13 nComidas
    14 nJaques
    15 boolEnroques
    16 nTablas
    '''

    def eloDefiner(wElo, bElo):
        avgElo = wElo if wElo > bElo else bElo
        eloNames = ['novice', 'class D', 'class C', 'class B', 'class A', 'CM', 'CM-NM', \
            'FM-IM', 'IM-GM', 'GM', 'SGM']
        if avgElo < u'1200':
            return eloNames[0]
        elif avgElo < u'1400':
            return eloNames[1]
        elif avgElo < u'1600':
            return eloNames[2]
        elif avgElo < u'1800':
            return eloNames[3]
        elif avgElo < u'2000':
            return eloNames[4]
        elif avgElo < u'2200':
            return eloNames[5]
        elif avgElo < u'2300':
            return eloNames[6]
        elif avgElo < u'2400':
            return eloNames[7]
        elif avgElo < u'2500':
            return eloNames[8]
        elif avgElo < u'2700':
            return eloNames[9]
        else:
            return eloNames[10]


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
    10 elodef
    11 empateBool
    '''

    def boolEnroque(mov):
        if '-' in str(mov):
            return 1
        return 0

    def jaqueMate(mov):
        mov = str(mov)
        nJaque = mov.count('+')
        if '++' in mov:
            nJaque -= 1
        return nJaque

    def nTablas(r):
      if r == "1/2-1/2":
          return 1
      else:
          return 0

    
    chess = lines.map(lambda line: (line[1], line[2]+'#'+line[3], line[4], line[5], line[6], \
        eloDefiner(line[5], line[6]), line[7], line[8], line[9], line[10], line[11], \
        line[11].count('.'), line[11].count('x'), jaqueMate(line[11]), boolEnroque(line[11]), eloDefiner(line[5],line[6]), nTablas(line[4]))) 
    
    #(Event, n_piezas_comidas)
    comLength =  chess.map(lambda line: (line[15], line[12]))

    #(Event, n_piezas_comidas)
    pComidasElo = comLength.aggregateByKey((0.0, 0), \
        lambda sumCount, nCom: (sumCount[0] + nCom, sumCount[1] + 1), \
        lambda sumCountA, sumCountB: (sumCountA[0] + sumCountB[0], sumCountA[1] + sumCountB[1])
        )
    
    #(Event, avg_piezas_comidas)
    avgComElo = pComidasElo.mapValues(lambda tup2n: tup2n[0]/tup2n[1])

    cAvgComElo = avgComElo.coalesce(1)

    sAvgComElo = cAvgComElo.sortByKey()

    sAvgComElo.saveAsTextFile(fileout)


    spark.stop()