from __future__ import print_function
from audioop import avg
from curses import raw

import sys
import statistics
from unittest.mock import seal
from pyspark.sql import SparkSession


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: average_series_rating.py <filein> <fileout>", file=sys.stderr)
        sys.exit(-1)


    #quizás más archivos de salida
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

    def eloDefiner(wElo, bElo):
        avgElo = statistics.mean([wElo, bElo])
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


    '''
    Chess:
    0 Event
    1 White#Black
    2 Result
    3 WhiteElo
    4 BlackElo
    5 pEloBlock
    6 Eco
    7 Opening
    8 TimeControl
    9 Temination
    10 AN
    11 nMovimientos
    12 nComidas
    13 nJaques
    14 boolEnroques
    '''

    def boolEnroque(mov):
        if mov.contains('-'):
            return 1
        return 0

    def jaqueMate(mov):
        nJaque = mov.count('+')
        if mov.contains('++'):
            nJaque -= 1
        return nJaque

    chess = lines.map(lambda line: (line[0], line[1]+'#'+line[2], line[3], line[4], line[5], \
        eloDefiner(line[4], line[5]), line[6], line[7], line[8], line[9], line[10], \
        line[10].count('.'), line[10].count('x'), jaqueMate(line[10]), boolEnroque(line[10])))

    
    chess_cached = chess.cache()

    #(Event, n_piezas_comidas)
    comEvent =   chess_cached.map(lambda line: (line[0], line[12]))

    #(Event, n_piezas_comidas)
    pComidasEvent = comEvent.aggregateByKey((0.0, 0), \
        lambda sumCount, nCom: (sumCount[0] + nCom, sumCount[1] + 1), \
        lambda sumCountA, sumCountB: (sumCountA[0] + sumCountB[0], sumCountA[1] + sumCountB[1])
        )
    
    #(Event, avg_piezas_comidas)
    avgComEvent = pComidasEvent.mapValues(lambda tup2n: tup2n[0]/tup2n[1])



    #(EloBlock, n_piezas_comidas)
    comElo = chess_cached.map(lambda line: (line[5], line[12]))

    #(EloBlock, n_piezas_comidas)
    pComidasElo = comElo.aggregateByKey((0.0, 0), \
        lambda sumCount, nCom: (sumCount[0] + nCom, sumCount[1] + 1), \
        lambda sumCountA, sumCountB: (sumCountA[0] + sumCountB[0], sumCountA[1] + sumCountB[1])
        )
    
    #(Event, avg_piezas_comidas)
    avgComElo = pComidasElo.mapValues(lambda tup2n: tup2n[0]/tup2n[1])

    #(nMovimientos, nComidas)
    comNMov = chess_cached.map(lambda line: (line[5], line[12]))

    #(nMovimientos, nComidas)
    pComidasNMov = comNMov.aggregateByKey((0.0, 0), \
        lambda sumCount, nCom: (sumCount[0] + nCom, sumCount[1] + 1), \
        lambda sumCountA, sumCountB: (sumCountA[0] + sumCountB[0], sumCountA[1] + sumCountB[1])
        )

    #(nMovimientos, avg_piezas_comidas)
    avgComNMov = pComidasNMov.mapValues(lambda tup2n: tup2n[0]/tup2n[1])

    ###################CANTIDAD DE JAQUES####################################

    #(Event, cantidad de jaques)
    nJaquesEvent =   chess_cached.map(lambda line: (line[0], line[13]))

    ######SEGUN EL TIPO DE PARTIDA
    #(cantidad de jaques, cantidad de partidas de cierto tipo)
    nJaquesEvent = nJaquesEvent.aggregateByKey((0.0, 0), \
        lambda sumCount, nJaques: (sumCount[0] + nJaques, sumCount[1] + 1), \
        lambda sumCountA, sumCountB: (sumCountA[0] + sumCountB[0], sumCountA[1] + sumCountB[1])
        )
    
    #(Event, avg_jaques)
    avgJaquesEvent = nJaquesEvent.mapValues(lambda tup2n: tup2n[0]/tup2n[1])


    ######SEGUN ELO
    #(EloBlock, cantidad de jaques)
    nJaquesElo = chess_cached.map(lambda line: (line[5], line[13]))

    #(cantidad de jaques, cantidad de partidas de cierto ELO)
    nJaquesElo = nJaquesElo.aggregateByKey((0.0, 0), \
        lambda sumCount, nJaques: (sumCount[0] + nJaques, sumCount[1] + 1), \
        lambda sumCountA, sumCountB: (sumCountA[0] + sumCountB[0], sumCountA[1] + sumCountB[1])
        )
    
    #(ELO, avg_jaques)
    avgJaquesElo = nJaquesElo.mapValues(lambda tup2n: tup2n[0]/tup2n[1])


    ######SEGÚN LARGO DE PARTIDA
    nJaquesDuration = chess_cached.map(lambda line: (line[11], line[13]))

    #(cantidad de jaques, cantidad de partidas de cierta duración)
    nJaquesDuration = nJaquesDuration.aggregateByKey((0.0, 0), \
        lambda sumCount, nJaques: (sumCount[0] + nJaques, sumCount[1] + 1), \
        lambda sumCountA, sumCountB: (sumCountA[0] + sumCountB[0], sumCountA[1] + sumCountB[1])
        )
    
    #(Duration, avg_jaques)
    avgJaquesDuration = nJaquesDuration.mapValues(lambda tup2n: tup2n[0]/tup2n[1])

    ###################APERTURAS QUE GENERAN TABLAS####################################
    openingEvent = chess_cached.map(lambda line: (line[7]+'#'+line[0], line[2]))

    filteredOpeningEvent = openingEvent.filter(openingEvent[1] == '1/2-1/2')

    #(cantidad de tablas, cantidad de partidas de tipo Opening#Event)
    nTablasEvent = filteredOpeningEvent.aggregateByKey((0.0, 0), \
        lambda sumCount, nTablas: (sumCount[0] + nTablas, sumCount[1] + 1), \
        lambda sumCountA, sumCountB: (sumCountA[0] + sumCountB[0], sumCountA[1] + sumCountB[1])
        )

    #(Opening#Event, avg_tablas)
    avgTablasEvent = nTablasEvent.mapValues(lambda tup2n: tup2n[0]/tup2n[1])

    # Aperturas que generan ganadores de negras
    # (apertura#tipoEvento, result)
    #eventResult = chess_cached.map(lambda line: (line[7]+'#'+line[0], line[7]))