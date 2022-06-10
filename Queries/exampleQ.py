from __future__ import print_function
from curses import raw

import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: average_series_rating.py <filein> <fileout>", file=sys.stderr)
        sys.exit(-1)

    filein = sys.argv[1] #"hdfs://cm:9000/uhadoop/shared/imdb/imdb-ratings-test.tsv"
    fileout = sys.argv[2] #"hdfs://cm:9000/uhadoop2021/<user>/series-avg/"

    spark = SparkSession.builder.appName("Pythonlab5").getOrCreate()

    inputRDD = spark.read.text(filein).rdd.map(lambda r: r[0])

    lines = inputRDD.map(lambda line: line.split("\t"))

    tvSeries = lines.filter(lambda line: ("TV_SERIES" == line[6]) and not ('null' == line[7]))

    #(id_serie, nombre episodio, ep_score)
    seriesEpisodeRating = tvSeries.map(lambda line: (line[3]+ "#" + line[4] + "#" + line[5], line[7], float(line[2])))
    seriesEpisodeRating_cached = seriesEpisodeRating.cache()

    #(id_serie, ep_score)
    seriesToEpisodeRating = seriesEpisodeRating_cached.map(lambda tup: (tup[0], tup[2]))
    seriesToEpisodeRating_cached = seriesToEpisodeRating.cache() 

    # aggregateByKey(initial_value, combiner, reducer)
    seriesToSumCountRating = seriesToEpisodeRating_cached.aggregateByKey((0.0, 0), \
        lambda sumCount, rating: (sumCount[0] + rating, sumCount[1] + 1), \
        lambda sumCountA, sumCountB: (sumCountA[0] + sumCountB[0], sumCountA[1] + sumCountB[1]))

    #(id_serie, avg_ep_score)
    seriesToAvgRating = seriesToSumCountRating.mapValues(lambda tup2n: tup2n[0]/tup2n[1])


    #calculamos el maximo por serie
    #(id_serie, max_ep_score_rating)
    seriesToMaxRating = seriesToEpisodeRating_cached.reduceByKey(lambda epx, epy : max(epx,epy))
    seriesToMaxRating_cached = seriesToMaxRating.cache()

    #realizamos un join y nos quedamos solo con los capitulos que tengan id1 == id2 & rating == MaxRating
    #cond = [seriesToMaxRating[0] == seriesEpisodeRating[0] ,seriesToMaxRating[1]==seriesEpisodeRating[2]]
    #(id_serie, max_ep_score_rating, id_serie, nombre episodio, ep_score)
    joinedSeriesMaxRating = seriesToMaxRating_cached.join(seriesEpisodeRating_cached) 

    #se deberia realziar un select para que filtremos solo lo que nos interesa
    #(id_serie, episode_name, max_ep_score, ep_score) VERIFICAR SI SON LOS INDICES CORRECTOS
    #serieEpisodeMax = joinedSeriesMaxRating.map(lambda tup: (tup[0], tup[3], tup[1], tup[4]))

    #(id_serie, episodeMaxName)
    serieEpisodeMaxName = joinedSeriesMaxRating.map(lambda tup: (tup[0], tup[1][1]))

    #se procede a mergear los nombres
    #(id_serie, merged_names)
    seriesMergedNames = serieEpisodeMaxName.reduceByKey(lambda name1, name2: name1 + ' | ' + name2)


    #se supone que en este punto se tienen:
    #(id_serie, merged_names), (id_serie, max_ep_score), por lo que joineamos y luego deberia bastar con un join seriesEpisodeRating
    mergedNames_maxScore = seriesMergedNames.join(seriesToMaxRating_cached)
    rawResult = mergedNames_maxScore.join(seriesToAvgRating)


    rawResult.saveAsTextFile(fileout)

    spark.stop()
   