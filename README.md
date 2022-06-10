# Procesamiento Masivo De Datos

Proyecto con el fin de experimentar con SparkQL en una base de datos masiva, en este caso se decidió por una que consta de las partidas de ajedrez del año 2016 de la página Lichess.

En principio se pretenden realizar las siguientes quieries:

- ***QUERY1:*** map de cantidad de movimientos realizados en la partida (por tipo de evento)

#Nick
- cantidad de tipos de enroques (blancas o negras; por ELO - tipo de partida - largo de partida - #movimiento)

#Raúl
- cantidad de piezas comidas (blancas o negras; por ELO - tipo de partida - largo de partida - #movimiento) 

#Vane
- cantidad de Jaque (blancas o negras; por ELO - tipo de partida - largo de partida - #movimiento) 

#Nick
- aperturas que generan ganadores de blancas (top 10) (por tipo de evento y general)

#Raúl
- defensas que generan ganadores de negras (top 10) (por tipo de evento y general)

#Vane
- aperturas y defensas que generan tablas (top 10) (por tipo de evento y general)


De momento se posee un archivo con extesión .csv con las siguientes conlumnas:
