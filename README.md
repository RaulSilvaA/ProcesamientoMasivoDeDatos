# Procesamiento Masivo De Datos

Proyecto con el fin de experimentar con SparkQL en una base de datos masiva, en este caso se decidió por una que consta de las partidas de ajedrez del año 2016 de la página Lichess.

En principio se pretenden realizar las siguientes quieries:

- ***QUERY1:*** map de cantidad de movimientos realizados en la partida (por tipo de evento)
- aperturas que generan ganadores de blancas (top) (por tipo de evento y general)
- defensas que generan ganadores de negras (top) (por tipo de evento y general)
- aperturas y defensas que generan tablas (top) (por tipo de evento y general)

- cantidad de tipos de enroques (blancas o negras, o win; por ELO - tipo de partida - largo de partida - #movimiento)
- cantidad de piezas comidas (blancas o negras, o win; por ELO - tipo de partida - largo de partida - #movimiento) 
- cantidad de Jaque (blancas o negras, o win; por ELO - tipo de partida - largo de partida - #movimiento) 

De momento se posee un archivo con extesión .csv con las siguientes conlumnas:
