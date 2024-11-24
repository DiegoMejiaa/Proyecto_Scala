// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// COMMAND ----------

val arch : String ="dbfs:/FileStore/chess_games.csv"

// COMMAND ----------

val games = spark.read.option("header", "true").option("inferSchema", "true").csv(arch)

// COMMAND ----------

display(games) 

// COMMAND ----------

val cleanedGames = games.drop("opening_code", "opening_fullname", "opening_response", "opening_variation")

// COMMAND ----------

display(cleanedGames)

// COMMAND ----------

//Identificar las aperturas mas comunes 
val commonOpenings = cleanedGames.groupBy("opening_shortname")
  .count()
  .orderBy(desc("count"))
  .limit(10)
commonOpenings.show()

// COMMAND ----------

// Verificar cómo ciertas aperturas afectan las victorias para blancos o negros
val openingEffect = cleanedGames.groupBy("opening_shortname", "winner")
  .count()
  .orderBy(desc("count"))
openingEffect.show()

// COMMAND ----------

// Filtrar partidas con más de 50 movimientos y agrupar por apertura para ver cuáles se juegan más en partidas largas
val longGamesByOpening = cleanedGames.filter($"turns" > 50)
  .groupBy("opening_shortname")
  .count()
  .orderBy(desc("count"))
  .limit(10)
longGamesByOpening.show()

// COMMAND ----------

// Contar las aperturas más usadas en partidas clasificadas (rated = true)
val ratedOpenings = cleanedGames.filter($"rated" === true)
  .groupBy("opening_shortname")
  .count()
  .orderBy(desc("count"))
  .limit(10)
ratedOpenings.show()


// COMMAND ----------

// Calcular el promedio de movimientos (turns) según el tipo de victoria
val avgTurnsByVictory = cleanedGames.groupBy("victory_status")
  .agg(avg("turns").as("avg_turns"))
  .orderBy(desc("avg_turns"))
avgTurnsByVictory.show()

// COMMAND ----------

// Agrupar partidas por los valores de "time_increment" y contar cuántas partidas se jugaron con cada configuración
val timeIncrementDistribution = cleanedGames.groupBy("time_increment")
  .count()
  .orderBy(desc("count"))
timeIncrementDistribution.show()

// COMMAND ----------

// Contar las victorias por jugador (blanco y negro) y listar los 5 jugadores con más victorias
val topPlayers = cleanedGames.filter($"winner" =!= "draw")
  .withColumn("winner_id", when($"winner" === "White", $"white_id").otherwise($"black_id"))
  .groupBy("winner_id")
  .count()
  .orderBy(desc("count"))
  .limit(5)
topPlayers.show()

// COMMAND ----------

// Calcular la duración promedio (en turnos) de las partidas según el incremento de tiempo
val avgTurnsByTimeIncrement = cleanedGames.groupBy("time_increment")
  .agg(avg("turns").as("avg_turns"))
  .orderBy(desc("avg_turns"))
avgTurnsByTimeIncrement.show()

// COMMAND ----------

// Analizar qué tipo de victorias son más comunes en partidas rápidas
val victoryInFastGames = cleanedGames.filter($"time_increment".contains("5") || $"time_increment".contains("3"))
  .groupBy("victory_status")
  .count()
  .orderBy(desc("count"))
victoryInFastGames.show()

// COMMAND ----------

// Extraer la secuencia de los primeros 5 movimientos de la columna "moves" y contar las secuencias más comunes
val mostCommonMoves = cleanedGames.withColumn("first_5_moves", expr("split(moves, ' ')"))
  .withColumn("first_5_moves", array_join(slice($"first_5_moves", 1, 5), " "))
  .groupBy("first_5_moves")
  .count()
  .orderBy(desc("count"))
  .limit(10)

mostCommonMoves.show(false)
