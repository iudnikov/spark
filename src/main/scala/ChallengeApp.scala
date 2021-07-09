import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object ChallengeApp {

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder
      .master("local[4]")
      .appName("Leo Vegas")
      .getOrCreate()

    factBet(sparkSession)
    dimGame(sparkSession)
    dimPlayer(sparkSession)

    sparkSession.stop()

  }

  def dimPlayer(sparkSession: SparkSession): Unit = {
    val playersRaw = sparkSession.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load("data/Player.csv")

    val playersGrouped = playersRaw
      .select(
        col("playerID"),
        col("latestUpdate"))
      .groupBy(
        col("playerID"))
      .agg(
        max("latestUpdate"))

    playersRaw.as("raw")
      .join(
        playersGrouped,
        col("raw.playerID").equalTo(playersGrouped("playerID"))
          .and(playersRaw("latestUpdate").equalTo(playersGrouped("max(latestUpdate)"))))
      .select(
        col("raw.playerID").as("player_id"),
        col("gender"),
        col("country"),
        col("latestUpdate"))
      .sort(desc("latestUpdate"))
      .repartition(1)
      .write
      .mode("overwrite")
      .option("header", true)
      .option("delimiter", ",")
      .csv("data/Dim_player.csv")
  }

  def dimGame(sparkSession: SparkSession): Unit = {

    val gameRaw = sparkSession.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load("data/Game.csv")

    val gameCategory = sparkSession.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load("data/GameCategory.csv")

    val gameProvider = sparkSession.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load("data/GameProvider.csv")

    gameRaw
      .join(gameCategory, gameRaw("ID").===(gameCategory("Game ID")))
      .join(gameProvider, gameRaw("GameProviderId").===(gameProvider("ID")))
      .select(
        gameRaw("ID").as("game_id"),
        gameRaw("Game Name").as("game_name"),
        gameCategory("Game Category").as("game_category"),
        gameProvider("Game Provider Name").as("Provider_name"))
      .sort(
        desc("game_id"))
      .repartition(1)
      .write
      .mode("overwrite")
      .option("header", true)
      .option("delimiter", ",")
      .csv("data/Dim_game.csv")

  }

  def factBet(sparkSession: SparkSession): Unit = {

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault())

    val players = {

      val playersRaw = sparkSession.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("delimiter", ",")
        .load("data/Player.csv")

      val window = Window.partitionBy("playerID").orderBy("latestUpdate")

      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault())

      playersRaw
        .withColumn("validUntil", lag(col("latestUpdate"), -1).over(window))
        .na.fill(formatter.format(Instant.now()), Array("validUntil"))

    }

    val bets = {
      sparkSession.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("delimiter", ";")
        .load("data/GameTransaction.csv")

    }

    val currencies = {
      val currenciesRaw = sparkSession.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("delimiter", ",")
        .load("data/CurrencyExchange.csv")

      val window = Window.partitionBy("currency").orderBy("date")

      currenciesRaw
        .withColumn("validUntil", lag(col("date"), -1).over(window))
        .na.fill(formatter.format(Instant.now()), Array("validUntil"))

    }

    val joined = {
      bets
        .select(
          col("Date").as("txDate"),
          col("PlayerId").as("txPlayerId"),
          col("gameID"),
          col("txCurrency"),
          col("realAmount"),
          col("bonusAmount"),
          col("txType"),
        )
        .join(
          players,
          col("txDate").>=(players("latestUpdate"))
            .and(col("txDate").<(players("validUntil")))
            .and(col("txPlayerId").===(players("playerID")))
        )
        .join(
          currencies,
          bets("txCurrency").===(currencies("currency"))
            .and(col("txDate").<(currencies("validUntil")))
            .and(col("txDate").>=(currencies("date")))
        )
    }

    joined
      .withColumn("realAmountEuro", regexp_replace(col("realAmount"), ",", ".").*(col("baseRateEuro")))
      .withColumn("bonusAmountEuro", regexp_replace(col("bonusAmount"), ",", ".").*(col("baseRateEuro")))
      .withColumn("cashTurnover", when(col("txType").===("WAGER"), col("realAmountEuro")).otherwise(0.0))
      .withColumn("bonusTurnover", when(col("txType").===("WAGER"), col("bonusAmountEuro")).otherwise(0.0))
      .withColumn("cashWinnings", when(col("txType").===("RESULT"), col("realAmountEuro")).otherwise(0.0))
      .withColumn("bonusWinnings", when(col("txType").===("RESULT"), col("bonusAmountEuro")).otherwise(0.0))
      .withColumn("turnover", col("cashTurnover").+(col("bonusTurnover")))
      .withColumn("winnings", col("cashWinnings").+(col("bonusWinnings")))
      .withColumn("cashResult", col("cashTurnover").-(col("cashWinnings")))
      .withColumn("bonusResult", col("bonusTurnover").-(col("bonusWinnings")))
      .withColumn("grossResult", col("turnover").-(col("winnings")))
      .select(
        col("txDate").as("date"),
        col("gameID"),
        col("txPlayerId").as("playerId"),
        col("country"),
        col("cashTurnover"),
        col("bonusTurnover"),
        col("cashWinnings"),
        col("bonusWinnings"),
        col("turnover"),
        col("winnings"),
        col("cashResult"),
        col("bonusResult"),
        col("grossResult"))
      .groupBy(
        col("date"),
        col("playerId"),
        col("country"),
        col("gameId"))
      .agg(
        sum(col("cashTurnover")),
        sum(col("bonusTurnover")),
        sum(col("cashWinnings")),
        sum(col("bonusWinnings")),
        sum(col("turnover")),
        sum(col("winnings")),
        sum(col("cashResult")),
        sum(col("bonusResult")),
        sum(col("grossResult")))
      .sort(
        desc("date"),
        desc("playerId"),
        desc("country"),
        desc("gameId"))
      .repartition(1)
      .write
      .mode("overwrite")
      .option("header", true)
      .option("delimiter", ",")
      .csv("data/Fact_bet.csv")

  }

}
