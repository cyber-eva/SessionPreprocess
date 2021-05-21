import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types
import org.apache.spark.sql.SparkSession
import SessionPreprocess_assist.{regDate}

object SessionPreprocess {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SeesionPreprocess").getOrCreate()
    import spark.implicits._

    //REGISTRATION
    val regDate_udf  = spark.udf.register("regDate_udf",regDate)

    val input_path:String = args(0)

    val colsToRename:Map[String,String] = Map(
      "ga_clientid"          -> "ClientID",
      "ga_campaign"          -> "utm_campaign",
      "ga_keyword"           -> "utm_term",
      "ga_adcontent"         -> "utm_content",

    )

    val colsToAdd:Map[String,String] = Map(
      "interaction_type" -> "session",
      "src"              -> "ga",
      "goal"             -> "0",
      "campaign_id"      -> "(not set)",
      "profile_id"       -> "(not set)",
      "creative_id"      -> "(not set)",
      "ad_id"            -> "(not set)"
    )

    val data = spark.read.
      format("parquet").
      option("inferSchema","false").
      option("header","true").
      load(input_path)

    val data_work = data.select(
      $"ga_adcontent".cast(sql.types.StringType),
      $"ga_datehourminute".cast(sql.types.StringType),
      $"ga_keyword".cast(sql.types.StringType),
      $"ga_sourcemedium".cast(sql.types.StringType),
      $"ga_sessioncount".cast(sql.types.StringType),
      $"ga_clientid".cast(sql.types.StringType),
      $"ga_campaign".cast(sql.types.StringType),
      $"ga_sessions".cast(sql.types.StringType)
    )

//    data_work.show(20)

    val data_rename = colsToRename.foldLeft(data_work){
      (acc,names) => acc.withColumnRenamed(names._1,names._2)
    }

    val data_add = colsToAdd.foldLeft(data_rename){
      (acc,name) => acc.withColumn(name._1,lit(name._2))
    }

    val data_datereg = data_add.
      withColumn(
        "datehourminute",
        regDate_udf(col("ga_datehourminute"),col("interaction_type"))
      )

    val data_utc = data_datereg.
      withColumn(
        "HitTimeStamp",
        unix_timestamp(col("datehourminute"),"yyyy-MM-dd HH:mm:ss") * 1000
      )

    val data_sourcemedium = data_utc.
      withColumn("utm_source",split($"ga_sourcemedium"," / ")(0)).
      withColumn("utm_medium",split($"ga_sourcemedium"," / ")(1))

    val data_select = data_sourcemedium.select(
      $"interaction_type",
      $"src",
      $"ClientID",
      $"goal",
      $"HitTimeStamp",
      $"ga_sessioncount",
      $"utm_source",
      $"utm_medium",
      $"utm_campaign",
      $"utm_term",
      $"utm_content",
      $"campaign_id",
      $"profile_id",
      $"creative_id",
      $"ad_id",
      $"datehourminute"
    )

    data_select.show(20)

  }

}