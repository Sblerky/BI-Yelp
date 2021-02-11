import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object ScriptGallayPerrin {
    def main(args: Array[String]) {
        // Initialisation de Spark
        val spark = SparkSession.builder.appName("ETL").master("local[4]").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        // Paramètres de la connexion BD
        Class.forName("org.postgresql.Driver")
        val url = "jdbc:postgresql://stendhal:5432/tpid2020"
        import java.util.Properties
        val connectionProperties = new Properties()
        connectionProperties.setProperty("driver", "org.postgresql.Driver")
        connectionProperties.setProperty("user", "tpid")
        connectionProperties.setProperty("password","tpid")

        //Récupération des informations sur les review
        val review = spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", "(SELECT review_id, business_id, user_id, stars, cool, funny, useful, date  FROM yelp.\"review\") AS t")
        .option("user", "tpid")
        .option("password", "tpid")
        .load()
        review.show(10)
        review.printSchema()
        
        //Création de la dimension dates à partir des informations de reviews
        var date = review.select("date").distinct()
        date = date.withColumn("year", year(col("date")))
        date = date.withColumn("month", concat(date_format(col("date"), "yyyy"), lit('-'), month(col("date"))))
        date = date.withColumn("quarter", concat(date_format(col("date"), "yyyy"), lit('-'), quarter(col("date"))))
        date = date.withColumn("weekOfYear",concat(date_format(col("date"), "yyyy"), lit('-'), weekofyear(col("date"))))
        date = date.withColumn("dayOfWeek", concat(date_format(col("date"), "yyyy"), lit('-'), weekofyear(col("date")), lit('-'), dayofweek(col("date"))))
        date.show(10)

        //Récupération des informations sur les utilisateurs
        val users = spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", "(SELECT  user_id, name, review_count, average_stars, fans FROM yelp.\"user\" ) AS t")
        .option("user", "tpid")
        .option("password", "tpid")
        .load()
        users.show(10)

        
        //Récupération des infos sur les business à partir du json
        var path_bis = "/home7/mp861554/BI/Projet/dataset/yelp_academic_dataset_business.json"
        var business = spark.read.json(path_bis).cache()
        business.show(10)

        //On ne conserve que certaines parties des informations
        var businessTab = business.select("business_id", "name", "address", "city", "categories");
        businessTab = businessTab.withColumn("categories", substring(col("categories"), 0, 250))

        //Récupération des infos sur les tip à parti du csv
        /* On n'utilisera pas la dimension tip car le csv est mal formé.
           La colonne commentaire contient des virgules ce qui fait que le fichier est mal lu
           par la fonction read.csv()
        */
        var path_tip = "/home7/mp861554/BI/Projet/dataset/yelp_academic_dataset_tip.csv"
        var tip = spark.read.csv(path_tip).cache()
        tip.show(10)

        //Récupération des informations des check in à partir du json
        var path_checkin = "/home7/mp861554/BI/Projet/dataset/yelp_academic_dataset_checkin.json"
        var checkin = spark.read.json(path_checkin).cache()
        checkin = checkin.withColumn("business_id", substring(col("business_id"), 1, 30))
        checkin = checkin.withColumn("date", substring(col("date"), 0, 250))
        checkin.show()

        //Création de la dimension geography à partir des infos contenues dans business
        var geo = business.select("business_id","state","postal_code","city","latitude","longitude")

        //Création de la dimension ouverture à partir des infos contenues dans business
        var ouverture = business.select("business_id","is_open","hours")
        ouverture.printSchema()
        ouverture=ouverture.select("business_id","is_open","hours.Monday","hours.Tuesday","hours.Wednesday", "hours.Thursday", "hours.Friday","hours.Saturday")
        ouverture.show(10)


        //Préparation de la connection à Oracle
        Class.forName("oracle.jdbc.driver.OracleDriver")
        val urlOra = "jdbc:oracle:thin:@stendhal:1521:enss2020"
        val connectionPropertiesOra = new Properties()
        connectionPropertiesOra.setProperty("driver", "oracle.jdbc.OracleDriver")
        connectionPropertiesOra.setProperty("user", "mp861554")
        connectionPropertiesOra.setProperty("password","mp861554")
        
        //Insertion des dimensions et des faits
        checkin.write.mode(SaveMode.Overwrite).jdbc(urlOra, "checkin", connectionPropertiesOra)
        businessTab.write.mode(SaveMode.Overwrite).jdbc(urlOra, "business", connectionPropertiesOra)
        users.write.mode(SaveMode.Overwrite).jdbc(urlOra, "users", connectionPropertiesOra)
        date.write.mode(SaveMode.Overwrite).jdbc(urlOra, "dates", connectionPropertiesOra)
        review.write.mode(SaveMode.Overwrite).jdbc(urlOra, "reviews", connectionPropertiesOra)
        geo.write.mode(SaveMode.Overwrite).jdbc(urlOra, "geography", connectionPropertiesOra)
        ouverture.write.mode(SaveMode.Overwrite).jdbc(urlOra, "ouverture", connectionPropertiesOra)
        spark.stop()
    }
}