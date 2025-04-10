package spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object increload_tfl {
  def main(args: Array[String]): Unit = {

    // Create SparkSession with Hive support
    val spark = SparkSession.builder()
      .appName("Read from Hive")
      .config("spark.sql.catalogImplementation", "hive")  // Enables Hive support
      .enableHiveSupport()  // Required for using Hive
      .getOrCreate()

    // Define JDBC connection parameters
    val jdbcUrl = "jdbc:postgresql://18.170.23.150:5432/testdb"
    val dbTable = "tfl2"
    val dbProperties = new java.util.Properties()
    dbProperties.setProperty("user", "consultants")  // Your database username
    dbProperties.setProperty("password", "WelcomeItc@2022")  // Your database password
    dbProperties.setProperty("driver", "org.postgresql.Driver")

    spark.sql(s"USE big_datajan2025")

    val hiveTable = "TFL_UNDERGROUND"
    val maxIdHive = spark.sql(s"SELECT MAX(CAST(UID AS BIGINT)) AS max_id FROM $hiveTable")
      .first()
      .getAs[Long]("max_id")

    println(s"Maximum ID in Hive table: $maxIdHive")

    // Read data from the PostgreSQL table into a DataFrame
    val query = s"(SELECT * FROM sop_fraud_scala_b WHERE id > $maxIdHive) AS incremental_data"
    val df_postgres = spark.read
      .jdbc(jdbcUrl, query, dbProperties)  // Replace "your_table_name" with your table name

    val newRowCount = df_postgres.count()
    println(s"Number of new rows to add: $newRowCount")

    println("read successful")

    //df.show(5)

    // update #1 >> Fill empty rows in categories with "Travel"
    val df_cleaned = df_postgres.withColumn(
      "category",
      when(col("category").isNull || col("category") ==="", "travel").otherwise (col("category"))
    )
    println("updated DataFrame #1: empty rows in columns filled")

    // update #2 >> Rename columns
    val df_renamed = df_cleaned
      .withColumnRenamed("first", "first_name")
      .withColumnRenamed("last", "last_name")
      .withColumnRenamed("city_pop", "population")
      .withColumnRenamed("trans_date_trans_time", "trans_date_time")
    println("updated DataFrame #2: first, last, city_pop, & trans_date_trans_time  columns renamed")

    // update #3 >> Add age column
    val df_WithAge = df_renamed.withColumn(
      "age",
      floor(datediff(current_date(), to_date(col("dob"), "yyyy-mm-dd"))/365)
    )
    println("updated DataFrame #3: age column added")

    // update #4 >> combine first_name & last_name into full_name
    val df_FullName = df_WithAge.withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))
    println("updated DataFrame #4: first & last name combined into fullName")

    // update #5 >> Drop cc_num, first_name, & last_name column
    val columnsToDrop = Seq("cc_num", "first_name", "last_name")
    val df_Updated = df_FullName.drop(columnsToDrop:_*)
    println("updated DataFrame #5: cc_num first_name, & last_name columns dropped")

    // update #6 >> reorder columns and move gender, dob & age next to full name
    val df_reordered = df_Updated.select( "id", "trans_date_time", "merchant", "category", "amt", "full_name",
      "gender", "dob", "age", "street", "city", "state", "zip", "lat", "long", "population", "job",
      "trans_num", "unix_time", "merch_lat", "merch_long", "is_fraud")
    println("updated DataFrame #6: columns reordered")

    // update #7 >> sort table by id ascending
    val df_sorted = df_reordered.orderBy((col("id").asc))
    println("updated DataFrame #7: table sorted by id column")

    // update #8 >> change amount column to decimals
    val df_updated = df_sorted.withColumn("amt", col("amt").cast("decimal"))
    println("updated DataFrame #8: amt data type changed to decimal")

    // update #9 >> drop duplicates
    val df_distinct = df_sorted.dropDuplicates("id")
    println("updated DataFrame #9: duplicates dropped")

    // update #10 >> remove "fraud_" from all merchants name
    val leadingString = "fraud_"
    val df_modifiedRows = df_distinct.withColumn("merchant", regexp_replace(col("merchant"), "^" + leadingString, ""))
    println("updated DataFrame #10: leading strings in merchant names removed")
    df_modifiedRows.show(5)

    // Write DataFrame to Hive table
    df_modifiedRows.write
      .mode("append")  // Use append for adding data without overwriting
      .saveAsTable("bigdata_nov_2024.sop_fraud_trans_b")  // Specify your database and table name

    println(s"$newRowCount new records added")

    // Stop SparkSession
    spark.stop()
  }
}
