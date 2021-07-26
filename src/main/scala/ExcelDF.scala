import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, length, locate, round, substring, substring_index, trim}
import org.apache.spark.sql.types.{FloatType, IntegerType}
import org.apache.hadoop.fs._

object ExcelDF {
  def main(args: Array[String]): Unit ={
    val spark = SparkSession.builder().master("local[*]").appName("Excel to DataFrame").getOrCreate()
    val df1 = spark.read
      .format("com.crealytics.spark.excel")
      .option("sheetName", "Sheet1")
      .option("header", "true")
      .option("treatEmptyValuesAsNulls", "false")
      .load("C:/Users/an_ma/Programming/Spark/15.05.21.xls")
    val df2 = spark.read
      .format("com.crealytics.spark.excel")
      .option("sheetName", "Sheet1")
      .option("header", "true")
      .option("treatEmptyValuesAsNulls", "false")
      .load("C:/Users/an_ma/Programming/Spark/16.05.21.xls")
    val df3 = df1.union(df2)
    val df4 = spark.read
      .format("com.crealytics.spark.excel")
      .option("sheetName", "Sheet1")
      .option("header", "true")
      .option("treatEmptyValuesAsNulls", "false")
      .load("C:/Users/an_ma/Programming/Spark/17.05.21.xls")
    val dffin = df3.union(df4)
    dffin.show(300)
    val dffil = dffin.filter(col("Статус") =!= "cancelled_by_guest")
    val dfrnm = dffil.withColumnRenamed("Номер бронирования", "Booking")
      .withColumnRenamed("Кто забронировал", "Who_reserved")
      .withColumnRenamed("Имя гостя", "Guest_name")
      .withColumnRenamed("Заезд", "Arrival")
      .withColumnRenamed("Отъезд", "Departure")
      .withColumnRenamed("Дата бронирования", "Reservation_date")
      .withColumnRenamed("Статус", "Status")
      .withColumnRenamed("Номера", "Rooms")
      .withColumnRenamed("Количество гостей", "Guests")
      .withColumnRenamed("Взрослые", "Adults")
      .withColumnRenamed("Дети", "Children")
      .withColumnRenamed("Возраст детей", "Children_age")
      .withColumnRenamed("Цена", "Price")
      .withColumn("Price", trim(col("Price"), " RUB"))
      .withColumn("Price", col("Price").cast(FloatType))
      .withColumn("Price", round(col("Price"), 2))
      .withColumnRenamed("Комиссия, %", "Commission_%")
      .withColumn("Commission_%", col("Commission_%").cast(FloatType))
      .withColumn("Commission_%", round(col("Commission_%"), 2))
      .withColumnRenamed("Сумма комиссии", "Commission_sum")
      .withColumn("Commission_sum", trim(col("Commission_sum"), " RUB"))
      .withColumn("Commission_sum", col("Commission_sum").cast(FloatType))
      .withColumn("Commission_sum", round(col("Commission_sum"), 2))
      .withColumnRenamed("Статус оплаты", "Payment_status")
      .withColumnRenamed("Способ оплаты", "Payment_method")
      .withColumnRenamed("Комментарии", "Comments")
      .withColumnRenamed("Группа", "Group")
      .where("Price <= 10000")
    dfrnm.printSchema()
    dfrnm.show(50)
    dfrnm.write.format("com.crealytics.spark.excel")
      .option("header", "true")
      .save("C:/Users/an_ma/Programming/Spark/15-18.xls")
    val jsonDF = dfrnm.toJSON
    val count = jsonDF.count()
    jsonDF.repartition(1)
      .rdd
      .zipWithIndex()
      .map { case(json, idx) =>
        if(idx == 0) "[\n" + json + ","
        else if(idx == count-1) json + "\n]"
        else json + ","
      }
      .saveAsTextFile("C:/Users/an_ma/Programming/Spark/bookings")
    val sc = spark.sparkContext
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val basePath = "C:/Users/an_ma/Programming/Spark/bookings"
    val newFileName = "bookings.json"
    fs.rename(new Path(s"$basePath/part-00000"), new Path(s"$basePath/$newFileName"))
  }
}
