// Databricks notebook source
// STARTER CODE - DO NOT EDIT THIS CELL
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._
import org.apache.spark.sql.expressions.Window

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
val customSchema = StructType(Array(StructField("lpep_pickup_datetime", StringType, true), StructField("lpep_dropoff_datetime", StringType, true), StructField("PULocationID", IntegerType, true), StructField("DOLocationID", IntegerType, true), StructField("passenger_count", IntegerType, true), StructField("trip_distance", FloatType, true), StructField("fare_amount", FloatType, true), StructField("payment_type", IntegerType, true)))

// COMMAND ----------

// STARTER CODE - YOU CAN LOAD ANY FILE WITH A SIMILAR SYNTAX.
val df = spark.read
   .format("com.databricks.spark.csv")
   .option("header", "true") // Use first line of all files as header
   .option("nullValue", "null")
   .schema(customSchema)
   .load("/FileStore/tables/nyc_tripdata.csv") // the csv file which you want to work with
   .withColumn("pickup_datetime", from_unixtime(unix_timestamp(col("lpep_pickup_datetime"), "MM/dd/yyyy HH:mm")))
   .withColumn("dropoff_datetime", from_unixtime(unix_timestamp(col("lpep_dropoff_datetime"), "MM/dd/yyyy HH:mm")))
   .drop($"lpep_pickup_datetime")
   .drop($"lpep_dropoff_datetime")

// COMMAND ----------

// LOAD THE "taxi_zone_lookup.csv" FILE SIMILARLY AS ABOVE. CAST ANY COLUMN TO APPROPRIATE DATA TYPE IF NECESSARY.

// ENTER THE CODE BELOW
val df_taxi_zone = spark.read
   .format("com.databricks.spark.csv")
   .option("header", "true") // Use first line of all files as header
   .option("nullValue", "null")
   .load("/FileStore/tables/taxi_zone_lookup.csv") // the csv file which you want to work with


// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
// Some commands that you can use to see your dataframes and results of the operations. You can comment the df.show(5) and uncomment display(df) to see the data differently. You will find these two functions useful in reporting your results.
// display(df)
df.show(5) // view the first 5 rows of the dataframe

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
// Filter the data to only keep the rows where "PULocationID" and the "DOLocationID" are different and the "trip_distance" is strictly greater than 2.0 (>2.0).

// VERY VERY IMPORTANT: ALL THE SUBSEQUENT OPERATIONS MUST BE PERFORMED ON THIS FILTERED DATA

val df_filter = df.filter($"PULocationID" =!= $"DOLocationID" && $"trip_distance" > 2.0)
df_filter.show(5)

// COMMAND ----------

// PART 1a: The top-5 most popular drop locations - "DOLocationID", sorted in descending order - if there is a tie, then one with lower "DOLocationID" gets listed first
// Output Schema: DOLocationID int, number_of_dropoffs int 

// Hint: Checkout the groupBy(), orderBy() and count() functions.

// ENTER THE CODE BELOW
val df_popular_dropoff = df_filter.groupBy("DOLocationID").count().orderBy(desc("count"),asc("DOLocationID")).withColumnRenamed("count", "number_of_dropoffs").limit(5)
// val df_popular_dropoff2 = df_popular_dropoff.withColumnRenamed("count", "number_of_dropoffs")
// df_popular_dropoff = df_popular_dropoff.withColumn("count", df_popular_dropoff.count.cast('int'))


// COMMAND ----------

// PART 1b: The top-5 most popular pickup locations - "PULocationID", sorted in descending order - if there is a tie, then one with lower "PULocationID" gets listed first 
// Output Schema: PULocationID int, number_of_pickups int

// Hint: Code is very similar to part 1a above.

// ENTER THE CODE BELOW
val df_popular_pickup = df_filter.groupBy("PULocationID").count().orderBy(desc("count"),asc("PULocationID")).withColumnRenamed("count", "number_of_pickups").limit(5)



// COMMAND ----------

// PART 2: List the top-3 locations with the maximum overall activity, i.e. sum of all pickups and all dropoffs at that LocationID. In case of a tie, the lower LocationID gets listed first.
// Output Schema: LocationID int, number_activities int

// Hint: In order to get the result, you may need to perform a join operation between the two dataframes that you created in earlier parts (to come up with the sum of the number of pickups and dropoffs on each location). 
// val joined = family.joinWith(cities, family("cityId") === cities("id"))


// ENTER THE CODE BELOW
// empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"fullouter")
//     .show(false)


val df_popular_dropoff_all = df_filter.groupBy("DOLocationID").count().orderBy(desc("count"),asc("DOLocationID")).withColumnRenamed("count", "number_of_dropoffs")
val df_popular_pickup_all = df_filter.groupBy("PULocationID").count().orderBy(desc("count"),asc("PULocationID")).withColumnRenamed("count", "number_of_pickups")


val joined_df = df_popular_dropoff_all.join(df_popular_pickup_all, df_popular_dropoff_all("DOLocationID") === df_popular_pickup_all("PULocationID"), "fullouter")

val updated_joined_df = joined_df.withColumn("number_activities", col("number_of_pickups") + col("number_of_dropoffs"))

// val df_popular_dropoff2 = df_popular_dropoff.withColumnRenamed("DOLocationID", "LocationID")
// val df_popular_pickup2 = df_popular_pickup.withColumnRenamed("PULocationID", "LocationID")


// val joined_df = df_popular_dropoff2.join(df_popular_pickup2, "LocationID", "fullouter")

// val joined_df = df_filter.join(df_popular_pickup, "PULocationID")
// val updated_joined_df = joined_df.join(df_popular_dropoff, "DOLocationID")


// COMMAND ----------

// PART 3: List all the boroughs in the order of having the highest to lowest number of activities (i.e. sum of all pickups and all dropoffs at that LocationID), along with the total number of activity counts for each borough in NYC during that entire period of time.
// Output Schema: Borough string, total_number_activities int

// Hint: You can use the dataframe obtained from the previous part, and will need to do the join with the 'taxi_zone_lookup' dataframe. Also, checkout the "agg" function applied to a grouped dataframe.

// ENTER THE CODE BELOW

val sliced_joined_df = updated_joined_df.select("PULocationID", "number_activities")

// val joined_df = df_popular_dropoff_all.join(df_popular_pickup_all, df_popular_dropoff_all("DOLocationID") === df_popular_pickup_all("PULocationID"), "fullouter")


val borrough_df = sliced_joined_df.join(df_taxi_zone, sliced_joined_df("PULocationID") === df_taxi_zone("LocationID"))
val grouped_borrough_df = borrough_df.groupBy("Borough").sum("number_activities")
val final_grouped_borrough_df = grouped_borrough_df.withColumnRenamed("sum(number_activities)", "total_number_activities").orderBy(desc("total_number_activities"))


// orderBy(desc("count"),asc("DOLocationID"))

// COMMAND ----------

// PART 4: List the top 2 days of week with the largest number of (daily) average pickups, along with the values of average number of pickups on each of the two days. The day of week should be a string with its full name, for example, "Monday" - not a number 1 or "Mon" instead.
// Output Schema: day_of_week string, avg_count float

// Hint: You may need to group by the "date" (without time stamp - time in the day) first. Checkout "to_date" function.

// ENTER THE CODE BELOW
//     df = df.withColumn("tpep_pickup_datetime", df["tpep_pickup_datetime"].cast('timestamp'))


val df_filter_timestamped = df_filter.withColumn("pickup_datetime", df_filter("pickup_datetime").cast("timestamp"))
val df_date = df_filter_timestamped.withColumn("date_pickup", date_format(col("pickup_datetime"), "yyyy-MM-dd"))
val count_date_df = df_date.groupBy("date_pickup").count().withColumnRenamed("count", "count_per_day").withColumn("day_of_week", date_format(col("date_pickup"), "EEEE"))

val df_grouped_weekday = count_date_df.groupBy("day_of_week").agg(avg("count_per_day").as("avg_count")).orderBy(desc("avg_count")).limit(2)

// COMMAND ----------

// PART 5: For each particular hour of a day (0 to 23, 0 being midnight) - in their order from 0 to 23, find the zone in Brooklyn borough with the LARGEST number of pickups. 
// Output Schema: hour_of_day int, zone string, max_count int

// Hint: You may need to use "Window" over hour of day, along with "group by" to find the MAXIMUM count of pickups

//get the filter dataframe with right timestamp and hour of pickup
//get the pick up zones that are in the brooklyn zone
//count the maximum number of pickups doing the window max thingy over hour pickup column

// ENTER THE CODE BELOW

val df_filter_timestamped = df_filter.withColumn("pickup_datetime", df_filter("pickup_datetime").cast("timestamp"))
val df_hour_pickup = df_filter_timestamped.withColumn("hour_pickup", date_format(col("pickup_datetime"), "HH"))


//filtered data with only Brooklyn
val df_hour_pickup_count_borrough_join = df_hour_pickup.join(df_taxi_zone, df_hour_pickup("PULocationID") === df_taxi_zone("LocationID")).where(df_taxi_zone("Borough") === "Brooklyn")
val grouped_location_hour = df_hour_pickup_count_borrough_join.groupBy("PULocationID", "hour_pickup").count()


val window_def = Window.partitionBy("hour_pickup").orderBy(col("count").desc)
val partitioned_df = grouped_location_hour.withColumn("row", row_number.over(window_def)).where($"row" === 1).drop("row")
val updated_partitioned_df = partitioned_df.join(df_taxi_zone, partitioned_df("PULocationID") === df_taxi_zone("LocationID"))
val final_df = updated_partitioned_df.select("hour_pickup", "Zone", "count").orderBy(desc("hour_pickup"))


// COMMAND ----------

// PART 6 - Find which 3 different days of the January, in Manhattan, saw the largest percentage increment in pickups compared to previous day, in the order from largest increment % to smallest increment %. 
// Print the day of month along with the percent CHANGE (can be negative), rounded to 2 decimal places, in number of pickups compared to previous day.
// Output Schema: day int, percent_change float


// Hint: You might need to use lag function, over a window ordered by day of month.

// ENTER THE CODE BELOW
// converting to month data
val df_filter_timestamped = df_filter.withColumn("pickup_datetime", df_filter("pickup_datetime").cast("timestamp"))
val df_month_pickup = df_filter_timestamped.withColumn("month_pickup", date_format(col("pickup_datetime"), "MMMM"))

// filtering all data but the January ones
val df_filtered_month = df_month_pickup.filter(col("month_pickup") === "January")


// filter all data but the Manhattan ones
val manhattan_pickups_data = df_filtered_month.join(df_taxi_zone, df_filtered_month("PULocationID") === df_taxi_zone("LocationID")).where(df_taxi_zone("Borough") === "Manhattan").drop("LocationID", "Zone", "service_zone")

// get dates for each data
val df_day_pickup = manhattan_pickups_data.withColumn("day_pickup", date_format(col("pickup_datetime"), "D").cast("int"))


// groupby each dates to get counts 
val date_counts = df_day_pickup.groupBy("day_pickup").count()

val joined_date_counts = df_day_pickup.join(date_counts, "day_pickup")

val window_day_count = Window.partitionBy("day_pickup").orderBy(col("count"))

val lag_date_counts = joined_date_counts.select("day_pickup", "count").withColumn("previous_day",lag("count",1).over(window_day_count))
