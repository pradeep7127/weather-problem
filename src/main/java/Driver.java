import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.log4j.*;

import static org.apache.spark.sql.functions.*;




//Using the global weather data, answer the following:
//        1. Which country had the hottest average mean temperature over the year?
//        2. Which country had the most consecutive days of tornadoes/funnel cloud
//        formations?
//        3. Which country had the second highest average mean wind speed over the year?


public class Driver {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder().master("local[2]")
                .appName("Weather Data Analysis")
                .getOrCreate();

        Logger.getLogger("org").setLevel(Level.ERROR);

        Dataset<Row> weatherData = readData(spark, "../weather-problem/data/2019/");
        Dataset<Row> stationList = readData(spark, "stationlist.csv");
        Dataset<Row> countryList = readData(spark, "countrylist.csv");

        Dataset<Row> stationCountryInfo = stationList.join(countryList,"COUNTRY_ABBR");

        stationCountryInfo.persist(StorageLevel.MEMORY_ONLY());

        weatherData = weatherData.withColumnRenamed("STN---","STN_NO")
                .join(stationCountryInfo,"STN_NO");

        weatherData.createOrReplaceTempView("weather_data");

        //        1. Which country had the hottest average mean temperature over the year?
        Dataset<Row> highestAverageMeanTemp = spark.sql("select  COUNTRY_FULL, AVG(TEMP) as temp from weather_data " +
                "where TEMP != 9999.9 group by COUNTRY_FULL order by temp desc limit 1");
        highestAverageMeanTemp.show();

        //        2. Which country had the most consecutive days of tornadoes/funnel cloud
        //        formations?
        // It seems there are no consecutive instances of tornadoes or funnels

        Dataset<Row> consecutiveDays = spark.sql("WITH temp_table AS (   SELECT    COUNTRY_FULL,    to_date(cast(YEARMODA as string),'yyyyMMdd') as YEARMODA ,    date_add(to_date(cast(YEARMODA as string),'yyyyMMdd'), -(row_number() over (partition by COUNTRY_FULL order by to_date(cast(YEARMODA as string),'yyyyMMdd')))) as diff  FROM weather_data where SUBSTRING(FRSHTT,6,1)='1') SELECT  COUNTRY_FULL,  COUNT(1) AS duration FROM temp_table GROUP BY COUNTRY_FULL, diff having duration >1 order by duration desc  ");
        consecutiveDays.limit(1).show();

        //        3. Which country had the second highest average mean wind speed over the year?
        Dataset<Row> secondHigestWindSpeed = spark.sql("select  COUNTRY_FULL, AVG(WDSP) as wind_speed from " +
                "weather_data where WDSP != 999.9 group by COUNTRY_FULL order by wind_speed desc limit 2");
        secondHigestWindSpeed.sort(asc("wind_speed")).limit(1).show();


    }

    private static Dataset<Row> readData(SparkSession spark, String path) {
        return spark.read().format("csv")
                .option("header","true")
                .option("inferSchema","true")
                .load(path);
    }

}
