from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder\
        .appName('Exercise6') \
        .getOrCreate()
    data = spark.read\
                .option("header","true")\
                .option("infer_schema","true")\
                .csv("/Users/mohan_sharma/Desktop/data engineer practice/exercise1/Exercises/Exercise-6/data/Divvy_Trips_2019_Q4.csv")

    data.createOrReplaceTempView("data_tbl")

    # What is the average trip duration per day?
    avg_trip_duration = spark.sql("""select date(start_time) as date ,avg(tripduration) 
                        from data_tbl 
                        group by date(start_time)
                        order by 1
                        """)

    # How many trips were taken each day?
    trips_each_day = spark.sql("""select date(start_time) as date ,count(*) as total_trips
                        from data_tbl 
                        group by date(start_time)
                        order by 1
                        """)

    # What was the most popular starting trip station for each month?
    most_popular_starting_station = spark.sql("""
                        with cte as (
                        select extract(MONTH from start_time) as month,
                        from_station_name ,count(*) as total_trips,
                        row_number() over (partition by extract(MONTH from start_time) order by count(*) desc) as rn
                        from data_tbl 
                        group by 1,2
                        )
                        select month,from_station_name,total_trips from cte where rn = 1
                        order by 1"""
                        )
    # What were the top 3 trip stations each day for the last two weeks?
    top_3_trip_station_2weeks = spark.sql("""
                        select date,from_station_name,trips from(
                        select date(start_time) as date,
                        from_station_name,
                        count(*) as trips,
                        row_number() over(partition by date(start_time) order by count(*) desc) as rn 
                        from data_tbl 
                        where date(start_time) >= date_sub((select date(max(start_time)) from data_tbl),14)
                        group by date(start_time),from_station_name
                        order by 1,trips desc)
                        where rn <= 3
                        """)

    # Do Males or Females take longer trips on average?
    male_or_females = spark.sql("""
                        select gender,avg(tripduration) from data_tbl
                        where gender is not null
                        group by gender order by 2 desc
                        """)
    # What is the top 10 ages of those that take the longest trips, and shortest?
    top_10_ages = spark.sql("""
                        select age,trip_dur from (
                        (select (extract(year from current_date()) - birthyear ) as age,
                        avg(tripduration) as trip_dur
                        from data_tbl
                        where birthyear is not null
                        group by 1
                        order by 2 desc limit 10)
                        union all 
                        (select (extract(year from current_date()) - birthyear ) as age,
                        avg(tripduration) as trip_dur
                        from data_tbl
                        where birthyear is not null
                        group by 1
                        order by 2  limit 10)
                        
                        ) order by 2 desc,1
                        """)





if __name__ == '__main__':
    main()
