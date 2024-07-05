spark.sql("""Select movieid,
sum(rating) as Total_ratings,
from sparkdatalake.ratings 
group by movieid
order by cast(movieid as int) asc
""").repartition(1).write.format("csv").option("header","true").save("result")
System.exit(0)
