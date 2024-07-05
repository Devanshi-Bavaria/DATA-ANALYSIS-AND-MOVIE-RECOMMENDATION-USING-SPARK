# Data Analysis and Movie Recommendation Using Spark
Data is transforming the film industry, and movie analytics using Scala and Spark addresses critical challenges in this dynamic field. This project harnesses Scala's expressiveness and Spark's distributed processing power to optimize content creation and investment decisions. By analyzing movie-related datasets with Spark RDDs, Spark SQL, and Spark DataFrames, it uncovers valuable insights, enhances decision-making, and improves the movie-watching experience for audiences.

# Tech Stack
- **Apache Spark**: High-performance distributed computing system for large-scale data processing and analytics.
- **Scala**: Dynamic programming language blending functional and object-oriented concepts, running on the JVM for enhanced developer productivity.

# Installation Steps for Apache Spark

1. **Download and install Python 3.11**
2. **Download and install Java SDK 17**
3. **Download Spark 3.4.1 pre-built for Apache Hadoop 3.3 & later**
4. **Extract the Spark download files**
5. **Download Hadoop winutils from GitHub**
6. **Create folder structure:**
    - `C:\SPARK`
    - `C:\HADOOP\bin`
7. **Copy the downloaded Spark and Hadoop files into the above folders**
8. **Set the environment variables for Java, Hadoop, and Spark**
9. **In CMD, from the path `C:/SPARK/bin`, execute the command:**
   ```sh
   spark-shell

# Dataset

We have three datasets named `movies.dat`, `ratings.dat`, and `users.dat` which are explained below:

1. **ratings.dat**: This dataset consists of user ratings with the following columns:
   - **UserID**: Ranges from 1 to 6040. Each user has rated at least 20 movies. References UserID in `users.dat`.
   - **MovieID**: Ranges from 1 to 3952. References MovieID in `movies.dat`.
   - **Rating**: Ratings are on a scale of 1 to 5 stars.
   - **Timestamp**: Represented in seconds since the epoch as returned by time.

2. **users.dat**: This dataset consists of user data with the following columns:
   - **UserID**: Ranges from 1 to 6040 and is unique for all users.
   - **Gender**: 'M' for male and 'F' for female.
   - **Age**: Categorized into ranges such as under 18, 18-24, 25-34, 35-44, 45-49, 50-55, 56+.
   - **Occupation**: Categorized into 20 different categories such as farmer, artist, homemaker, customer service, healthcare, retired, scientist, engineer, tradesman, unemployed, executive/managerial, K-12 student, educator, programmer, lawyer, etc.
   - **Zip-code**: User-provided postal codes.

3. **movies.dat**: This dataset consists of movie information with the following columns:
   - **MovieID**: Ranges from 1 to 3952 and is unique for all movies.
   - **Title**: Unique movie titles provided by IMDB.
   - **Genres**: Includes more than 10 genres such as action, adventure, fantasy, drama, war, western, thriller, horror, musical, animation, etc.

# Data Preparation:

1. **Prepare Movies dataset**: Clean delimited data and extract the year and genre.
2. **Prepare Users dataset**: Load a double-delimited CSV file into a DataFrame and specify the schema programmatically.
3. **Prepare Ratings dataset**: Load a double-delimited CSV file into a DataFrame and specify the schema programmatically.

To achieve the above results, run the command `sh execute.sh` in the terminal at the relevant path.


# Data Analysis Results

### Find the latest released movies.

```scala
val movies_rdd=sc.textFile("../../Movielens/movies.dat")
val movie_nm=movies_rdd.map(lines=>lines.split("::")(1))
val year=movie_nm.map(lines=>lines.substring(lines.lastIndexOf("(")+1,lines.lastIndexOf(")")))
val latest=year.max
val latest_movies=movie_nm.filter(lines=>lines.contains("("+latest+")")).saveAsTextFile("result")
System.exit(0)
```

![image](https://github.com/Devubavariaa/DATA-ANALYSIS-AND-MOVIE-RECOMMENDATION-USING-SPARK/assets/172360482/1d5c4643-6ef7-4075-b1e2-84c8b0baf3e7)

### Identify distinct genres in the dataset.

```scala
val movies_rdd = sc.textFile("../../Movielens/movies.dat")
val genres = movies_rdd.map(lines => lines.split("::")(2))
val testing = genres.flatMap(line => line.split('|'))
val genres_distinct_sorted = testing.distinct().sortBy(_(0))
genres_distinct_sorted.saveAsTextFile("result")
System.exit(0)
```

![image](https://github.com/Devubavariaa/DATA-ANALYSIS-AND-MOVIE-RECOMMENDATION-USING-SPARK/assets/172360482/55f61ff1-2050-41d0-89df-9835817709f0)

### Count the number of movies in each genre.

```scala
val movies_rdd=sc.textFile("../../Movielens/movies.dat")
val genre=movies_rdd.map(lines=>lines.split("::")(2))
val flat_genre=genre.flatMap(lines=>lines.split("\\|"))
val genre_kv=flat_genre.map(k=>(k,1))
val genre_count=genre_kv.reduceByKey((k,v)=>(k+v))
val genre_sort= genre_count.sortByKey()
genre_sort.saveAsTextFile("result-csv")
System.exit(0)
```
![image](https://github.com/Devubavariaa/DATA-ANALYSIS-AND-MOVIE-RECOMMENDATION-USING-SPARK/assets/172360482/a6c2be5e-de7c-43fb-aa63-8e1df0a2892e)

### Count the number of movies starting with letters or numbers.

```scala
val movies_rdd=sc.textFile("../../Movielens/movies.dat")
val movies=movies_rdd.map(lines=>lines.split("::")(1))
val string_flat=movies.map(lines=>lines.split(" ")(0))
// check for the first character for a letter then find the count
val movies_letter=string_flat.filter(word=>Character.isLetter(word.head)).map(word=>(word.head.toUpper,1))
val movies_letter_count=movies_letter.reduceByKey((k,v)=>k+v).sortByKey()
// check for the first character for a digit then find the count
val movies_digit=string_flat.filter(word=>Character.isDigit(word.head)).map(word=>(word.head,1))
val movies_digit_count=movies_digit.reduceByKey((k,v)=>k+v).sortByKey()
// Union the partitions into a same file
val result=movies_digit_count.union(movies_letter_count).repartition(1).saveAsTextFile("result-csv")
System.exit(0)
```

![image](https://github.com/Devubavariaa/DATA-ANALYSIS-AND-MOVIE-RECOMMENDATION-USING-SPARK/assets/172360482/2a7a73d3-07ee-4281-be5b-f24aa8153f0c)

### To retrieve the most popular films.

```scala
val ratingsRDD=sc.textFile("../../Movielens/ratings.dat")
val movies=ratingsRDD.map(line=>line.split("::")(1).toInt)
val movies_pair=movies.map(mv=>(mv,1))

val movies_count=movies_pair.reduceByKey((x,y)=>x+y)
val movies_sorted=movies_count.sortBy(x=>x._2,false,1)

val mv_top10List=movies_sorted.take(10).toList
val mv_top10RDD=sc.parallelize(mv_top10List)

val mv_names=sc.textFile("../../Movielens/movies.dat").map(line=>(line.split("::")(0).toInt,line.split("::")(1)))
val join_out=mv_names.join(mv_top10RDD)
join_out.sortBy(x=>x._2._2,false).map(x=> x._1+","+x._2._1+","+x._2._2).repartition(1).saveAsTextFile("Top-10-CSV")
System.exit(0)
```
![image](https://github.com/Devubavariaa/DATA-ANALYSIS-AND-MOVIE-RECOMMENDATION-USING-SPARK/assets/172360482/5e240a6a-685f-407a-bf76-5a61fc561e32)

### To obtain average rating per movie.

```scala
spark.sql("""Select movieid,
cast((avg(rating)) as decimal(16,2))  as Average_ratings 
from sparkdatalake.ratings 
group by movieid
order by cast(movieid as int) asc
""").repartition(1).write.format("csv").option("header","true").save("result")
System.exit(0)
```

![image](https://github.com/Devubavariaa/DATA-ANALYSIS-AND-MOVIE-RECOMMENDATION-USING-SPARK/assets/172360482/c9bb6c51-af90-48f9-9d37-ac5802df6e45)

### To obtain list of the oldest movies.

```scala
val movies_rdd=sc.textFile("../../Movielens/movies.dat")
// 1st method, convert existing rdd into DF using toDF function and then make it into a view
val movies_DF=movies_rdd.toDF.createOrReplaceTempView("movies_view")
// To use spark.sql, it should be at least a temporary view or even an table
spark.sql(""" select
split(value,'::')[0] as movieid,
split(value,'::')[1] as moviename,
substring(split(value,'::')[1],length(split(value,'::')[1])-4,4) as year
from movies_view """).createOrReplaceTempView("movies");
// To view the records, use spark.sql("select * from movies").show()
var result=spark.sql("Select * from movies m1 where m1.year=(Select min(m2.year) from movies m2)").repartition(1).rdd.saveAsTextFile("result")
System.exit(0);
```

![image](https://github.com/Devubavariaa/DATA-ANALYSIS-AND-MOVIE-RECOMMENDATION-USING-SPARK/assets/172360482/7849b8f4-e7b1-422c-bfe5-64abbf3b1628)

# Movie Recommendations Results
### Generate top 10 user recommendations for each movie

![image](https://github.com/Devubavariaa/DATA-ANALYSIS-AND-MOVIE-RECOMMENDATION-USING-SPARK/assets/172360482/d688eb4b-5111-4c0e-8182-427e4c77d8d7)

### Generate top 10 movie recommendations for each user

![image](https://github.com/Devubavariaa/DATA-ANALYSIS-AND-MOVIE-RECOMMENDATION-USING-SPARK/assets/172360482/fbf9fe5a-8946-4172-8892-1b9c2ab7f2e8)


### Generate top 10 user recommendations for a specified set of users and movies

![image](https://github.com/Devubavariaa/DATA-ANALYSIS-AND-MOVIE-RECOMMENDATION-USING-SPARK/assets/172360482/7453471d-275b-4945-a99f-f6ff3bcc46b8)





