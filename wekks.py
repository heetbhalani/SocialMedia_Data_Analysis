visits = ['Mon', 'Mon', 'Mon']
day_to_num = {'Mon': 0, 'Tue': 1, 'Wed': 2, 'Thu': 3, 'Fri': 4, 'Sat': 5, 'Sun': 6}

numerical_visits = [day_to_num[day] for day in visits]
print(numerical_visits)
num_cards = 1

start_of_week = numerical_visits[0]

for i in range(len(numerical_visits)-1):
    print(numerical_visits[i] <= numerical_visits[i+1])
    if numerical_visits[i] >= numerical_visits[i+1]:
        num_cards +=1


print(num_cards)


spark = SparkSession.builder \
        .appName("Job Count") \
        .getOrCreate()

    # Read the CSV file
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Group by 'job' and count occurrences
    job_counts = df.groupBy("job").count()

    # Sort by count ascending and then by job name ascending
    sorted_job_counts = job_counts.orderBy(col("count").asc(), col("job").asc())

    # Collect the results
    result = sorted_job_counts.collect()

    # Convert to dictionary
    job_dict = {row['job']: row['count'] for row in result}

    # Stop the Spark session
    spark.stop()

    # Return the ordered dictionary
    return OrderedDict(sorted(job_dict.items(), key=lambda x: (x[1], x[0])))    