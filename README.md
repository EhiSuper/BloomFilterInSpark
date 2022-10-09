# BloomFilterInSpark

<h2> Description </h2>
<h4>
This is a project for my Cloud Computing course @ University of Pisa.<br>
The project consist in the creation of 10 bloom filters starting from a specific file that contains the film title and the relative average rating, made available by IMDb.<br>
The Application is completely developed using Spark, and it is written in Python.<br>
For more specification on <a href=https://spark.apache.org/docs/latest/>Apache Spark</a><br>
For more specification on how to use python for spark: <a href=https://spark.apache.org/docs/latest/api/python/>Pyspark</a><br>
</h4>
<h2>Utilization</h2>
<h4>
<ol>
  <li>Install Pyspark with bot pip or anaconda (you can also use a preinstalled spark installation with a yarn hadoop cluster):
    <ul>
      <li>Pip: pip install pyspark</li>
      <li>Conda: conda install -c conda-forge pyspark</li>
    </ul>
  </li>
   <li>Go to the src directory.</li>
   <li>Run: spark-submit BloomFiltersApp.py [file] [false_positive_rate] [partitions] <br>
   Where file is the file to be processed under the directory Data/Input/.<br>
   False positive rate is the desired false positive rate for each of the 10 bloom filter.<br>
   Partitions is the number of partitions in which the application divide the RDD.
   </li>
   <li>Results: you can find the results in the directory Data/Output/.</li>
</ol>

</h4>
