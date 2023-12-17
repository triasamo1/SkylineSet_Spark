package org.example

import org.apache.spark.rdd.RDD

object Task3 {
  def task3BruteForce(data: RDD[List[Double]], top: Int) = {
    //TODO check if size < top
    Task1.task1BruteForce(data)
      .collect()
      .map(point => Tuple2(point, Task2.countDominatedPoints2(point, data.collect())))
      .sortBy(-_._2)
      .take(top)
  }
}
