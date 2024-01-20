package org.example

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Task2 {

  // --- Brute Force Solution---

  /**
   * This is the brute force solution to find the top k points based on their dominance score.
   * First, we create a cartesian product for the input rdd with it self,
   * then we group the pairs based on the key and then using the function countDominatedPoints, we calculate its
   * dominanace score. Lastly, we sort them in an descending order and we return the top k
   * @param data input rdd with all the points
   * @param topK number of top elements we want to find
   * @return an array containing the top k elements with their dominance score
   */

  def task2BruteForce(data: RDD[List[Double]], topK: Int): Array[(List[Double], Long)] = {
    data.cartesian(data) //get the cartesian product of the data rdd with it self
      .filter(pair => pair._1 != pair._2) //filter the pairs that are the same point for the key and value
      .groupByKey() //group them based on the key
      .map(pair => (pair._1, countDominatedPoints(pair._1, pair._2))) // map its point to a tuple where the first element is the point and the second its dominance score
      .sortBy(_._2) //sort map by value in descending order
      .top(topK)(Ordering[Long].on(_._2)) //take the first k
  }

  /**
   * This is a helper function used in the brute force solution, that return the number of points the point dominates
   * @param point the point we want to find its dominance score
   * @param values all the other points we have to check if it dominates
   * @return the total number of points the 'point' dominates
   */
  def countDominatedPoints(point: List[Double], values: Iterable[List[Double]]): Long = {
    var totalPoints = 0
    values
      .filter(p => !p.equals(point))
      .foreach(nums => {
        if(Task1.dominates(point, nums)) totalPoints += 1
      })
    totalPoints
  }

  //------ Partitioning ----
  /**
   * Finds top k dominating points using the algorithm STD. It finds the dominating points in parallel for each
   * partition and then combine them to find the global dominating points.
   * @param data input rdd with all the points
   * @param top number of points we want to find
   * @param sc spark context
   * @return an iterator with the top dominating points
   */
  def topKDominatingPoints(data: RDD[List[Double]], top: Int, sc: SparkContext): Array[(List[Double], Long)] = {
    val skylines = Task1.ALS(data).toList //find skyline points
    data.mapPartitions(par => STD(par, top, skylines))
      .top(top)(Ordering[Long].on(_._2))
  }

  /**
   * It implements the STD algorithm to find the top-k dominating elements
   * @param data input data with all the points
   * @param top number of points we want to find
   * @param skylines the points that are skylines in the dataset
   * @return an iterator with the top dominating points
   */
  def STD(data: Iterator[List[Double]], top: Int, skylines: List[List[Double]]): Iterator[(List[Double], Long)] = {
    if (top == 0) Array()

    val points = data.toList

    var skylinePoints = skylines
      .map(point => Tuple2(point, countDominatedPoints(point, points))) //for every skyline point calculate its dominance score
      .to[ArrayBuffer]
      .sortBy(-_._2) //sort them based on their dominance score

    var result = ArrayBuffer[(List[Double], Long)]() //add the point with the max dominance score to the result array
    var topK = top
    // until all the top-k elements have been found
    while(topK > 0) {
      val pointToAdd = skylinePoints.apply(0) //get the first point of the skylinePoints buffer
      result :+= pointToAdd //add first point(the one with the maximum score) to the result array

      skylinePoints.remove(0) //remove the point since we have already added to the result
      topK -= 1
      val currPoint = pointToAdd._1

      points
        .filter(p => !p.equals(currPoint)) //filter the current point
        .filter(p => !skylinePoints.map(_._1).contains(p)) //filter the point if it's already in toCalculatePoints array
        .filter(p => Task1.dominates(currPoint, p)) //get only the points that are dominated by 'point'
        .filter(p => isInRegion(currPoint, p, skylinePoints)) //get only the points belonging to region of current point
        .map(p => (p, countDominatedPoints(p, points))) //count the dominated points for each
        .foreach(p => skylinePoints.append(p)) //add every point toCalculatePoints array

      //Add points belonging only to region of point to the toCalculatePoints RDD
      skylinePoints = skylinePoints
        .sortWith(_._2 > _._2) //sort points based on their dominance score
    }

    result.toIterator
  }

  // ----- STD ----
  /**
   * It implements the STD algorithm to find the top-k dominating elements. It used recursion.
   * @param data input data with all the points
   * @param top number of points we want to find
   * @return an array with the top dominating points and their dominance score
   */
  def STD(data: RDD[List[Double]], top: Int): Array[(List[Double], Long)] = {
    if(top == 0) Array()

    val skylinePoints2 = Task1.ALS(data).toList //find skyline points

    //creates a new rdd containing tuples where the first elements are skyline points and the second is 0 or 1.
    // 1 if a point (from the input dataset) was found that the skyline point dominated, and 0 otherwise.
    val newRdd = data
      .flatMap(point => {
        val skylinePointsBuffer = ArrayBuffer[(List[Double], Long)]()
        skylinePoints2.foreach(t => {
          if (Task1.dominates(t, point)) {
            skylinePointsBuffer.append((t, 1L))
          }
        })
        skylinePointsBuffer
      })

    //Sorts the skyline points by their dominance score in a descending order
    val skylinePoints = newRdd
      .reduceByKey(_+_)
      .collect()
      .to[ArrayBuffer]
      .sortBy(-_._2) //sort them based on their dominance score

    val point = skylinePoints.head //takes the first element from the skylinePoints array (meaning the top-1 point)
    val result = Array(point) //add the point with the max dominance score to the result array
    skylinePoints.remove(0) //remove it from the skylinePoints array since its no longer needed to be checked

    val answer = calculatePoints(top - 1, point._1, data.filter(p => !p.equals(point)), result, skylinePoints)

    answer
  }

  // ----- STD ----
  /**
   * It implements the STD algorithm to find the top-k dominating elements. It used recursion.
   * @param data input data with all the points
   * @param top number of points we want to find
   * @return an array with the top dominating points and their dominance score
   */
  def STD(data: RDD[List[Double]], top: Int, sc: SparkContext): Array[(List[Double], Long)] = {
    if(top == 0) Array()

    val skylinePoints = sc.parallelize(Task1.ALS(data).toList).cartesian(data) //cartesian product of the skyline points with the input data
      .filter(pair => pair._1 != pair._2) //filter the pairs that have the same elements
      //map each pair to the first element, and 1 if the first point dominated the second, and 0 otherwise
      .map(points => Tuple2(points._1, if (Task1.dominates(points._1, points._2)) 1L else 0L))
      .reduceByKey(_+_) //for every skyline point calculate its dominance score
      .sortBy(-_._2) //sort them based on their dominance score
      .collect()
      .to[mutable.ArrayBuffer]

    val point = skylinePoints.head
    val result = Array(point) //add the point with the max dominance score to the result array
    skylinePoints.remove(0)

    val answer = calculatePoints(top - 1, point._1, data.filter(p => !p.equals(point)), result, skylinePoints)

    answer
  }

  /**
   * Recursive function used to calculate the top-k points for the STD algorithm. Runs until all the points
   * have been found (topk equals 0)
   * @param topK number of points we want to find
   * @param point current point
   * @param data all the points
   * @param result top k dominating points
   * @param toCalculatePoints array for the points that need to be checked
   * @return the topK dominating points (result array)
   */
  @tailrec
  def calculatePoints(topK: Int, point: List[Double], data: RDD[List[Double]], result: Array[(List[Double], Long)],
                      toCalculatePoints: ArrayBuffer[(List[Double], Long)]): Array[(List[Double], Long)] = {
    if(topK == 0)  return result

    var res = result

    data
      .filter(p => !p.equals(point)) //filter the current point
      .filter(p => !toCalculatePoints.map(_._1).contains(p)) //filter the point if it's already in toCalculatePoints array
      .filter(p => Task1.dominates(point, p)) //get only the points that are dominated by 'point'
      .filter(p => isInRegion(point, p, toCalculatePoints)) //get only the points belonging to region of current point
      .cartesian(data)
      .map(points => Tuple2(points._1, if (Task1.dominates(points._1, points._2)) 1L else 0L))
      .reduceByKey(_+_) //for every skyline point calculate its dominance score
      .sortBy(-_._2) //sort them based on their dominance score
      .collect()
      .foreach(p => toCalculatePoints.append(p))

    //Add points belonging only to region of point to the toCalculatePoints RDD
    val newCalculatePoints = toCalculatePoints
      .sortWith(_._2 > _._2) //sort points based on their dominance score

    val pointToAdd = toCalculatePoints.apply(0)
    res :+= pointToAdd //add first point(the one with the maximum score) to the result array

    newCalculatePoints.remove(0)
    calculatePoints(topK - 1, pointToAdd._1, data.filter(p => !p.equals(pointToAdd)), res, newCalculatePoints)
  }

  // ---- Helper Functions ----

  /**
   * Finds if pointB is in the region of pointA
   * @param pointA the pointA
   * @param pointB the pointB
   * @param skylines the skyline points
   * @return true if pointB is in region of pointA, else false
   */
  def isInRegion(pointA: List[Double], pointB: List[Double], skylines: ArrayBuffer[(List[Double], Long)]): Boolean = {
    skylines
      .map(_._1) //get the points, without their dominance score
      .filter(p => !p.equals(pointA))  //get the points that are not equal to pointA
      .map(point => !Task1.dominates(point, pointB)) //map to false or true depending if pointB is not dominated by point
      .reduce(_&&_)
  }

  /**
   * Count the points, point dominates from the points dataset
   * @param point the point we want to find how many points dominates
   * @param points the points dataset
   * @return the number of points
   */
  def countDominatedPoints(point: List[Double], points: List[List[Double]]): Long = {
    points
      .filter(p => !p.equals(point))
      .count(p => Task1.dominates(point, p))
  }

  //------------------------------------------------------------------------------------------------------------------

  // Given a point, return the CellID( Coordinates of cell in D-dimension space) that it belongs assuming that max cell per dim are 5
  def GetCellID(point: List[Double]) ={
    val cell_id: List[Int] = point.map( elem => (BigDecimal(elem) / BigDecimal("0.2")).toInt )
    cell_id
  }

  def findCellsGreaterOrEqual(startingCell: List[Int], cubeSize: Int, dimensions: Int): List[List[Int]] = {
    var resultCells = List[List[Int]]()

    def iterate(currentCell: List[Int], currentDimension: Int): Unit = {
      if (currentDimension == dimensions) {
        // Base case: reached the last dimension, add the current cell to the result
        resultCells = resultCells :+ currentCell
      } else {
        for (i <- currentCell(currentDimension) to cubeSize) {
          // Recursive case: iterate through the current dimension
          val nextCell = currentCell.updated(currentDimension, i)
          iterate(nextCell, currentDimension + 1)
        }
      }
    }

    // Start the recursion from the first dimension
    iterate(startingCell, 0)

    resultCells
  }

  // Calculate the minimum and maximum dominance of a point that belongs to a specific cell
  def GetMinMaxCount(point: List[Double], CountsPerCell: Map[List[Int], Int], dimensions: Int): (Long, Long) ={

    // MIN
    // Add 1 to all dims to take the cell that is definitely dominated by the point and then find all the cells outwards that
    var countsForCoordinates_min: List[Int] = List()
    var outwardCoordinates_min: List[List[Int]] = List()
    val starting_cell_min: List[Int] = point.map(elem => ((BigDecimal(elem) / BigDecimal("0.2")).toInt + 1))

    if (!starting_cell_min.exists(elem => elem > 4)) {
      outwardCoordinates_min = findCellsGreaterOrEqual(starting_cell_min, 4, dimensions)
      // Extract counts for each list in outwardCoordinates
      countsForCoordinates_min = outwardCoordinates_min.map { coordinates =>
        CountsPerCell.getOrElse(coordinates, 0)
      }
    }

    // MAX
    val starting_cell_max: List[Int] = point.map( elem => (BigDecimal(elem) / BigDecimal("0.2")).toInt )
    val outwardCoordinates_max = findCellsGreaterOrEqual(starting_cell_max, 4, dimensions)
    val countsForCoordinates_max: List[Int] = outwardCoordinates_max.map { coordinates =>
      CountsPerCell.getOrElse(coordinates, 0)
    }

    (countsForCoordinates_min.sum.toLong, countsForCoordinates_max.sum.toLong)
  }


  // Return true if point_B is dominated by point_A
  def IsDominatedByPoint(point_A: List[Double], point_B: List[Double]): Boolean = {
    point_A.zip(point_B).forall(pair => pair._1 <= pair._2)
  }

  // Compare a given point with the points of the given block_id
  def Count_Dominance_in_Cells(point: List[Double], points_to_check: RDD[(List[Double], List[Int])]): Long = {
    val points_dominated =
      points_to_check
        //        .flatMap(_._2)
        .filter(pair => !pair._1.equals(point)) // exclude the point we are checking
        .filter(pair => IsDominatedByPoint(point, pair._1))
        .count()
        .toLong

    points_dominated
  }

  // Get the total dominance score of a given point
  def GetTotalCount(point: List[Double], minCount: Long , points_with_cells: RDD[(List[Double], List[Int])]): Long ={
    var sum = minCount
    sum = sum + Count_Dominance_in_Cells(point, points_with_cells)
    sum
  }

  // Get the total dominance score of a given point
  def FindNeighbouringCells(point: List[Double], CountsPerCell: Map[List[Int], Int], dimensions: Int): List[List[Int]] ={

    // MIN
    // Add 1 to all dims to take the cell that is definitely dominated by the point and then find all the cells outwards that
    var outwardCoordinates_min: List[List[Int]] = List(List())
    val starting_cell_min: List[Int] = point.map(elem => ((BigDecimal(elem) / BigDecimal("0.2")).toInt + 1))
    if (!starting_cell_min.exists(elem => elem > 4)) {
      outwardCoordinates_min = findCellsGreaterOrEqual(starting_cell_min, 4, dimensions)
    }

    // MAX
    val starting_cell_max: List[Int] = point.map( elem => (BigDecimal(elem) / BigDecimal("0.2")).toInt )
    val outwardCoordinates_max = findCellsGreaterOrEqual(starting_cell_max, 4, dimensions)

    outwardCoordinates_max.diff(outwardCoordinates_min)
  }

  def Top_k_GridDominance(data: RDD[List[Double]],dimensions: Int ,top: Int, sc: SparkContext): Array[(List[Double], Long)] = {

    // Create an RDD of the data points along with the BLock ID RDD: (point,BlockID)
    val points_with_cellID =
      data
        .map(point => (point, GetCellID(point)))
    //        .groupBy { case (_, cellID) => cellID }
    //        .mapValues(iter => iter.map { case (point, _) => point })

    val CountsPerCell = data
      .map(point => (GetCellID(point), 1))
      .aggregateByKey(0)(_ + _, _ + _)
      .collect()
      .toMap

    val points_with_min_max =
      data
        .map { point =>
          val (minCount, maxCount) = GetMinMaxCount(point, CountsPerCell, dimensions)
          (point, minCount, maxCount)
        }
        .sortBy(_._3, ascending= false)

    val minCountOfFirstElement: Long = points_with_min_max.first._2

    val candidate_points =
      points_with_min_max
//        .filter(  _._3 >=  minCountOfFirstElement)    // I assume that Always gives an RDD greater or equal than top-k and that it fits to the memory
        .collect()
        .toList

    val top_k =
      candidate_points
        //        .collect()
        //        .toList
        .map( triplet => (triplet, FindNeighbouringCells(triplet._1, CountsPerCell, dimensions))) // point, minCount, maxCount, Neighbouring Cells to check
        .map( triplet => (triplet._1, GetTotalCount(triplet._1._1, triplet._1._2, points_with_cellID.filter(pair => triplet._2.contains(pair._2) ))))
        .map( triplet => (triplet._1._1, triplet._2)) // Keep only point and score

    top_k.sortBy(_._2)(Ordering[Long].reverse)
      .take(top)
      .toArray
  }

}
