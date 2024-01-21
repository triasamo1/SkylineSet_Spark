package org.example

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object Task3 {

  // --- Brute Force Solution---

  /**
   * This is the brute force solution to find the top k skyline points based on their dominance score.
   * First,find the skyline points (using the function skylinesBruteForce) and then we map the points to a Tuple with
   * itself as first element and its dominance score as second. We calculate its dominance score using the function
   * countDominatedPoints where we give the input data as input alongside with the skyline point.
   * Lastly, we sort them in an descending order and we return the top k
   * @param data input data with all the points
   * @param top number of points we want to find
   * @return an array with the top dominating points and their dominance score
   */
  def topKSkylineBruteForce(data: RDD[List[Double]], top: Int): Array[(List[Double], Long)] = {
    Task1.skylinesBruteForce(data) //get skyline points
      .collect()
      .map(point => (point, countDominatedPoints(point, data))) //map them to a Tuple with point as first element and its dominance score as second
      .sortBy(-_._2) //sort elements in a descending order based on their dominance score
      .take(top) //take first 'top' elements
  }

  //----- Solution 2 -----

  /**
   * First, we broadcast the input data rdd, and then using the method ALS from Task1 we find the skyline points.
   * Then we map the skyline points to partitions and for each partition we calculate for its points their dominance
   * score. We calculate their dominance score using the function countDominatedPoints that takes as argument
   * the point and the broadcasted value. Lastly, we sort them in an descending order based on their dominance
   * score and we return the top k.
   * @param data input data with all the points
   * @param top number of points we want to find
   * @param sc spark context
   * @return an array with the top dominating points and their dominance score
   */
  def topKSkylinePoints(data: RDD[List[Double]], top: Int, sc: SparkContext): Array[(List[Double], Long)] = {
    val broadcastData = sc.broadcast(data.collect()) //broadcast data rdd
    val skylines = Task1.ALS(data).toList //find skyline points

    sc.parallelize(skylines)
      .mapPartitions(par => par //map to partitions
        .map(point => (point, countDominatedPoints(point, broadcastData)))) //for each point in the partition find its dominance score
      .sortBy(-_._2) //sort elements in a descending order based on their dominance score
      .take(top) //take first 'top' elements
  }

  //----- Solution 3 -----

  /**
   * First, we create a cartesian product of the skyline rdd (was found using function ALS from Task1) with
   * the input rdd, then we remove pairs that have the same element as second and first. After that, we find for every
   * pair if the first element dominates the second.If does we map the element to a tuple with itself and 1 if
   * it doesn't, we map the element to a tuple with itself and 0. Then we reduce the rdd by merging the values
   * of each key by adding every value for that key. We do that to find for its point its dominance score.
   * Lastly, we sort them in an descending order based on their dominance score and we return the top k
   * @param data input data with all the points
   * @param top number of points we want to find
   * @param sc spark context
   * @return an array with the top dominating points and their dominance score
   */
  def topKSkylinePoints2(data: RDD[List[Double]], top: Int, sc: SparkContext): Array[(List[Double], Long)] = {
    sc.parallelize(Task1.ALS(data).toList).cartesian(data)
      .filter(pair => pair._1 != pair._2)
      .map(pair => (pair._1, if (Task1.dominates(pair._1, pair._2)) 1L else 0L))
      .reduceByKey(_+_)
      .sortBy(-_._2)
      .take(top)
  }

  // --- Helper Functions ---

  def countDominatedPoints(point: List[Double], values: Broadcast[Array[List[Double]]]): Long = {
    var totalPoints = 0
    values
      .value
      .filter(p => !p.equals(point))
      .foreach(nums => {
        if(Task1.dominates(point, nums)) totalPoints += 1
      })
    totalPoints
  }

  def countDominatedPoints(point: List[Double], points: Iterable[List[Double]]): Long = {
    points
      .filter(p => !p.equals(point))
      .count(p => Task1.dominates(point, p))
  }

  def countDominatedPoints(point: List[Double], points: RDD[List[Double]]): Long = {
    points
      .filter(p => !p.equals(point))
      .filter(p => Task1.dominates(point, p))
      .count()
  }

  //------------------------------------------------------------------------------------------------------------------

  // Given a point, return the CellID( Coordinates of cell in D-dimension space) that it belongs assuming that max cell per dim are 5
  def getCellID(point: List[Double]) ={
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
  def getMinMaxCount(point: List[Double], countsPerCell: Map[List[Int], Int], dimensions: Int): (Long, Long) ={

    // MIN
    // Add 1 to all dims to take the cell that is definitely dominated by the point and then find all the cells outwards that
    var countsForCoordinates_min: List[Int] = List()
    var outwardCoordinates_min: List[List[Int]] = List()
    val starting_cell_min: List[Int] = point.map(elem => ((BigDecimal(elem) / BigDecimal("0.2")).toInt + 1))

    if (!starting_cell_min.exists(elem => elem > 4)) {
      outwardCoordinates_min = findCellsGreaterOrEqual(starting_cell_min, 4, dimensions)
      // Extract counts for each list in outwardCoordinates
      countsForCoordinates_min = outwardCoordinates_min.map { coordinates =>
        countsPerCell.getOrElse(coordinates, 0)
      }
    }

    // MAX
    val starting_cell_max: List[Int] = point.map( elem => (BigDecimal(elem) / BigDecimal("0.2")).toInt )
    val outwardCoordinates_max = findCellsGreaterOrEqual(starting_cell_max, 4, dimensions)
    val countsForCoordinates_max: List[Int] = outwardCoordinates_max.map { coordinates =>
      countsPerCell.getOrElse(coordinates, 0)
    }

    (countsForCoordinates_min.sum.toLong, countsForCoordinates_max.sum.toLong)
  }


  // Return true if point_B is dominated by point_A
  def isDominatedByPoint(point_A: List[Double], point_B: List[Double]): Boolean = {
    point_A.zip(point_B).forall(pair => pair._1 <= pair._2)
  }

  // Compare a given point with the points of the given block_id
  def countDominanceInCells(point: List[Double], pointsToCheck: RDD[(List[Double], List[Int])]): Long = {
    val pointsDominated =
      pointsToCheck
        //        .flatMap(_._2)
        .filter(pair => !pair._1.equals(point)) // exclude the point we are checking
        .filter(pair => isDominatedByPoint(point, pair._1))
        .count()
        .toLong

    pointsDominated
  }

  // Get the total dominance score of a given point
  def getTotalCount(point: List[Double], minCount: Long , pointsWithCells: RDD[(List[Double], List[Int])]): Long ={
    var sum = minCount
    sum = sum + countDominanceInCells(point, pointsWithCells)
    sum
  }

  // Get the total dominance score of a given point
  def findNeighbouringCells(point: List[Double], CountsPerCell: Map[List[Int], Int], dimensions: Int): List[List[Int]] ={

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

  def topKGridDominance(data: RDD[List[Double]],dimensions: Int ,top: Int, sc: SparkContext): Array[(List[Double], Long)] = {

    // Create an RDD of the data points along with the BLock ID RDD: (point,BlockID)
    val pointsWithCellID =
      data
        .map(point => (point, getCellID(point)))

    val countsPerCell = data
      .map(point => (getCellID(point), 1))
      .aggregateByKey(0)(_ + _, _ + _)
      .collect()
      .toMap

    //find skyline points
    val skylines = sc.parallelize(Task1.ALS(data).toList)

    val pointsWithMinMax =
      skylines
        .map { point =>
          val (minCount, maxCount) = getMinMaxCount(point, countsPerCell, dimensions)
          (point, minCount, maxCount)
        }
        .sortBy(_._3, ascending= false)

    val minCountOfFirstElement: Long = pointsWithMinMax.first._2

    val candidatePoints =
      pointsWithMinMax
        .filter(  _._3 >=  minCountOfFirstElement)    // I assume that Always gives an RDD greater or equal than top-k and that it fits to the memory
        .collect()
        .toList

    val top_k =
      candidatePoints
        .map( triplet => (triplet, findNeighbouringCells(triplet._1, countsPerCell, dimensions))) // point, minCount, maxCount, Neighbouring Cells to check
        .map( triplet => (triplet._1, getTotalCount(triplet._1._1, triplet._1._2, pointsWithCellID.filter(pair => triplet._2.contains(pair._2) ))))
        .map( triplet => (triplet._1._1, triplet._2)) // Keep only point and score

    top_k.sortBy(_._2)(Ordering[Long].reverse)
      .take(top)
      .toArray
  }

}