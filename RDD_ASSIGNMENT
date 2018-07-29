package com.sparkrdd.assignment

import org.apache.spark.sql.SparkSession
import scala.util.matching.Regex

object RDD_ASSIGNMENT {
  def main(args: Array[String]): Unit = {

    //Create spark object
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark RDD Example")
      .getOrCreate()

    //Task 1.1
    val newdataset = spark.sparkContext.textFile("C:\\Users\\Shruthi\\Desktop\\19_Dataset.txt")
    val noOfLines = newdataset.count()
    println("no. Of lines present in the file: " + noOfLines)

    //Task 1.2
    val splittedRDDs = newdataset.flatMap(f=>f.split(','))
    def isWord(a:String):Boolean={
      val numberPattern: Regex = "^[0-9]*$".r
      numberPattern.findFirstMatchIn(a) match {
        case  Some(_) =>
          return false
        case None =>
          return true
      }
    }
    val wordOnly = splittedRDDs.filter(f=>isWord(f))
    wordOnly.foreach(x=>println(x))
    val countOfWords = wordOnly.count()
    println("no. Of words present in the document: " + countOfWords)

    //Task 1.3
    val splittedRDDs2 = newdataset.flatMap(f=>f.split('-'))
    val AfterSplitting = splittedRDDs2.map(x=>(x,1)).reduceByKey(_+_)
    AfterSplitting.foreach(x=>println(x))
    val countAfterSplitting = AfterSplitting.count()
    println("no. Of words present in the document after separating with -: " + countAfterSplitting)

    //Task 2
    //Problem Statement 1:
    //1. Read the text file, and create a tupled rdd.
    val tupledRDD = newdataset.map(x=>x.split(","))
    //Printing tupled RDD
    tupledRDD.collect().foreach(row => println(row.mkString(",")))

    //2. Find the count of total number of rows present.
    val countResult = tupledRDD.count()
    println("no. Of rows present in tupled RDD -: " + countResult)

    //3. What is the distinct number of subjects present in the entire school
    val subjectOnly = tupledRDD.map(item=>item(1))
    //get distinct subjects
    val distinctSubjects = subjectOnly.distinct()
    distinctSubjects.foreach(x=>println(x))

    //4. What is the count of the number of students in the school, whose name is Mathew and marks is 55
    def getStudent(name:String,marks:Int) : Boolean ={
      if (name.equalsIgnoreCase("Mathew") && marks==55)
        return true
      else
        return false
    }

    val students = tupledRDD.filter(x=>getStudent(x(0),x(3).toInt))
    students.collect().foreach(row => println(row.mkString(",")))
    val countStudents = students.count()
    println("Number of students in the school, whose name is Mathew and marks is 55 -: " + countStudents)

    //Problem Statement 2:
    //1. What is the count of students per grade in the school?
    val stdsWithGrades = tupledRDD.map(x=>(x(2),1))
    val group = stdsWithGrades.countByKey()
    group.foreach(x=>println("Students in Grade:"+x))

    //Variation : Getting the distinct student counts per grade depending upon his name
    val distinctStds = tupledRDD.map(x=>(x(2),x(0)))
    val group1 = distinctStds.distinct.countByKey()
    group1.foreach(x=>println(x))

    //2. Find the average of each student (Note - Mathew is grade-1, is different from Mathew in
    //some other grade!)
    val students1 = tupledRDD.map(x=>((x(2),x(0)),x(3).toInt))
    val sum1 = students1.distinct.groupByKey().mapValues(x=>x.sum)
    sum1.foreach(x=>println(x))
    val students2 = tupledRDD.map(x=>((x(2),x(0)),1))
    //Sum up the counter to get the subjects count
    val count1 = students2.groupByKey().mapValues(x=>x.sum)
    count1.foreach(x=>println(x))
    //we have to join these two RDDs to get the Sum and count in one RDD so that we can get the Average of each student
    val joinedRDD = sum1.join(count1)
    joinedRDD.foreach(x=>println(x))
    //calculate average
    val averageOfEachStd = joinedRDD.map(x=>( (x._1),(x._2._1/x._2._2)))
    averageOfEachStd.foreach(x=>println("Average Of Student: " + x))

    //3. What is the average score of students in each subject across all grades?
    val subjectsMarks = tupledRDD.map(x=>(x(1),x(3).toInt))
    val sum3 = subjectsMarks.groupByKey().mapValues(x=>x.sum)
    sum3.foreach(x=>println(x))
    val noOfStudents = tupledRDD.map(x=>(x(1),1)).groupByKey().mapValues(x=>x.sum)
    noOfStudents.foreach(x=>println(x))
    //join these RDD and calculate the average
    val avg2 = sum3.join(noOfStudents).map(x=>( x._1 ,(x._2._1/x._2._2)))
    avg2.foreach(x=>println("Average for Subject:"+ x._1+"==>"+x._2))


    //4. What is the average score of students in each subject per grade?
    val subMarksPerGrade =  tupledRDD.map(x=>((x(1),x(2)),x(3).toInt))
    val sum4 = subMarksPerGrade.groupByKey().mapValues(x=>x.sum)
    sum4.foreach(x=>println(x))
    //Calculate count of students
    val noOfStudents1 = tupledRDD.map(x=>((x(1),x(2)),1)).groupByKey().mapValues(x=>x.sum)
    noOfStudents1.foreach(x=>println(x))
    //join these RDD and calculate the average
    val avg3 = sum4.join(noOfStudents1).map(x=>(x._1 ,(x._2._1/x._2._2)))
    avg3.foreach(x=>println("Average for :"+ x._1+"==>"+x._2))

    //5. For all students in grade-2, how many have average score greater than 50?
    val grade2Students = averageOfEachStd.filter(x=>x._1._1.equalsIgnoreCase("grade-2") && x._2>=50)
    grade2Students.foreach(x=>println(x))


    //Problem Statement 3:
    //1. Average score per student_name across all grades is same as average score per student_name per grade
    //Hint - Use Intersection Property
    val sumPerStdName = tupledRDD.map(x=>(x(0),x(3).toInt)).groupByKey().mapValues(y=>y.sum)
    sumPerStdName.foreach(x=>println(x))
    val countOfStdName = tupledRDD.map(x=>(x(0),1)).groupByKey().mapValues(y=>y.sum)
    countOfStdName.foreach(x=>println(x))
    //join these two RDDs to get average per student_name
    val avgStdName = sumPerStdName.join(countOfStdName).map(x=>(x._1,x._2._1/x._2._2))
    //The RDD containing average for student_name per grade is averageOfEachStd now intersect this RDD with avgStdName
    val avgPerGrade  = averageOfEachStd.map(x=>(x._1._2,x._2))
    avgStdName.foreach( x=>println("Average per student_Name"+x))
    avgPerGrade.foreach( x=>println("Average per student_Name & Grade"+x))
    val commonStds = avgStdName.intersection(avgPerGrade)
    commonStds.foreach(x=>println("Students having same average per name and per grade"+x))
  }
}
