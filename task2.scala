package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import scala.math.sqrt
import scala.util.matching.Regex
import scala.reflect.reify.phases.Calculate
import org.apache.spark.sql.SparkSession

object testclass {
  
  def call_campaign() : Map[Int, String] = {
    
    var campaign:Map[Int, String] = Map()
    
     val lines = Source.fromFile("../campaign.txt").getLines()
     for (line <- lines) {
       var fields = line.split(",")
       if (fields.length > 1) {
       campaign += (fields(0).toInt -> fields(1))
       }
     }
    
     return campaign
  }
  
  
  
  def record1(line:String)={
    val x=line.split(",")
    val user=x(0)
    val other =x(1)
    val direction=x(2)
    val duration=x(3)
    val data=x(4)
     (user,other,direction,duration ,data)
  }
 
  
  
   def main(args: Array[String]): Unit = {
       Logger.getLogger("org").setLevel(Level.ERROR)
    val sc=new SparkContext("local[*]","calls")
 
    val file =sc.textFile("../calls.csv")
    
   /* hna ana 3awz a3rf kol user 3amel kam mokalma w 3la 7asb el mokalmata ely 3amalha 
    * bdelo da2y2 free 
    */
    val campagain_method =call_campaign()
    
    val line=file.map(record1)
     val collection =line.filter(x => x._5.contains("2010")||x._5.contains("2011")) 
   val filter2 =collection.filter(x => x._3=="Missed"|| x._4.toInt!=0)
   
  
   val  outgoing =filter2.map(x =>(x._1,x._3))
    
   val outgoing2 =outgoing.filter(x => x._2=="Outgoing")
    val outgoing3 =outgoing2.mapValues(x => (x,1))
   
    val outgoing4=outgoing3.reduceByKey((x,y)=> (x._1,x._2 +y._2))
      val outgoing5=outgoing4.mapValues(
        x => x._2 match{
          case it if 1 until 200 contains it  => (x._1,it,campagain_method(3))
          case it if 200 until 499 contains it  => (x._1,it,campagain_method(4))
          case it if 500 until 1000 contains it  => (x._1,it,campagain_method(5)) 
          case it if 1000 until 1500 contains it  =>( x._1,it,campagain_method(6))
             
        })
   val dataset2=filter2.take(30)
     println("------------free minutes for special  user---------------- ")
   dataset2.foreach(println)
     
   
   /* hna ana 3amel lw el mokalma a2al mn 10 da2y2 f hya keda faaslt w bdelo free call
    * lw el user 3aml missed bftard eno msh m3ah rased f badelo free msg
    */
   println("#########################################################")
    println("----------free call or free Msg---------------")
     println("#########################################################")
  val data2=filter2.map(x => if(x._4!="duration")
  {if(x._4.toInt < 10 & x._3!="Missed") {(x,campagain_method(1))} 
  else if(x._3=="Missed"){(x,campagain_method(2))}
   else (x)}  )
   val dataset =data2.collect()
  dataset.foreach(println)
 
   

  }
}

