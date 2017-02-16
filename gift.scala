package com.project_team.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source

object CallCampaign {

  def call_campaign(): Map[Int, String] = {

    var campaign: Map[Int, String] = Map()
    //campaign.txt is the file that contains the campaign offers
    val lines = Source.fromFile("../campaign.txt").getLines()
    for (line <- lines) {
      var fields = line.split(",")
      if (fields.length > 1) {

        //separate each record to two fields [offerID , corresponding gift]
        campaign += (fields(0).toInt -> fields(1))
      } // end of if
    } // end of for_loop

    return campaign
  } // end of call_campaign()

  def calls_record(line: String) = {
    val x = line.split(",")
    val user = x(0)
    val other = x(1)
    val direction = x(2)
    val duration = x(3)
    val data = x(4)
    (user, other, direction, duration, data)
  } // end of calls_record()

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "calls")
    val campaign_map = call_campaign()
    val file = sc.textFile("../calls.csv")
    //map calls file to (user, other, direction, duration, data)record
    val line = file.map(calls_record)

    // filter records to return only records with timestamp from  2010 to 2011
    val calls_10_11 = line.filter(x => x._5.contains("2010") || x._5.contains("2011"))

    // filter calls_10_11 records and return records with call duration =0 but its not missed 
    val not_zero_duration = calls_10_11.filter(x => x._3 == "Missed" || x._4.toInt != 0)

    // make key_value pair of user_ID and direction
    val direction = not_zero_duration.map(x => (x._1, x._3))

    val outgoing = direction.filter(x => x._2 == "Outgoing")
    val outgoing_value = outgoing.mapValues(x => (x, 1))
    // return every user and the count of his (Outgoing) calls 
    val outgoing_count = outgoing_value.reduceByKey((x, y) => (x._1, x._2 + y._2))

    //matching every id with its corresponding gift depending on count of outgoing calls matching with campaign file
    val gift = outgoing_count.mapValues(
      x => x._2 match {
        case i if 1 until 200 contains i     => (x._1, i, campaign_map(3))
        case i if 200 until 500 contains i   => (x._1, i, campaign_map(4))
        case i if 500 until 1000 contains i  => (x._1, i, campaign_map(5))
        case i if 1000 until 1500 contains i => (x._1, i, campaign_map(6))

      })
    val gift_action = gift.take(40)
    println("\n------------free minutes for special  user depending on call direction---------------- ")
    gift_action.foreach(println)

    println("\n------------------free call or free Msg denpending on call duration--------------------\n")

    // if the duration of the call less than 10 seconds we consider that the call cut off 
    //then we give the user a free call
    val gift2 = not_zero_duration.map(x => if (x._4 != "duration") {
      if (x._4.toInt < 10 & x._3 != "Missed") { (x, campaign_map(1)) }
      // if the user made a missed only we consider that he did'nt have credit 
      //then we give him free a message 
      else if (x._3 == "Missed") { (x, campaign_map(2)) }
      else (x)
    })
    val gift2_action = gift2.take(40) 
    gift2_action.foreach(println)

  } // end_of_main
} //end_of_CallCampaign


