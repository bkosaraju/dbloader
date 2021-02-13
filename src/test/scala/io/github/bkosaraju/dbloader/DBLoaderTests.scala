/*
 * Copyright (c) 2021. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

package io.github.bkosaraju.dbloader

import java.io.{FileReader, StringReader}
import java.sql.{Connection, ResultSet}
import java.util.Properties

import scala.collection.JavaConverters._
import io.github.bkosaraju.utils.common.CommonUtils
import io.github.bkosaraju.utils.spark.{DataHashGenerator, SparkUtils}
import org.apache.spark.sql.SaveMode
import org.h2.jdbcx.JdbcDataSource
import org.h2.tools.RunScript
import org.scalatest.FunSuite
import io.github.bkosaraju.utils.common.LoadProperties

trait DBLoaderTests extends FunSuite with AppInterface  with LoadProperties  with DataHashGenerator {

  val sparkUtils = new SparkUtils
  val h2Props: Properties = "src/test/resources/H2.properties".loadParms()
  val jdbcURL = "jdbc:h2:mem:sparkTests;IGNORE_UNKNOWN_SETTINGS=TRUE;database_to_upper=false"
  val ds = new JdbcDataSource()
  ds.setURL(jdbcURL)
  ds.setPassword(h2Props.getProperty("password", "pwd"))
  ds.setUser(h2Props.getProperty("user", "sa"))
  val connection: Connection = ds.getConnection
  //val connection = DriverManager.getConnection("jdbc:h2:mem:test;LOCK_MODE=3;TRACE_LEVEL_FILE=3",h2Props)
  RunScript.execute(connection, new FileReader("src/test/resources/sparkTests.DDL"))
  RunScript.execute(connection, new FileReader("src/test/resources/sparkTests.DML"))
  connection.commit()
  val res: ResultSet = connection.createStatement().executeQuery("select count(1) from sparkTests.employees")
  test("dbloader: Test H2 database connectivity with Initialized table") {
    assertResult(9999) {
      var re: BigDecimal = 0
      while (res.next()) {
        re = res.getBigDecimal(1)
      }
      re
    }
  }

  val sDF = context.read.option("header", "true").csv("src/test/resources/rand_data.csv")
    .genHash(Seq("src_timestamp", "src_date", "keycol", "valuecol"), "row_hash")

  test("dbloader: Write data into RDBMS to for test preparation ..") {
    assertResult(true) {
      val opts = h2Props.asInstanceOf[java.util.Map[String, String]].asScala.toMap ++ Map("url" -> jdbcURL, "dbtable" -> "sparkTests.sourceTable_employee")
      sDF.write.mode(SaveMode.Overwrite).format("jdbc")
        .options(opts).save()
      true
    }
  }

  private val fulltableload = Array("src/test/resources/fulltableload.properties", "")
  test("dbloader: load delta data into target - table to table load") {
    RunScript.execute(connection, new StringReader("drop table if exists sparkTests.fulltableLoad;"))
    RunScript.execute(connection, new StringReader("create table sparkTests.fulltableLoad as select * from sparkTests.sourceTable_employee where 1=0;"))
    assertResult(500) {
      Launcher.main(fulltableload)
      val opts = h2Props.asInstanceOf[java.util.Map[String, String]].asScala.toMap ++ Map("url" -> jdbcURL, "dbtable" -> "sparkTests.fulltableLoad", "driver" -> "org.h2.Driver")
      sparkSession.read.format("jdbc")
        .options(opts).load().count()
    }
  }


  private val filteredData = Array("src/test/resources/tableloadwithextractfilter.properties", "empnoQualifier=18000")
  test("dbloader: load delta data into target - table to table load with extractFilter function") {
    RunScript.execute(connection, new StringReader("drop table if exists sparkTests.fulltableLoad_filter;"))
    RunScript.execute(connection, new StringReader("create table sparkTests.fulltableLoad_filter as select * from sparkTests.employees where 1=0;"))
    assertResult(1999) {
      DBLoader.dbloader(filteredData)
      val opts = h2Props.asInstanceOf[java.util.Map[String, String]].asScala.toMap ++ Map("url" -> jdbcURL, "dbtable" -> "sparkTests.fulltableLoad_filter", "driver" -> "org.h2.Driver")
      sparkSession.read.format("jdbc")
        .options(opts).load().count()
    }
  }

  private val sqlSelectionLoad = Array("src/test/resources/sqlfileload.properties", "")
  test("dbloader: load delta data into target - sqlFile to table load") {
    RunScript.execute(connection, new StringReader("drop table if exists sparkTests.fulltableLoad_fromSqlFile;"))
    RunScript.execute(connection, new StringReader("create table sparkTests.fulltableLoad_fromSqlFile as select * from sparkTests.employees where 1=0;"))
    assertResult(5000) {
      DBLoader.dbloader(sqlSelectionLoad)
      val opts = h2Props.asInstanceOf[java.util.Map[String, String]].asScala.toMap ++ Map("url" -> jdbcURL, "dbtable" -> "sparkTests.fulltableLoad_fromSqlFile", "driver" -> "org.h2.Driver")
      sparkSession.read.format("jdbc")
        .options(opts).load().count()
    }
  }

  private val sqlSelectionLoadwithDelta = Array("src/test/resources/sqlfileload_withdelta.properties", "")
  test("dbloader: load delta data into target - sqlFile to table with delta mode") {
    RunScript.execute(connection, new StringReader("drop table if exists sparkTests.fulltableLoad_fromSqlFile_withDelta;"))
    RunScript.execute(connection, new StringReader("create table sparkTests.fulltableLoad_fromSqlFile_withDelta as select * from sparkTests.employees where 1=0;"))
    assertResult(5000) {
      DBLoader.dbloader(sqlSelectionLoadwithDelta)
      val opts = h2Props.asInstanceOf[java.util.Map[String, String]].asScala.toMap ++ Map("url" -> jdbcURL, "dbtable" -> "sparkTests.fulltableLoad_fromSqlFile_withDelta", "driver" -> "org.h2.Driver")
      sparkSession.read.format("jdbc")
        .options(opts).load().count()
    }
  }

  test("dbloader: load delta data into target - delta mode for updates - cleanup") {
    RunScript.execute(connection, new StringReader("update  sparkTests.fulltableLoad_fromSqlFile_withDelta set emp_no=999999 where emp_no=19999"))
    RunScript.execute(connection, new StringReader("update  sparkTests.fulltableLoad_fromSqlFile_withDelta set emp_no=999998 where emp_no=19998"))
    assertResult(0) {
      val opts = h2Props.asInstanceOf[java.util.Map[String, String]].asScala.toMap ++ Map("url" -> jdbcURL, "dbtable" -> "sparkTests.fulltableLoad_fromSqlFile_withDelta", "driver" -> "org.h2.Driver")
      sparkSession.read.format("jdbc")
        .options(opts).load().filter("emp_no in (19999,19998)").count()
    }
  }

  private val sqlSelectionLoadwithDelta_Delta = Array("src/test/resources/sqlfileload_withdelta.properties", "")
  test("dbloader: load delta data into target - delta mode for updates - change only insert") {
    assertResult(5002) {
      DBLoader.dbloader(sqlSelectionLoadwithDelta_Delta)
      val opts = h2Props.asInstanceOf[java.util.Map[String, String]].asScala.toMap ++ Map("url" -> jdbcURL, "dbtable" -> "sparkTests.fulltableLoad_fromSqlFile_withDelta", "driver" -> "org.h2.Driver")
      sparkSession.read.format("jdbc")
        .options(opts).load().count()
    }
  }

  test("dbloader: Raise an exception in case if required arugments not passed") {
    intercept[Exception] {
      DBLoader.dbloader(Array())
    }
  }

  test("dbloader: Raise an exception in case if there is any issue with processing..") {
    intercept[Exception] {
      DBLoader.dbloader(Array(""))
    }
  }

  private val readExcetpionProps  = Array("src/test/resources/exceptiontest.properties","")
  test("dbloader: Raise an exception in case if there is any issue with processing(handle at Launcher).. ") {
    intercept[Exception]{
      Launcher.main(readExcetpionProps)
    }
  }
}
