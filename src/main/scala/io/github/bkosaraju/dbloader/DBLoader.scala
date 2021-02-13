/*
 * Copyright (c) 2021. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

package io.github.bkosaraju.dbloader

import java.io.{File, FileInputStream}
import java.nio.file.{Files, Path}
import java.util.{InputMismatchException, Properties}
import io.github.bkosaraju.utils.common.{CommonUtils, CryptoSuite, LoadProperties, SendMail}
import org.apache.commons.io.FileUtils
import scala.collection.JavaConverters._
import scala.util.Try

object DBLoader extends AppFunctions  with LoadProperties with CryptoSuite with SendMail {
  var content: String = _
  val templateFileLocation =  Files.createTempDirectory("")

  /**
   * Application start entry point which orchestrate application
   * @param args
   * @return Unit
   */
  def dbloader(args: Array[String]): Unit = {
    if (args.length < 1) {
      logger.error(
        "Required Arguments Not Provided as properties file needed for running application")
      throw new InputMismatchException("Required Arguments Not Provided as properties file needed for runnig application")
    }
    val props = args(0).loadParms()
    val mode = props.getProperty("mode")
    val appName = props.getProperty("appName")
    try {
      if (args.length > 1) {
        props.setProperty("dwsVars", args(1).toString)
      }
      if (props.containsKey("encryptedConfig")) {
        if (props.containsKey("encryptedConfigKey")) {
          decryptFile(props.getProperty("encryptedConfig"), templateFileLocation + "/encData", props.getProperty("encryptedConfigKey"))
          props.load(new FileInputStream(templateFileLocation + "/encData"))
        } else {
          logger.warn("cant get encryptionKey from configuration hence ignoring encrypted config[may case failure later point of time]")
        }
      }
      IngestData(props.asInstanceOf[java.util.Map[String, String]].asScala
        .map(x => x._1 -> x._2.replaceAll(""""""", "")).toMap
      )
    } catch {
      case e: Exception => {
        logger.error("Exception Occurred While Processing Data", e)
        if (props.getProperty("sendMailFlag", "").equalsIgnoreCase("true")) {
          sendMailFromError(e, props.asInstanceOf[java.util.Map[String, String]].asScala.toMap)
        }
        Try(FileUtils.deleteDirectory(new File(templateFileLocation.toString)))
        throw e
      }
    }
  }
}