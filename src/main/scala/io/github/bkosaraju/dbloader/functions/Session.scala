/*
 * Copyright (c) 2021. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

package io.github.bkosaraju.dbloader.functions

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import org.slf4j.Logger
import io.github.bkosaraju.utils.common.{Session => Ses}


trait Session extends Ses {
  /** Application logger
   * One place to define it and invoke every where.
   */
  override
  def logger: Logger = LoggerFactory.getLogger(getClass)
  /**session
    *here the spark session initiated with sql Support
    */
  val sparkSession : SparkSession = SparkSession
    .builder()
    .getOrCreate()
  val hadoopfs : FileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
}
