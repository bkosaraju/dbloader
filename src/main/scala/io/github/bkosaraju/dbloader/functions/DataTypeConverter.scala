/*
 * Copyright (c) 2021. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

package io.github.bkosaraju.dbloader.functions

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructType

trait DataTypeConverter {

  implicit class DataTypeConverter(stgDF: DataFrame) {

    def amendDType(tgtDataTypeMap: Map[String, String]): DataFrame = {
        if (tgtDataTypeMap.keys.isEmpty) stgDF else {
          tgtDataTypeMap.keys.foldLeft(stgDF)((stgDF, col) => if (stgDF.schema.map(_.name.toLowerCase).contains(col.toLowerCase)) {
            stgDF.withColumn(col, lit(stgDF(col)).cast(tgtDataTypeMap(col)))
          }
          else stgDF
          )
        }
    }
  }

}