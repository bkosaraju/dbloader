/*
 * Copyright (c) 2021. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */
package io.github.bkosaraju.dbloader

import io.github.bkosaraju.dbloader.functions.Exceptions
import io.github.bkosaraju.utils.common.ConfigMaker
import io.github.bkosaraju.utils.snowflake.ReadSFTable
import io.github.bkosaraju.utils.spark._
import io.github.bkosaraju.dbloader.functions.{DataTypeConverter, Exceptions}

import java.util.concurrent.ForkJoinPool
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ForkJoinTaskSupport

class IngestData extends ReadSFTable with DataFrameWriter with DataframeReader with ConfigMaker with DataTypeConverter with Exceptions {
  /**
   * Entry method to Ingest the data into target table from source View/table/function/script
   * This method is a higher order method which invoke Ingestion functions Such as decrypting the encrypted User credentials and calling the loader functions etc..
   *
   * @param srcConfig - configuration to load into Local/cloud/HDFS storage.
   * @return Unit
   */
  var edwsCols: String = _

  def ingestData(srcConfig: Map[String, String]): Unit = {
    try {
      var listedDataSets: collection.mutable.ArrayBuffer[String] = ArrayBuffer[String]()
      var origTargetTable: String = ""

      val dwConfig = {
        collection.mutable.Map[String, String]() ++
          srcConfig ++
          srcConfig.getOrElse("readerOptions", "").replaceAll("\"", "").stringToMap ++
          srcConfig.getOrElse("dwsVars", "").stringToMap
      }

      if (dwConfig.contains("extraDWSColumns")) {
        edwsCols = dwConfig.getOrElse("extraDWSColumns", "")
        dwConfig.keySet.toList.foreach(r => edwsCols = edwsCols.replaceAll(s"#${r}#", dwConfig.getOrElse(r, "")))
        dwConfig.put("dwsVars",
          Array(dwConfig.getOrElse("dwsVars", ""), edwsCols).filter(_.nonEmpty).mkString(",")
        )
      }

      dwConfig.keySet.foreach(param => {
        var configValue = dwConfig(param)
        dwConfig.keys.foreach(r => configValue = configValue.replaceAll(s"#${r}#", dwConfig(r)))
        dwConfig.put(param, configValue)
      })
      if (dwConfig.contains("extractFilter") && dwConfig.getOrElse("filterClause", "").isEmpty) {
        dwConfig.put("filterClause", dwConfig("extractFilter"))
      }

      if (dwConfig.getOrElse("sourceDataSetList", "").nonEmpty) {
        listedDataSets ++= dwConfig("sourceDataSetList").split(",")
      } else {
        listedDataSets += dwConfig.getOrElse("dataSet", "")
      }
      if (dwConfig.contains("targetTable")) {
        origTargetTable = dwConfig("targetTable")
      }
      //val localConfig = dwConfig
      dataExtract(listedDataSets, dwConfig, dwConfig, origTargetTable)

    } catch {
      case e: Exception =>
        logger.error("Unable to Load Data from Source")
        throw e
    }
  }


  def dataExtract(dataSetList: collection.mutable.ArrayBuffer[String],
                  config: collection.mutable.Map[String, String],
                  localConfig: collection.mutable.Map[String, String],
                  origTargetTable: String): Unit = {

    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(config.getOrElse("extractionParallelism", "1").toInt))
    var extractedDatasets: collection.mutable.ArrayBuffer[String] = ArrayBuffer[String]()
    var errorDatasets: collection.mutable.Map[String, String] = collection.mutable.Map()
    val parallel = dataSetList.par
    parallel.tasksupport = taskSupport
    parallel.map(dataset => {
      try {
        logger.info(s"Preparing to loadData data from: ${dataset}")

        if (origTargetTable.nonEmpty) {
          localConfig.put("targetTable", origTargetTable)
        } else {
          localConfig.put("targetTable", dataset)
        }
        if (localConfig.contains("dataSourceURI")) {
          localConfig.put("path",
            localConfig("dataSourceURI").replaceAll("/$", "")
              + "/" +
              dataset.replaceAll("^/", "").replaceAll("[sS]3://", "s3a://")
          )
        }
        val optionsMap = localConfig.toMap
        val srcDF = dataframeReader(optionsMap.generateSourceConfig)
        val sparkUtils = new SparkUtils
        val targetDF =
          if (optionsMap.getOrElse("writeMode", "append").equalsIgnoreCase("delta")) {
            if (srcDF.columns.map(_.toLowerCase).contains(config.getOrElse("hashedColumn", "row_hash").toLowerCase())) {
              logger.info(s"delta hashed column is already present in source data hence using the same(${optionsMap.getOrElse("hashedColumn", "row_hash").toLowerCase()})..")
              sparkUtils.getDeltaDF(srcDF, sparkUtils.targetHashSelector(optionsMap.generateTargetConfig), optionsMap.generateSourceConfig)
            } else {
              sparkUtils.getDeltaDF(
                srcDF.genHash(
                  config.getOrElse("hashedKeyColumns", srcDF.columns.mkString(",")).split(","),
                  config.getOrElse("hashedColumn", "row_hash"))
                , sparkUtils.targetHashSelector(optionsMap.generateTargetConfig), optionsMap)
            }
          } else {
            srcDF
          }
        dataframeWriter(optionsMap.generateTargetConfig, targetDF.amendDType(optionsMap.getOrElse("tgtDataTypeMap","").stringToMap))
        extractedDatasets.append(dataset)
        logger.info(s"Successfully extracted data from: ${dataset}")
      } catch {
        case e: Exception => {
          errorDatasets.put(dataset, e.getMessage)
          logger.error(s"Unable to extract and copy data from : ${dataset}")
        }
      }
    }
    )

    if (extractedDatasets.nonEmpty) {
      logger.error("*****************************************\n\n")
      logger.error("Extracted table/dataset List\n\n")
      logger.error("*****************************************\n\n")
      extractedDatasets.foreach(x => logger.info(x))
    }
    if (errorDatasets.nonEmpty) {
      logger.error("*****************************************\n\n")
      logger.error("Unable extract data from following tables\n\n")
      logger.error("*****************************************\n\n")
      logger.error("Table Name".formatted(s"%40s") + " : " + "Reason".formatted(s"%110s"))
      collection.mutable.LinkedHashMap(errorDatasets.toSeq.sortBy(_._1): _*)
        .foreach { x =>
          logger.error(x._1.formatted(s"%40s") + " : " + x._2.formatted(s"%110s"))
        }
      throw new IncompleteExtraction("Not all the tables/datasets are extracted!!")
    }
  }

}

object IngestData {
  def apply(config: Map[String, String]): Unit = (new IngestData).ingestData(config)
}
