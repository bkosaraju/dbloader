/*
 * Copyright (c) 2021. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

package io.github.bkosaraju.dbloader

object Launcher extends AppFunctions {
  /**
  Entrypoint for Main Application
  @param args Input arguments for application to run primaryly the config file
  @return Unit
   */
  def main(args: Array[String]): Unit = {
    try {
      DBLoader.dbloader(args)
      if (sparkSession.conf.getAll.getOrElse("spark.master", "").contains("k8s:")) {
        sparkSession.stop()
      }
    } catch {
      case e: Exception => {
        logger.error("Exception occurred wile running Application..")
        if (sparkSession.conf.getAll.getOrElse("spark.master", "").contains("k8s:")) {
          sparkSession.stop()
        }
        throw e
      }
    }
  }
}
