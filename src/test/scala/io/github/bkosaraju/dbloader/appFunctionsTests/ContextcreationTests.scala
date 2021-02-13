/*
 * Copyright (c) 2021. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

package io.github.bkosaraju.dbloader.appFunctionsTests

import io.github.bkosaraju.dbloader.AppInterface

trait ContextcreationTests extends AppInterface {

  val t1SQL = """select '2018/12/14 12:10:21' as src_timestamp, '15/04/2018' as src_date"""
  test("Context : Context Creation for SQL") {
    val sDF = context.sql(t1SQL)
    assert(sDF.count == 1)
  }
}
