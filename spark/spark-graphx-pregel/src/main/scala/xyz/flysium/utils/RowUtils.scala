package xyz.flysium.utils

/*
 * MIT License
 *
 * Copyright (c) 2022 SvenAugustus
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

import org.apache.spark.sql.Row

/**
 * Row Utils
 *
 * @author zeno
 */
case object RowUtils extends scala.Serializable {

  def getRowAsLong(row: Row, fieldName: String): Long = {
    val value = row.getAs[Any](fieldName)
    value match {
      case l: Long =>
        l
      case i: Int =>
        i.longValue()
      case _ => (value.toString + "").toLong
    }
  }

  def getRowAsInt(row: Row, fieldName: String): Int = {
    val value = row.getAs[Any](fieldName)
    value match {
      case l: Long =>
        l.toInt
      case i: Int =>
        i
      case _ => (value.toString + "").toInt
    }
  }

  def getRowAsBigDecimal(row: Row, fieldName: String): BigDecimal = {
    val value = row.getAs[Any](fieldName)
    value match {
      case d: Double =>
        BigDecimal(d + "")
      case l: Long =>
        BigDecimal(l + "")
      case i: Int =>
        BigDecimal(i + "")
      case _ => BigDecimal(value.toString + "")
    }
  }

}
