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

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Graph Utils
 *
 * @author zeno
 */
case object GraphUtils extends scala.Serializable {

  /**
   * 移除独岛节点，返回新的 Graph
   */
  def removeSingletons[VD: ClassTag, ED: ClassTag](g: Graph[VD, ED]): Graph[VD, ED] = {
    Graph(g.triplets.map(et => (et.srcId, et.srcAttr))
      .union(g.triplets.map(et => (et.dstId, et.dstAttr)))
      .distinct, g.edges)
  }

}
