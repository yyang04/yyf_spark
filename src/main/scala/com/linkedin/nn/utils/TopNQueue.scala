/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.nn.utils

import com.linkedin.nn.Types.{ItemId, ItemIdDistancePair}

import scala.collection.mutable

/**
  * This is a simple wrapper around the scala [[mutable.PriorityQueue]] that allows it to only hold a fixed number of
  * elements. By default, [[mutable.PriorityQueue]] behaves as a max-priority queue i.e as a max heap. [[TopNQueue]]
  * can be used to get smallest-n elements in a streaming fashion.
  *
  * We also deduplicate the contents based on the first value of the tuple ([[ItemId]] id).
  *
  * @param maxCapacity max number of elements the queue will hold
  */
class TopNQueue(maxCapacity: Int) extends Serializable {

  val priorityQ: mutable.PriorityQueue[ItemIdDistancePair] = mutable.PriorityQueue[ItemIdDistancePair]()(Ordering.by[ItemIdDistancePair, Double](_._2))
  val elements: mutable.HashSet[ItemId] = mutable.HashSet[ItemId]() // for deduplication

  /**
    * Enqueue elements in the queue
    * @param elems The elements to enqueue
    */
  def enqueue(elems: ItemIdDistancePair*): Unit = {  // 可能不止一个(Long, Double)
    elems.foreach { x =>
      if (!elements.contains(x._1)) {                // 如果不存在这个Long
        if (priorityQ.size < maxCapacity) {          // 并且队列长度小于maxCapacity
          priorityQ.enqueue(x)                       // 入队
          elements.add(x._1)                         // 添加到哈希中
        } else {
          if (priorityQ.head._2 > x._2) {            //
            elements.remove(priorityQ.dequeue()._1)  // dequeue() 把最大元素除去
            priorityQ.enqueue(x)
            elements.add(x._1)
          }
        }
      }
    }
  }

  def nonEmpty(): Boolean = priorityQ.nonEmpty

  def iterator(): Iterator[ItemIdDistancePair] = priorityQ.reverseIterator
}
