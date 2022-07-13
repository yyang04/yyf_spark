/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.nn.lsh

import org.apache.spark.ml.linalg.Vector

private[nn] abstract class HashFunction extends Serializable {

  /**
    * Compute the hash signature of the supplied vector
    */
  def compute(v: Vector): Array[Int]
}
