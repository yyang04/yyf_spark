/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.nn.params

import org.apache.spark.ml.param.{BooleanParam, IntParam, ParamValidators, Params}

trait LSHNNSParams extends Params {

  private[nn] val numHashes:  IntParam =
    new IntParam(this, "numHashes", "Number of hashes to use for estimation", ParamValidators.gt(0))
  final def getNumHashes: Int = $(numHashes)
  setDefault(numHashes -> 256)

  private[nn] val signatureLength: IntParam =
    new IntParam(
      this,
      "signatureLength",
      "Length of signature, also referred to as band size in literature",
      ParamValidators.gt(0))
  final def getSignatureLength: Int = $(signatureLength)
  setDefault(signatureLength -> 16)


  private[nn] val joinParallelism: IntParam = new IntParam(this, "joinParallelism",
    "Paralellism of the join for nearest neighbor search", ParamValidators.gt(0))
  final def getJoinParallelism: Int = $(joinParallelism)
  setDefault(joinParallelism -> 500)


  private[nn] val bucketLimit: IntParam =
    new IntParam(
      this,
      "bucketLimit",
      "To prevent a single bucket from getting too big, set bucketLimit to be the upper limit on the bucket size. " +
        "If the number of items mapped to this bucket are more than the set limit, we use reservoir sampling to select " +
        "the ones that get preserved while the rest get discarded",
      ParamValidators.gt(0))
  final def getBucketLimit: Int = $(bucketLimit)
  setDefault(bucketLimit -> 1000)

  private[nn] val shouldSampleBuckets: BooleanParam =
    new BooleanParam(
      this,
      "shouldSampleBuckets",
      "To prevent a single bucket from getting too big, we set a limit on how many elements we consider from a given " +
        "bucket. This parameter controls how we reach this limit. If set to false, we simply fill the bucket till it " +
        "reaches the limit and ignore all the remaining items that fall in the bucket. If set to true, we look at all " +
        "the elements that fall in the bucket and take a uniform sample from it to populate the bucket using reservoir " +
        "sampling. ")
  final def getShouldSampleBuckets: Boolean = $(shouldSampleBuckets)
  setDefault(shouldSampleBuckets -> false)


  private[nn] val numOutputPartitions: IntParam =
    new IntParam(this, "numOutputPartitions", "Number of partitions in the output", ParamValidators.gt(0))
  final def getNumOutputPartitions: Int = $(numOutputPartitions)
  setDefault(numOutputPartitions -> 100)
}
