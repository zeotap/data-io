package com.zeotap.data.io.common.constructs

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF

object SupportedFeatureOps {

  def featuresCompiler[A](features: Seq[SupportedFeaturesF[A]]): SupportedFeaturesF[A] =
    features.reduce((a, b) => for {
      _ <- a
      v2 <- b
    } yield v2)

}
