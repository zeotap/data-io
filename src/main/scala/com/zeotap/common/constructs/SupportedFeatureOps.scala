package com.zeotap.common.constructs

import com.zeotap.common.types.SupportedFeaturesF.SupportedFeaturesF

object SupportedFeatureOps {

  def featuresCompiler[A](features: Seq[SupportedFeaturesF[A]]): SupportedFeaturesF[A] =
    features.reduce((a, b) => for {
      _ <- a
      v2 <- b
    } yield v2)

}
