package com.zeotap.data.io.common.utils

object Logger extends Serializable {
  @transient lazy val log: org.apache.log4j.Logger = org.apache.log4j.Logger.getLogger(this.getClass.getName)
}
