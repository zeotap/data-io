package com.zeotap.data.io.common.utils

import org.apache.log4j.Logger

object logger extends Serializable {
  @transient lazy val log: Logger = Logger.getLogger(this.getClass.getName)
}
