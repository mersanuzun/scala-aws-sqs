import java.io.File

import com.typesafe.config.{Config, ConfigFactory}

import scala.util.control.NonFatal

/**
  * Created by mersanuzun on 1/24/17.
  */
object AwsConfig{
  val confFile: String = "conf/application.conf"
  val conf: Config = ConfigFactory.parseFile(new File(confFile))

  def getString(path: String): String = {
    conf.getString(path)
  }

  def getOptionString(path: String): Option[String] = {
    try{
      Some(getString(path))
    }catch {
      case NonFatal(_) => None
    }
  }
}
