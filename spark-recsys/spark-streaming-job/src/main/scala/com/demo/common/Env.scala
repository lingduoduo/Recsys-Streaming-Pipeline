package com.demo.common

object Env {
  def argOrEnv(args: Array[String], index: Int, envKey: String): Option[String] =
    args.lift(index).orElse(sys.env.get(envKey))

  def requiredArgOrEnv(args: Array[String], index: Int, envKey: String, description: String): String =
    argOrEnv(args, index, envKey).getOrElse {
      throw new IllegalArgumentException(
        s"Missing $description. Pass it as argument ${index + 1} or set $envKey."
      )
    }

  def int(envKey: String, default: Int): Int =
    sys.env.get(envKey).flatMap(toIntOption).getOrElse(default)

  def double(envKey: String, default: Double): Double =
    sys.env.get(envKey).flatMap(toDoubleOption).getOrElse(default)

  def boolean(envKey: String, default: Boolean): Boolean =
    sys.env.get(envKey).map(_.trim.toLowerCase) match {
      case Some("true")  => true
      case Some("false") => false
      case Some(other) =>
        System.err.println(s"Unrecognised boolean value for $envKey: '$other', using default=$default")
        default
      case None => default
    }

  private def toIntOption(value: String): Option[Int] =
    try Some(value.toInt)
    catch { case _: NumberFormatException => None }

  private def toDoubleOption(value: String): Option[Double] =
    try Some(value.toDouble)
    catch { case _: NumberFormatException => None }
}
