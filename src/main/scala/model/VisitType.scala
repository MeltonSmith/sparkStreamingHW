package model

/**
 * Created by: Ian_Rakhmatullin
 * Date: 19.04.2021
 */
object VisitType extends Enumeration {
  val erroneous: VisitType.Value = Value("Erroneous")
  val short_stay: VisitType.Value = Value("Short Stay")
  val standard_stay: VisitType.Value = Value("Standard Stay")
  val standard_extended_stay: VisitType.Value = Value("Standard Extended Stay")
  val long_stay: VisitType.Value = Value("Long Stay")
}
