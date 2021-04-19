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

  /**
   * Defines the visit type based on stay duration
   */
  def defineVisitType(stayDuration: Int): VisitType.Value ={
    if (stayDuration > 30 || stayDuration < 0)
      VisitType.erroneous
    else if (stayDuration == 1)
      VisitType.short_stay
    else if (stayDuration < 7 && stayDuration > 2)
      VisitType.standard_stay
    else if (stayDuration < 14 && stayDuration > 8)
      VisitType.standard_extended_stay
    else
      VisitType.long_stay
  }
}
