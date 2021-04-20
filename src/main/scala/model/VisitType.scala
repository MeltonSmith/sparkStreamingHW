package model

/**
 * Created by: Ian_Rakhmatullin
 * Date: 19.04.2021
 *
 * Visit types for hotels.
 *
 * Defined by stay duration.
 * These are:
 * Erroneous data": null, more than month(30 days), less than or equal to 0
 * "Short stay": 1 day stay
 * "Standard stay": 2-7 days
 * "Standard extended stay": 1-2 weeks
 * "Long stay": 2-4 weeks (less than month)
 */
object VisitType extends Enumeration {
  //const
  val erroneousStr = "Erroneous"
  val shortStayStr = "Short Stay"
  val standardStr = "Standard Stay"
  val standardExStr = "Standard Extended Stay"
  val longStr = "Long Stay"


  val erroneous: VisitType.Value = Value(erroneousStr)
  val short_stay: VisitType.Value = Value(shortStayStr)
  val standard_stay: VisitType.Value = Value(standardStr)
  val standard_extended_stay: VisitType.Value = Value(standardExStr)
  val long_stay: VisitType.Value = Value(longStr)

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
