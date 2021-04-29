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
object VisitType{
  //const
  val erroneousStr = "Erroneous"
  val shortStayStr = "Short Stay"
  val standardStr = "Standard Stay"
  val standardExStr = "Standard Extended Stay"
  val longStayStr = "Long Stay"

  /**
   * Defines the visit type based on stay duration
   */
  def defineVisitType(stayDuration: Int): String ={
    if (stayDuration > 30 || stayDuration < 0)
      erroneousStr
    else if (stayDuration == 1)
      shortStayStr
    else if (stayDuration <= 7 && stayDuration >= 2)
      standardStr
    else if (stayDuration <= 14 && stayDuration >= 8)
      standardExStr
    else
      longStayStr
  }
}
