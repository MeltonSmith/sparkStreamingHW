package model

/**
 * Created by: Ian_Rakhmatullin
 * Date: 19.04.2021
 *  Hotel state which will look like: hotel, with_children, batch_timestamp, erroneous_data_cnt, short_stay_cnt, standard_stay_cnt,
 *  standard_extended_stay_cnt, long_stay_cnt, most_popular_stay_type
 */
case class HotelState(hotel_id:Long, withChildren: Boolean,batch_timestamp:java.sql.Timestamp, var erroneous_data_cnt:Int,
                 var short_stay_cnt:Int, var standard_stay_cnt:Int, var standard_extended_stay_cnt:Int, var long_stay_cnt: Int,
                 var most_popular_stay_type:String) {

  /**
   * Updates state in the group(grouped by hotel_id) with the particular visitType => by incrementing an appropriate count by 1
   * Also finds a new visit type (if applicable) for the new maximum value
   */
  def updateState(visitType: String) : HotelState = {
    increment(visitType)

    //if the visit type in state does not equal the current visit type from iterator
    //we should check for the new maximum to update the most_popular_visit_type
    if (!this.most_popular_stay_type.equals(visitType)) {
      val cntWithMaxCurrentType = getCountByVisitType(most_popular_stay_type)
      val maxCandidate = getCountByVisitType(visitType)

      if (!cntWithMaxCurrentType.equals(cntWithMaxCurrentType.max(maxCandidate))) {
        //new visit type with maximum counts, and it should be updated
        this.most_popular_stay_type = visitType
      }
    }
    //else no need to to something as the visitType is the same as the previous one (for max)
    this
  }

  /**
   * increments an appropriate counter for a passed visit type
   */
  private def increment(visitType: String) = {
    visitType match {
      case VisitType.erroneousStr => this.erroneous_data_cnt = this.erroneous_data_cnt + 1
      case VisitType.shortStayStr => this.short_stay_cnt = this.short_stay_cnt + 1
      case VisitType.standardStr => this.standard_stay_cnt = this.standard_stay_cnt + 1
      case VisitType.standardExStr => this.standard_extended_stay_cnt = this.standard_extended_stay_cnt + 1
      case VisitType.longStayStr => long_stay_cnt = long_stay_cnt + 1
      case _ => throw new UnsupportedOperationException("Wrong visit type")
    }
  }

  /**
   * @return count for a passed visit type
   */
  private def getCountByVisitType(visitType: String): Int ={
    visitType match {
      case VisitType.erroneousStr => erroneous_data_cnt
      case VisitType.shortStayStr => short_stay_cnt
      case VisitType.standardStr => standard_stay_cnt
      case VisitType.standardExStr =>  standard_extended_stay_cnt
      case VisitType.longStayStr =>  long_stay_cnt
      case _ => throw new UnsupportedOperationException("Wrong visit type")
    }
  }
}
