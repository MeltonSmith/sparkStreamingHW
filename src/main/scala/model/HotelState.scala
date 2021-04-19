package model

/**
 * Created by: Ian_Rakhmatullin
 * Date: 19.04.2021
 *  (Result will look like: hotel, with_children, batch_timestamp, erroneous_data_cnt, short_stay_cnt, standard_stay_cnt,
 *  standard_extended_stay_cnt, long_stay_cnt, most_popular_stay_type
 */
case class HotelState(hotel_id:Long, batch_timestamp:java.sql.Timestamp, var erroneous_data_cnt:Int,
                 var short_stay_cnt:Int, var standard_stay_cnt:Int, var standard_extended_stay_cnt:Int, var long_stay_cnt: Int,
                 var most_popular_stay_type:VisitType.Value) {

  def updateState(visitType: VisitType.Value) : HotelState = {
    increment(visitType)

    if (!this.most_popular_stay_type.equals(visitType)) {
      val cntWithMaxCurrentType = getCountByVisitType(most_popular_stay_type)
      val maxCandidate = getCountByVisitType(visitType)

      if (!cntWithMaxCurrentType.equals(cntWithMaxCurrentType.max(maxCandidate))) {
        //new visit type with maximum counts
        this.most_popular_stay_type = visitType
      }
    }
    //else no need to to something as the visitType is the same as the previous one (for max)
    this
  }

  private def increment(visitType: VisitType.Value) = {
    visitType match {
      case VisitType.erroneous => this.erroneous_data_cnt = this.erroneous_data_cnt + 1
      case VisitType.short_stay => this.short_stay_cnt = this.short_stay_cnt + 1
      case VisitType.standard_stay => this.standard_stay_cnt = this.standard_stay_cnt + 1
      case VisitType.standard_extended_stay => this.standard_extended_stay_cnt = this.standard_extended_stay_cnt + 1
      case VisitType.long_stay => long_stay_cnt = long_stay_cnt + 1
      case _ => throw new UnsupportedOperationException("Wrong visit type")
    }
  }

  private def getCountByVisitType(visitType: VisitType.Value): Int ={
    visitType match {
      case VisitType.erroneous => erroneous_data_cnt
      case VisitType.short_stay => short_stay_cnt
      case VisitType.standard_stay => standard_stay_cnt
      case VisitType.standard_extended_stay =>  standard_extended_stay_cnt
      case VisitType.long_stay =>  long_stay_cnt
      case _ => throw new UnsupportedOperationException("Wrong visit type")
    }
  }


}
