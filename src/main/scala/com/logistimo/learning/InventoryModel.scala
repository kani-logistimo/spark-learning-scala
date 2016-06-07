package com.logistimo.learning

/**
 * Created by kaniyarasu on 01/06/16.
 */
class InventoryModel(dids :String, mids :String,dates:String, sqs:String,tcs :Integer) extends Serializable{
  var did = dids
  var id = mids
  var date = dates
  var sq = sqs
  var tc = tcs

  override  def toString(): String ={
    did+","+ id+","+dates+","+sqs+","+tc

  }

}