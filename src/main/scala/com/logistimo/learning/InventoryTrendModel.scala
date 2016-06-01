package com.logistimo.learning


/**
 * Created by kaniyarasu on 03/05/16.
 */
class InventoryTrendModel(dIDc: String, rkc: String, tdc: String, stockCountC: String) extends Serializable{
  var did = dIDc
  var mid = rkc
  var i_date = tdc
  var count = stockCountC

  override def toString:String = {
    did + "\t" + mid + "\t" + i_date + "\t" + count
  }
}
