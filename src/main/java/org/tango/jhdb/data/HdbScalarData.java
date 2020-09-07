//+======================================================================
// $Source: $
//
// Project:   Tango
//
// Description:  java source code for HDB extraction library.
//
// $Author: pons $
//
// Copyright (C) :      2015
//						European Synchrotron Radiation Facility
//                      BP 220, Grenoble 38043
//                      FRANCE
//
// This file is part of Tango.
//
// Tango is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Tango is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with Tango.  If not, see <http://www.gnu.org/licenses/>.
//
// $Revision $
//
//-======================================================================
package org.tango.jhdb.data;

import org.tango.jhdb.HdbFailed;
import org.tango.jhdb.SignalInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * HDB scalar data
 */
public abstract class HdbScalarData extends HdbData {

  public HdbScalarData(SignalInfo info) {
    super(info);
  }

  public String toString() {

    if(hasFailed())
      return timeToStr(dataTime)+": "+errorMessage;

    if(!hasWriteValue())
      return timeToStr(dataTime)+": "+getValueAsString()+" "+qualitytoStr(qualityFactor);
    else
      return timeToStr(dataTime)+": "+getValueAsString()+";"+getWriteValueAsString()+" "+
             qualitytoStr(qualityFactor);

  }

  int dataSize() {
    return 1;
  }
  int dataSizeW() {
    if(hasWriteValue())
      return 1;
    else
      return 0;
  }

  public double[] getValueAsDoubleArray() throws HdbFailed {
    throw new HdbFailed("This datum is not an array");
  }

  public double[] getWriteValueAsDoubleArray() throws HdbFailed {
    throw new HdbFailed("This datum is not an array");
  }

  public long getValueAsLong() throws HdbFailed {
    throw new HdbFailed("This datum is not an integer");
  }

  public long getWriteValueAsLong() throws HdbFailed {
    throw new HdbFailed("This datum is not an integer");
  }

  public long[] getValueAsLongArray() throws HdbFailed {
    throw new HdbFailed("This datum is not an integer");
  }

  public long[] getWriteValueAsLongArray() throws HdbFailed {
    throw new HdbFailed("This datum is not an integer");
  }

  public Map<Aggregate, List<Number>> getAggregate() throws HdbFailed
  {
    return EMPTY_AGGREGATE;
  }

  protected void doParseAggregate(long count_rows, long count_errors
          , ArrayList<Long> count_r, ArrayList<Long> count_nan_r, ArrayList<Double> mean_r, ArrayList<Number> min_r, ArrayList<Number> max_r, ArrayList<Double> stddev_r
          , ArrayList<Long> count_w, ArrayList<Long> count_nan_w, ArrayList<Double> mean_w, ArrayList<Number> min_w, ArrayList<Number> max_w, ArrayList<Double> stddev_w)
  {
    //do nothing
  }
}
