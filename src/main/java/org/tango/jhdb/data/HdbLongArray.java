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

/**
 * HDB long array data (32 bits integer)
 */
public class HdbLongArray extends HdbArrayData {

  int[] value = new int[0];
  int[] wvalue = new int[0];

  public HdbLongArray(SignalInfo info) {
    super(info);
  }

  public HdbLongArray(SignalInfo info, int[] value, int[] wvalue) {
    this(info);
    this.value = value.clone();
    this.wvalue = wvalue.clone();
  }

  @Override
  public HdbLongArray copyData()
  {
    return new HdbLongArray(info, value, wvalue);
  }

  public int[] getValue() throws HdbFailed {

    if(hasFailed())
      throw new HdbFailed(this.errorMessage);
    return value;

  }

  public int[] getWriteValue() throws HdbFailed {

    if(hasFailed())
      throw new HdbFailed(this.errorMessage);
    return wvalue;

  }

  public void parseValue(ArrayList<Object> value) throws HdbFailed {

    this.value = parseLongArray(value);

  }

  public void parseWriteValue(ArrayList<Object> value) throws HdbFailed {

    if(value!=null)
      this.wvalue = parseLongArray(value);

  }

  private int[] parseLongArray(ArrayList<Object> value) throws HdbFailed {

    if(value==null)
      return new int[0];

    int[] ret = new int[value.size()];
    if(value.size()==0)
      return ret;

    for(int i=0;i<value.size();i++)
      ret[i] = (int)parseInteger(value.get(i));

    return ret;

  }

  // Convenience function
  public void applyConversionFactor(double f) {
    for(int i=0;i<dataSize();i++)
      value[i] = (int)(value[i] * f);
    for(int i=0;i<dataSizeW();i++)
      wvalue[i] = (int)(wvalue[i] * f);
  }
  int dataSize() {
    if(value==null)
      return 0;
    else
      return value.length;
  }
  int dataSizeW() {
    if(hasWriteValue())
      if(wvalue==null)
        return 0;
      else
        return wvalue.length;
    else
      return 0;
  }

  public String getValueAsString() {
    if(hasFailed())
      return errorMessage;
    if(isInvalid())
      return "ATTR_INVALID";
    return arrayValue(value);
  }

  public String getWriteValueAsString() {
    if(hasFailed())
      return errorMessage;
    if(hasWriteValue()) {
      if(isInvalid())
        return "ATTR_INVALID";
      return arrayValue(wvalue);
    } else
      return "";
  }

  private String arrayValue(int[] b) {
    StringBuffer ret = new StringBuffer();
    ret.append("Integer["+b.length+"]\n");
    for(int i=0;i<b.length;i++) {
      ret.append(Integer.toString(b[i]));
      if(i<b.length-1)
        ret.append("\n");
    }
    return ret.toString();
  }

  public double[] getValueAsDoubleArray() throws HdbFailed {
    if(hasFailed())
      throw new HdbFailed(this.errorMessage);
    double[] ret = new double[value.length];
    for(int i=0;i<value.length;i++)
      ret[i] = (double)value[i];
    return ret;
  }

  public double[] getWriteValueAsDoubleArray() throws HdbFailed {
    if(hasFailed())
      throw new HdbFailed(this.errorMessage);
    if(!hasWriteValue())
      throw new HdbFailed("This datum has no write value");
    double[] ret = new double[wvalue.length];
    for(int i=0;i<wvalue.length;i++)
      ret[i] = (double)wvalue[i];
    return ret;
  }

  public long[] getValueAsLongArray() throws HdbFailed {
    if(hasFailed())
      throw new HdbFailed(this.errorMessage);
    long[] ret = new long[value.length];
    for(int i=0;i<value.length;i++)
      ret[i] = (long)value[i];
    return ret;
  }

  public long[] getWriteValueAsLongArray() throws HdbFailed {
    if(hasFailed())
      throw new HdbFailed(this.errorMessage);
    if(!hasWriteValue())
      throw new HdbFailed("This datum has no write value");
    long[] ret = new long[wvalue.length];
    for(int i=0;i<wvalue.length;i++)
      ret[i] = (long)wvalue[i];
    return ret;
  }

}
