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
 * HDB double array data
 */
public class HdbDoubleArray extends HdbArrayData {

  double[] value = new double[0];
  double[] wvalue = new double[0];

  public HdbDoubleArray(SignalInfo info) {
    super(info);
  }

  public HdbDoubleArray(SignalInfo info, double[] value) {
    this(info);
    this.value = value.clone();
  }

  public HdbDoubleArray(SignalInfo info, double[] value, double[] wvalue) {
    this(info);
    this.value = value.clone();
    this.wvalue = wvalue.clone();
  }

  @Override
  public HdbDoubleArray copyData()
  {
    return new HdbDoubleArray(info, value, wvalue);
  }

  public double[] getValue() throws HdbFailed {

    if(hasFailed())
      throw new HdbFailed(this.errorMessage);
    return value;

  }

  public double[] getWriteValue() throws HdbFailed {

    if(hasFailed())
      throw new HdbFailed(this.errorMessage);
    return wvalue;

  }

  public void parseValue(ArrayList<Object> value) throws HdbFailed {

    this.value = parseDoubleArray(value);

  }

  public void parseWriteValue(ArrayList<Object> value) throws HdbFailed {

    if(value!=null)
      this.wvalue = parseDoubleArray(value);

  }

  private double[] parseDoubleArray(ArrayList<Object> value) throws HdbFailed {

    double[] ret = new double[value.size()];
    if(value.isEmpty())
      return ret;

    try {

      Object o0 = value.get(0);
      if (o0==null || o0 instanceof String) {
        // Value given as string
        for (int i = 0; i < value.size(); i++) {
          String str = (String)value.get(i);
          if (str == null) {
            ret[i] = Double.NaN;
          } else {
            ret[i] = Double.parseDouble(str);
          }
        }
      } else {
        for (int i = 0; i < value.size(); i++) {
          Double d = (Double)value.get(i);
          if(d==null)
            ret[i] = Double.NaN;
          else
            ret[i] = d.doubleValue();
        }
      }

    } catch(NumberFormatException e) {
      throw new HdbFailed("parseDoubleArray: Invalid number syntax");
    }

    return ret;

  }

  // Convenience function
  public void applyConversionFactor(double f) {
    for(int i=0;i<dataSize();i++)
      value[i] = (value[i] * f);
    for(int i=0;i<dataSizeW();i++)
      wvalue[i] = (wvalue[i] * f);
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

  private String arrayValue(double[] b) {
    StringBuffer ret = new StringBuffer();
    ret.append("Double["+b.length+"]\n");
    for(int i=0;i<b.length;i++) {
      ret.append(Double.toString(b[i]));
      if(i<b.length-1)
        ret.append("\n");
    }
    return ret.toString();
  }

  public double[] getValueAsDoubleArray() throws HdbFailed {
    if(hasFailed())
      throw new HdbFailed(this.errorMessage);
    return value;
  }

  public double[] getWriteValueAsDoubleArray() throws HdbFailed {
    if(hasFailed())
      throw new HdbFailed(this.errorMessage);
    if(!hasWriteValue())
      throw new HdbFailed("This datum has no write value");
    return wvalue;
  }

  public long[] getValueAsLongArray() throws HdbFailed {
    throw new HdbFailed("This datum is not an integer");
  }

  public long[] getWriteValueAsLongArray() throws HdbFailed {
    throw new HdbFailed("This datum is not an integer");
  }

}
