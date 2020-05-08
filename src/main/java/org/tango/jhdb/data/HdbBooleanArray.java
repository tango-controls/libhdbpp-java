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
import org.tango.jhdb.HdbSigInfo;

import java.util.ArrayList;

/**
 * HDB boolean array
 */
public class HdbBooleanArray extends HdbArrayData {

  boolean[] value = null;
  boolean[] wvalue = null;

  public HdbBooleanArray(HdbSigInfo info){
    super(info);
  }

  public HdbBooleanArray(HdbSigInfo info, boolean[] value) {
    this(info);
    this.value = value.clone();
  }

  public HdbBooleanArray(HdbSigInfo info, boolean[] value, boolean[] wvalue) {
    this(info);
    this.value = value.clone();
    this.wvalue = wvalue.clone();
  }

  public boolean[] getValue() throws HdbFailed {

    if(hasFailed())
      throw new HdbFailed(this.errorMessage);
    return value;

  }

  public boolean[] getWriteValue() throws HdbFailed {

    if(hasFailed())
      throw new HdbFailed(this.errorMessage);
    return wvalue;

  }

  public void parseValue(ArrayList<Object> value) throws HdbFailed {

    this.value = parseBooleanArray(value);

  }

  public void parseWriteValue(ArrayList<Object> value) throws HdbFailed {

    if(value!=null)
      this.wvalue = parseBooleanArray(value);

  }

  private boolean[] parseBooleanArray(ArrayList<Object> value) throws HdbFailed {

    if(value==null)
      return new boolean[0];

    boolean[] ret = new boolean[value.size()];
    if(value.size()==0)
      return ret;

    if( value.get(0) instanceof String ) {

      // Value given as string
      try {
        for(int i=0;i<value.size();i++) {
          String str = (String)value.get(i);
          if(str==null) {
            ret[i] = false;
          } else {
            ret[i] = (str.equalsIgnoreCase("true") || str.equalsIgnoreCase("t"));
          }
        }
      } catch(NumberFormatException e) {
        throw new HdbFailed("parseBoolean: Invalid number syntax");
      }

    } else {

      for(int i=0;i<value.size();i++) {
        Boolean b = (Boolean)value.get(i);
        ret[i] = b.booleanValue();
      }

    }

    return ret;

  }

  // Convenience function
  public void applyConversionFactor(double f) {
    // Do nothing here
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

  void copyData(HdbData src) {
    value = ((HdbBooleanArray)src).value.clone();
    if(((HdbBooleanArray)src).wvalue!=null)
      wvalue = ((HdbBooleanArray)src).wvalue.clone();
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

  private String arrayValue(boolean[] b) {
    StringBuffer ret = new StringBuffer();
    ret.append("Boolean["+b.length+"]\n");
    for(int i=0;i<b.length;i++) {
      ret.append(Boolean.toString(b[i]));
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
      ret[i] = (value[i])?1:0;
    return ret;
  }

  public double[] getWriteValueAsDoubleArray() throws HdbFailed {
    if(hasFailed())
      throw new HdbFailed(this.errorMessage);
    if(!hasWriteValue())
      throw new HdbFailed("This datum has no write value");
    double[] ret = new double[wvalue.length];
    for(int i=0;i<wvalue.length;i++)
      ret[i] = (wvalue[i])?1:0;
    return ret;
  }

  public long[] getValueAsLongArray() throws HdbFailed {
    if(hasFailed())
      throw new HdbFailed(this.errorMessage);
    long[] ret = new long[value.length];
    for(int i=0;i<value.length;i++)
      ret[i] = (value[i])?1:0;
    return ret;
  }

  public long[] getWriteValueAsLongArray() throws HdbFailed {
    if(hasFailed())
      throw new HdbFailed(this.errorMessage);
    if(!hasWriteValue())
      throw new HdbFailed("This datum has no write value");
    long[] ret = new long[wvalue.length];
    for(int i=0;i<wvalue.length;i++)
      ret[i] = (wvalue[i])?1:0;
    return ret;
  }
}
