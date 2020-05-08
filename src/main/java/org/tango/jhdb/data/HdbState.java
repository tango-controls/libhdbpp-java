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
 * Tango HDB state data
 */
public class HdbState extends HdbScalarData {

  int value = 0;
  int wvalue = 0;

  public static HdbData createData(HdbSigInfo info) throws HdbFailed
  {
    switch (info.format)
    {
      case SCALAR:
        return new HdbState(info);
      case SPECTRUM:
        return new HdbStateArray(info);
      default:
        throw new HdbFailed("Format :" + info.format + " not supported.");
    }
  }
  public HdbState(HdbSigInfo info) {
    super(info);
  }

  public HdbState(HdbSigInfo info, int value, int wvalue) {
    this(info);
    this.value = value;
    this.wvalue = wvalue;
  }

  public int getValue() throws HdbFailed {

    if(hasFailed())
      throw new HdbFailed(this.errorMessage);
    return value;

  }

  public int getWriteValue() throws HdbFailed {

    if(hasFailed())
      throw new HdbFailed(this.errorMessage);
    return wvalue;

  }

  public void parseValue(ArrayList<Object> value) throws HdbFailed {

    this.value = parseState(value.get(0));

  }

  public void parseWriteValue(ArrayList<Object> value) throws HdbFailed {

    if(value!=null)
      this.wvalue = parseState(value.get(0));

  }

  private int parseState(Object value) throws HdbFailed {

    return (int)parseInteger(value);

  }

  /**
   * Returns String corresponding to given state
   * @param value State as int value
   */
  public static String getStateString(int value) {

    switch (value) {
      case 0:
        return "ON";
      case 1:
        return "OFF";
      case 2:
        return "CLOSE";
      case 3:
        return "OPEN";
      case 4:
        return "INSERT";
      case 5:
        return "EXTRACT";
      case 6:
        return "MOVING";
      case 7:
        return "STANDBY";
      case 8:
        return "FAULT";
      case 9:
        return "INIT";
      case 10:
        return "RUNNING";
      case 11:
        return "ALARM";
      case 12:
        return "DISABLE";
      case 13:
        return "UNKNOWN";
      default:
        return "Unknown code";
    }

  }

  public String toString() {

    if(hasFailed())
      return timeToStr(dataTime)+": "+errorMessage;

    if(!hasWriteValue())
      return timeToStr(dataTime)+": "+getStateString(value)+" "+qualitytoStr(qualityFactor);
    else
      return timeToStr(dataTime)+": "+getStateString(value)+";"+getStateString(wvalue)+" "+
          qualitytoStr(qualityFactor);

  }

  // Convenience function
  public void applyConversionFactor(double f) {
    // Do nothing here
  }

  void copyData(HdbData src) {
    this.value = ((HdbState)src).value;
    this.wvalue = ((HdbState)src).wvalue;
  }

  public String getValueAsString() {
    if(hasFailed())
      return errorMessage;
    if(isInvalid())
      return "ATTR_INVALID";
    return getStateString(value);
  }

  public String getWriteValueAsString() {
    if(hasFailed())
      return errorMessage;
    if(hasWriteValue()) {
      if(isInvalid())
        return "ATTR_INVALID";
      return getStateString(wvalue);
    } else
      return "";
  }

  public double getValueAsDouble() throws HdbFailed {
    if(hasFailed())
      throw new HdbFailed(this.errorMessage);
    if(isInvalid())
      return Double.NaN;
    return (double)value;
  }

  public double getWriteValueAsDouble() throws HdbFailed {
    if(hasFailed())
      throw new HdbFailed(this.errorMessage);
    if(hasWriteValue()) {
      if(isInvalid())
        return Double.NaN;
      return (double)wvalue;
    } else {
      throw new HdbFailed("This datum has no write value");
    }
  }

  public long getValueAsLong() throws HdbFailed {
    if(hasFailed())
      throw new HdbFailed(this.errorMessage);
    return (long)value;
  }

  public long getWriteValueAsLong() throws HdbFailed {
    if(hasFailed())
      throw new HdbFailed(this.errorMessage);
    if(hasWriteValue()) {
      return (long)wvalue;
    } else {
      throw new HdbFailed("This datum has no write value");
    }
  }
}
