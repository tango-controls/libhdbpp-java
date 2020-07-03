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
 * HDB boolean data (8bit integer)
 */
public class HdbBoolean extends HdbScalarData {

  boolean value = false;
  boolean wvalue = false;

  public static HdbData createData(SignalInfo info) throws HdbFailed
  {
    switch (info.format)
    {
      case SCALAR:
        return new HdbBoolean(info);
      case SPECTRUM:
        return new HdbBooleanArray(info);
      default:
        throw new HdbFailed("Format :" + info.format + " not supported.");
    }
  }

  public HdbBoolean(SignalInfo info)
  {
    super(info);
  }

  public HdbBoolean(SignalInfo info, boolean value) {
    this(info);
    this.value = value;
  }

  public HdbBoolean(SignalInfo info, boolean value, boolean wvalue) {
    this(info);
    this.value = value;
    this.wvalue = wvalue;
  }

  @Override
  public HdbBoolean copyData()
  {
    return new HdbBoolean(info, value, wvalue);
  }

  public boolean getValue() throws HdbFailed {

    if(hasFailed())
      throw new HdbFailed(this.errorMessage);
    return value;

  }

  public boolean getWriteValue() throws HdbFailed {

    if(hasFailed())
      throw new HdbFailed(this.errorMessage);
    return wvalue;

  }

  public void parseValue(ArrayList<Object> value) throws HdbFailed {

    this.value = parseBoolean(value.get(0));

  }

  public void parseWriteValue(ArrayList<Object> value) throws HdbFailed {

    if(value!=null)
      this.wvalue = parseBoolean(value.get(0));

  }

  private boolean parseBoolean(Object value) throws HdbFailed {

    boolean ret;

    if(value==null)
      return false;

    if( value instanceof String ) {

      // Value given as string
      String str = (String)value;
      return ( (str.equalsIgnoreCase("true") || str.equalsIgnoreCase("t")) );

    } else {

      Boolean b = (Boolean)value;
      ret = b.booleanValue();

    }

    return ret;

  }

  // Convenience function
  public void applyConversionFactor(double f) {
    // Do nothing here
  }

  public String getValueAsString() {
    if(hasFailed())
      return errorMessage;
    if(isInvalid())
      return "ATTR_INVALID";
    return Boolean.toString(value);
  }

  public String getWriteValueAsString() {
    if(hasFailed())
      return errorMessage;
    if(hasWriteValue()) {
      if(isInvalid())
        return "ATTR_INVALID";
      return Boolean.toString(wvalue);
    } else
      return "";
  }

  public double getValueAsDouble() throws HdbFailed {
    if(hasFailed())
      throw new HdbFailed(this.errorMessage);
    if(isInvalid())
      return Double.NaN;
    return (value)?1:0;
  }

  public double getWriteValueAsDouble() throws HdbFailed {
    if(hasFailed())
      throw new HdbFailed(this.errorMessage);
    if(hasWriteValue()) {
      if(isInvalid())
        return Double.NaN;
      return (wvalue)?1:0;
    } else {
      throw new HdbFailed("This datum has no write value");
    }
  }

  public long getValueAsLong() throws HdbFailed {
    if(hasFailed())
      throw new HdbFailed(this.errorMessage);
    return (value)?1:0;
  }

  public long getWriteValueAsLong() throws HdbFailed {
    if(hasFailed())
      throw new HdbFailed(this.errorMessage);
    if(hasWriteValue()) {
      return (wvalue)?1:0;
    } else {
      throw new HdbFailed("This datum has no write value");
    }
  }
}
