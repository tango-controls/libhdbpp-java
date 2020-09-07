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
 * HDB short data (16 bits integer)
 */
public class HdbShort extends HdbScalarData {

  short value = 0;
  short wvalue = 0;

  public static HdbData createData(SignalInfo info) throws HdbFailed
  {
    switch (info.format)
    {
      case SCALAR:
        return new HdbShort(info);
      case SPECTRUM:
        return new HdbShortArray(info);
      default:
        throw new HdbFailed("Format :" + info.format + " not supported.");
    }
  }

  public HdbShort(SignalInfo info) {
    super(info);
  }

  public HdbShort(SignalInfo info, short value) {
    this(info);
    this.value = value;
  }

  public HdbShort(SignalInfo info, short value, short wvalue) {
    this(info);
    this.value = value;
    this.wvalue = wvalue;
  }

  @Override
  public HdbShort copyData()
  {
    return new HdbShort(info, value, wvalue);
  }

  public short getValue() throws HdbFailed {

    if(hasFailed())
      throw new HdbFailed(this.errorMessage);
    return value;

  }

  public short getWriteValue() throws HdbFailed {

    if(hasFailed())
      throw new HdbFailed(this.errorMessage);
    return wvalue;

  }

  public void parseValue(ArrayList<Object> value) throws HdbFailed {

    this.value = parseShort(value.get(0));

  }

  public void parseWriteValue(ArrayList<Object> value) throws HdbFailed {

    if(value!=null)
      this.wvalue = parseShort(value.get(0));

  }

  private short parseShort(Object value) throws HdbFailed {

    return (short)parseInteger(value);

  }

  // Convenience function
  public void applyConversionFactor(double f) {
    value = (short)(value * f);
    wvalue = (short)(wvalue * f);
  }

  public String getValueAsString() {
    if(hasFailed())
      return errorMessage;
    if(isInvalid())
      return "ATTR_INVALID";
    return Short.toString(value);
  }

  public String getWriteValueAsString() {
    if(hasFailed())
      return errorMessage;
    if(hasWriteValue()) {
      if(isInvalid())
        return "ATTR_INVALID";
      return Short.toString(wvalue);
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
