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
 * HDB int data (32 bits unsigned integer)
 */
public class HdbULong extends HdbScalarData {

  long value = 0;
  long wvalue = 0;

  public static HdbData createData(HdbSigInfo info) throws HdbFailed
  {
    switch (info.format)
    {
      case SCALAR:
        return new HdbULong(info);
      case SPECTRUM:
        return new HdbULongArray(info);
      default:
        throw new HdbFailed("Format :" + info.format + " not supported.");
    }
  }
  public HdbULong(HdbSigInfo info) {
    super(info);
  }

  public HdbULong(HdbSigInfo info, long value, long wvalue) {
    this(info);
    this.value = value;
    this.wvalue = wvalue;
  }

  public long getValue() throws HdbFailed {

    if(hasFailed())
      throw new HdbFailed(this.errorMessage);
    return value;

  }

  public long getWriteValue() throws HdbFailed {

    if(hasFailed())
      throw new HdbFailed(this.errorMessage);
    return wvalue;

  }

  public void parseValue(ArrayList<Object> value) throws HdbFailed {

    this.value = parseULong(value.get(0));

  }

  public void parseWriteValue(ArrayList<Object> value) throws HdbFailed {

    if(value!=null)
      this.wvalue = parseULong(value.get(0));

  }

  private long parseULong(Object value) throws HdbFailed {

    return parseInteger(value);

  }

  // Convenience function
  public void applyConversionFactor(double f) {
    value = (long)(value * f);
    wvalue = (long)(wvalue * f);
  }

  void copyData(HdbData src) {
    this.value = ((HdbULong)src).value;
    this.wvalue = ((HdbULong)src).wvalue;
  }

  public String getValueAsString() {
    if(hasFailed())
      return errorMessage;
    if(isInvalid())
      return "ATTR_INVALID";
    return Long.toString(value);
  }

  public String getWriteValueAsString() {
    if(hasFailed())
      return errorMessage;
    if(hasWriteValue()) {
      if(isInvalid())
        return "ATTR_INVALID";
      return Long.toString(wvalue);
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
    return value;
  }

  public long getWriteValueAsLong() throws HdbFailed {
    if(hasFailed())
      throw new HdbFailed(this.errorMessage);
    if(hasWriteValue()) {
      return wvalue;
    } else {
      throw new HdbFailed("This datum has no write value");
    }
  }
}
