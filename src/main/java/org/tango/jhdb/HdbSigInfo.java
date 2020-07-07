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

package org.tango.jhdb;

import java.util.HashMap;
import java.util.Map;

/**
 * Signal info structure
 */
public class HdbSigInfo extends SignalInfo
{
  public final static int TYPE_SCALAR_DOUBLE_RO = 1;
  public final static int TYPE_SCALAR_DOUBLE_RW = 2;
  public final static int TYPE_ARRAY_DOUBLE_RO = 3;
  public final static int TYPE_ARRAY_DOUBLE_RW = 4;

  public final static int TYPE_SCALAR_LONG64_RO = 5;
  public final static int TYPE_SCALAR_LONG64_RW = 6;
  public final static int TYPE_ARRAY_LONG64_RO = 7;
  public final static int TYPE_ARRAY_LONG64_RW = 8;

  public final static int TYPE_SCALAR_CHAR_RO = 9;
  public final static int TYPE_SCALAR_CHAR_RW = 10;
  public final static int TYPE_ARRAY_CHAR_RO = 11;
  public final static int TYPE_ARRAY_CHAR_RW = 12;

  public final static int TYPE_SCALAR_STRING_RO = 13;
  public final static int TYPE_SCALAR_STRING_RW = 14;
  public final static int TYPE_ARRAY_STRING_RO = 15;
  public final static int TYPE_ARRAY_STRING_RW = 16;

  public final static int TYPE_SCALAR_FLOAT_RO = 17;
  public final static int TYPE_SCALAR_FLOAT_RW = 18;
  public final static int TYPE_ARRAY_FLOAT_RO = 19;
  public final static int TYPE_ARRAY_FLOAT_RW = 20;

  public final static int TYPE_SCALAR_UCHAR_RO = 21;
  public final static int TYPE_SCALAR_UCHAR_RW = 22;
  public final static int TYPE_ARRAY_UCHAR_RO = 23;
  public final static int TYPE_ARRAY_UCHAR_RW = 24;

  public final static int TYPE_SCALAR_SHORT_RO = 25;
  public final static int TYPE_SCALAR_SHORT_RW = 26;
  public final static int TYPE_ARRAY_SHORT_RO = 27;
  public final static int TYPE_ARRAY_SHORT_RW = 28;

  public final static int TYPE_SCALAR_USHORT_RO = 29;
  public final static int TYPE_SCALAR_USHORT_RW = 30;
  public final static int TYPE_ARRAY_USHORT_RO = 31;
  public final static int TYPE_ARRAY_USHORT_RW = 32;

  public final static int TYPE_SCALAR_LONG_RO = 33;
  public final static int TYPE_SCALAR_LONG_RW = 34;
  public final static int TYPE_ARRAY_LONG_RO = 35;
  public final static int TYPE_ARRAY_LONG_RW = 36;

  public final static int TYPE_SCALAR_ULONG_RO = 37;
  public final static int TYPE_SCALAR_ULONG_RW = 38;
  public final static int TYPE_ARRAY_ULONG_RO = 39;
  public final static int TYPE_ARRAY_ULONG_RW = 40;

  public final static int TYPE_SCALAR_STATE_RO = 41;
  public final static int TYPE_SCALAR_STATE_RW = 42;
  public final static int TYPE_ARRAY_STATE_RO = 43;
  public final static int TYPE_ARRAY_STATE_RW = 44;

  public final static int TYPE_SCALAR_BOOLEAN_RO = 45;
  public final static int TYPE_SCALAR_BOOLEAN_RW = 46;
  public final static int TYPE_ARRAY_BOOLEAN_RO = 47;
  public final static int TYPE_ARRAY_BOOLEAN_RW = 48;

  public final static int TYPE_SCALAR_ENCODED_RO = 49;
  public final static int TYPE_SCALAR_ENCODED_RW = 50;
  public final static int TYPE_ARRAY_ENCODED_RO = 51;
  public final static int TYPE_ARRAY_ENCODED_RW = 52;

  public final static int TYPE_SCALAR_ULONG64_RO = 53;
  public final static int TYPE_SCALAR_ULONG64_RW = 54;
  public final static int TYPE_ARRAY_ULONG64_RO = 55;
  public final static int TYPE_ARRAY_ULONG64_RW = 56;
  
  private final static Map<SignalInfo, Integer> sigInfoToType;

  static
  {
    sigInfoToType = new HashMap<>();
    SignalInfo scalar_double_ro = new SignalInfo(Type.DOUBLE, Format.SCALAR, Access.RO);
    SignalInfo scalar_double_rw = new SignalInfo(Type.DOUBLE, Format.SCALAR, Access.RW);
    SignalInfo array_double_ro = new SignalInfo(Type.DOUBLE, Format.SPECTRUM, Access.RO);
    SignalInfo array_double_rw = new SignalInfo(Type.DOUBLE, Format.SPECTRUM, Access.RW);

    SignalInfo scalar_float_ro = new SignalInfo(Type.FLOAT, Format.SCALAR, Access.RO);
    SignalInfo scalar_float_rw = new SignalInfo(Type.FLOAT, Format.SCALAR, Access.RW);
    SignalInfo array_float_ro = new SignalInfo(Type.FLOAT, Format.SPECTRUM, Access.RO);
    SignalInfo array_float_rw = new SignalInfo(Type.FLOAT, Format.SPECTRUM, Access.RW);

    SignalInfo scalar_state_ro = new SignalInfo(Type.STATE, Format.SCALAR, Access.RO);
    SignalInfo scalar_state_rw = new SignalInfo(Type.STATE, Format.SCALAR, Access.RW);
    SignalInfo array_state_ro = new SignalInfo(Type.STATE, Format.SPECTRUM, Access.RO);
    SignalInfo array_state_rw = new SignalInfo(Type.STATE, Format.SPECTRUM, Access.RW);

    SignalInfo scalar_string_ro = new SignalInfo(Type.STRING, Format.SCALAR, Access.RO);
    SignalInfo scalar_string_rw = new SignalInfo(Type.STRING, Format.SCALAR, Access.RW);
    SignalInfo array_string_ro = new SignalInfo(Type.STRING, Format.SPECTRUM, Access.RO);
    SignalInfo array_string_rw = new SignalInfo(Type.STRING, Format.SPECTRUM, Access.RW);

    SignalInfo scalar_enc_ro = new SignalInfo(Type.ENCODED, Format.SCALAR, Access.RO);
    SignalInfo scalar_enc_rw = new SignalInfo(Type.ENCODED, Format.SCALAR, Access.RW);
    SignalInfo array_enc_ro = new SignalInfo(Type.ENCODED, Format.SPECTRUM, Access.RO);
    SignalInfo array_enc_rw = new SignalInfo(Type.ENCODED, Format.SPECTRUM, Access.RW);

    SignalInfo scalar_boolean_ro = new SignalInfo(Type.BOOLEAN, Format.SCALAR, Access.RO);
    SignalInfo scalar_boolean_rw = new SignalInfo(Type.BOOLEAN, Format.SCALAR, Access.RW);
    SignalInfo array_boolean_ro = new SignalInfo(Type.BOOLEAN, Format.SPECTRUM, Access.RO);
    SignalInfo array_boolean_rw = new SignalInfo(Type.BOOLEAN, Format.SPECTRUM, Access.RW);

    SignalInfo scalar_char_ro = new SignalInfo(Type.CHAR, Format.SCALAR, Access.RO);
    SignalInfo scalar_char_rw = new SignalInfo(Type.CHAR, Format.SCALAR, Access.RW);
    SignalInfo array_char_ro = new SignalInfo(Type.CHAR, Format.SPECTRUM, Access.RO);
    SignalInfo array_char_rw = new SignalInfo(Type.CHAR, Format.SPECTRUM, Access.RW);

    SignalInfo scalar_uchar_ro = new SignalInfo(Type.UCHAR, Format.SCALAR, Access.RO);
    SignalInfo scalar_uchar_rw = new SignalInfo(Type.UCHAR, Format.SCALAR, Access.RW);
    SignalInfo array_uchar_ro = new SignalInfo(Type.UCHAR, Format.SPECTRUM, Access.RO);
    SignalInfo array_uchar_rw = new SignalInfo(Type.UCHAR, Format.SPECTRUM, Access.RW);

    SignalInfo scalar_long_ro = new SignalInfo(Type.LONG, Format.SCALAR, Access.RO);
    SignalInfo scalar_long_rw = new SignalInfo(Type.LONG, Format.SCALAR, Access.RW);
    SignalInfo array_long_ro = new SignalInfo(Type.LONG, Format.SPECTRUM, Access.RO);
    SignalInfo array_long_rw = new SignalInfo(Type.LONG, Format.SPECTRUM, Access.RW);

    SignalInfo scalar_ulong_ro = new SignalInfo(Type.ULONG, Format.SCALAR, Access.RO);
    SignalInfo scalar_ulong_rw = new SignalInfo(Type.ULONG, Format.SCALAR, Access.RW);
    SignalInfo array_ulong_ro = new SignalInfo(Type.ULONG, Format.SPECTRUM, Access.RO);
    SignalInfo array_ulong_rw = new SignalInfo(Type.ULONG, Format.SPECTRUM, Access.RW);

    SignalInfo scalar_long64_ro = new SignalInfo(Type.LONG64, Format.SCALAR, Access.RO);
    SignalInfo scalar_long64_rw = new SignalInfo(Type.LONG64, Format.SCALAR, Access.RW);
    SignalInfo array_long64_ro = new SignalInfo(Type.LONG64, Format.SPECTRUM, Access.RO);
    SignalInfo array_long64_rw = new SignalInfo(Type.LONG64, Format.SPECTRUM, Access.RW);

    SignalInfo scalar_ulong64_ro = new SignalInfo(Type.ULONG64, Format.SCALAR, Access.RO);
    SignalInfo scalar_ulong64_rw = new SignalInfo(Type.ULONG64, Format.SCALAR, Access.RW);
    SignalInfo array_ulong64_ro = new SignalInfo(Type.ULONG64, Format.SPECTRUM, Access.RO);
    SignalInfo array_ulong64_rw = new SignalInfo(Type.ULONG64, Format.SPECTRUM, Access.RW);

    SignalInfo scalar_short_ro = new SignalInfo(Type.SHORT, Format.SCALAR, Access.RO);
    SignalInfo scalar_short_rw = new SignalInfo(Type.SHORT, Format.SCALAR, Access.RW);
    SignalInfo array_short_ro = new SignalInfo(Type.SHORT, Format.SPECTRUM, Access.RO);
    SignalInfo array_short_rw = new SignalInfo(Type.SHORT, Format.SPECTRUM, Access.RW);

    SignalInfo scalar_ushort_ro = new SignalInfo(Type.USHORT, Format.SCALAR, Access.RO);
    SignalInfo scalar_ushort_rw = new SignalInfo(Type.USHORT, Format.SCALAR, Access.RW);
    SignalInfo array_ushort_ro = new SignalInfo(Type.USHORT, Format.SPECTRUM, Access.RO);
    SignalInfo array_ushort_rw = new SignalInfo(Type.USHORT, Format.SPECTRUM, Access.RW);

    sigInfoToType.put(scalar_double_ro, TYPE_SCALAR_DOUBLE_RO);
    sigInfoToType.put(scalar_double_rw, TYPE_SCALAR_DOUBLE_RW);
    sigInfoToType.put(array_double_ro, TYPE_ARRAY_DOUBLE_RO);
    sigInfoToType.put(array_double_rw, TYPE_ARRAY_DOUBLE_RW);

    sigInfoToType.put(scalar_long64_ro, TYPE_SCALAR_LONG64_RO);
    sigInfoToType.put(scalar_long64_rw, TYPE_SCALAR_LONG64_RW);
    sigInfoToType.put(array_long64_ro, TYPE_ARRAY_LONG64_RO);
    sigInfoToType.put(array_long64_rw, TYPE_ARRAY_LONG64_RW);

    sigInfoToType.put(scalar_char_ro, TYPE_SCALAR_CHAR_RO);
    sigInfoToType.put(scalar_char_rw, TYPE_SCALAR_CHAR_RW);
    sigInfoToType.put(array_char_ro, TYPE_ARRAY_CHAR_RO);
    sigInfoToType.put(array_char_rw, TYPE_ARRAY_CHAR_RW);

    sigInfoToType.put(scalar_string_ro, TYPE_SCALAR_STRING_RO);
    sigInfoToType.put(scalar_string_rw, TYPE_SCALAR_STRING_RW);
    sigInfoToType.put(array_string_ro, TYPE_ARRAY_STRING_RO);
    sigInfoToType.put(array_string_rw, TYPE_ARRAY_STRING_RW);

    sigInfoToType.put(scalar_float_ro, TYPE_SCALAR_FLOAT_RO);
    sigInfoToType.put(scalar_float_rw, TYPE_SCALAR_FLOAT_RW);
    sigInfoToType.put(array_float_ro, TYPE_ARRAY_FLOAT_RO);
    sigInfoToType.put(array_float_rw, TYPE_ARRAY_FLOAT_RW);

    sigInfoToType.put(scalar_uchar_ro, TYPE_SCALAR_UCHAR_RO);
    sigInfoToType.put(scalar_uchar_rw, TYPE_SCALAR_UCHAR_RW);
    sigInfoToType.put(array_uchar_ro, TYPE_ARRAY_UCHAR_RO);
    sigInfoToType.put(array_uchar_rw, TYPE_ARRAY_UCHAR_RW);

    sigInfoToType.put(scalar_short_ro, TYPE_SCALAR_SHORT_RO);
    sigInfoToType.put(scalar_short_rw, TYPE_SCALAR_SHORT_RW);
    sigInfoToType.put(array_short_ro, TYPE_ARRAY_SHORT_RO);
    sigInfoToType.put(array_short_rw, TYPE_ARRAY_SHORT_RW);

    sigInfoToType.put(scalar_ushort_ro, TYPE_SCALAR_USHORT_RO);
    sigInfoToType.put(scalar_ushort_rw, TYPE_SCALAR_USHORT_RW);
    sigInfoToType.put(array_ushort_ro, TYPE_ARRAY_USHORT_RO);
    sigInfoToType.put(array_ushort_rw, TYPE_ARRAY_USHORT_RW);

    sigInfoToType.put(scalar_long_ro, TYPE_SCALAR_LONG_RO);
    sigInfoToType.put(scalar_long_rw, TYPE_SCALAR_LONG_RW);
    sigInfoToType.put(array_long_ro, TYPE_ARRAY_LONG_RO);
    sigInfoToType.put(array_long_rw, TYPE_ARRAY_LONG_RW);

    sigInfoToType.put(scalar_ulong_ro, TYPE_SCALAR_ULONG_RO);
    sigInfoToType.put(scalar_ulong_rw, TYPE_SCALAR_ULONG_RW);
    sigInfoToType.put(array_ulong_ro, TYPE_ARRAY_ULONG_RO);
    sigInfoToType.put(array_ulong_rw, TYPE_ARRAY_ULONG_RW);

    sigInfoToType.put(scalar_state_ro, TYPE_SCALAR_STATE_RO);
    sigInfoToType.put(scalar_state_rw, TYPE_SCALAR_STATE_RW);
    sigInfoToType.put(array_state_ro, TYPE_ARRAY_STATE_RO);
    sigInfoToType.put(array_state_rw, TYPE_ARRAY_STATE_RW);

    sigInfoToType.put(scalar_boolean_ro, TYPE_SCALAR_BOOLEAN_RO);
    sigInfoToType.put(scalar_boolean_rw, TYPE_SCALAR_BOOLEAN_RW);
    sigInfoToType.put(array_boolean_ro, TYPE_ARRAY_BOOLEAN_RO);
    sigInfoToType.put(array_boolean_rw, TYPE_ARRAY_BOOLEAN_RW);

    sigInfoToType.put(scalar_enc_ro, TYPE_SCALAR_ENCODED_RO);
    sigInfoToType.put(scalar_enc_rw, TYPE_SCALAR_ENCODED_RW);
    sigInfoToType.put(array_enc_ro, TYPE_ARRAY_ENCODED_RO);
    sigInfoToType.put(array_enc_rw, TYPE_ARRAY_ENCODED_RW);

    sigInfoToType.put(scalar_ulong64_ro, TYPE_SCALAR_ULONG64_RO);
    sigInfoToType.put(scalar_ulong64_rw, TYPE_SCALAR_ULONG64_RW);
    sigInfoToType.put(array_ulong64_ro, TYPE_ARRAY_ULONG64_RO);
    sigInfoToType.put(array_ulong64_rw, TYPE_ARRAY_ULONG64_RW);
  }


  public final static String[] typeStr = {
      "NONE",
      "TYPE_SCALAR_DOUBLE_RO",
      "TYPE_SCALAR_DOUBLE_RW",
      "TYPE_ARRAY_DOUBLE_RO",
      "TYPE_ARRAY_DOUBLE_RW",
      "TYPE_SCALAR_LONG64_RO",
      "TYPE_SCALAR_LONG64_RW",
      "TYPE_ARRAY_LONG64_RO",
      "TYPE_ARRAY_LONG64_RW",
      "TYPE_SCALAR_CHAR_RO",
      "TYPE_SCALAR_CHAR_RW",
      "TYPE_ARRAY_CHAR_RO",
      "TYPE_ARRAY_CHAR_RW",
      "TYPE_SCALAR_STRING_RO",
      "TYPE_SCALAR_STRING_RW",
      "TYPE_ARRAY_STRING_RO",
      "TYPE_ARRAY_STRING_RW",
      "TYPE_SCALAR_FLOAT_RO",
      "TYPE_SCALAR_FLOAT_RW",
      "TYPE_ARRAY_FLOAT_RO",
      "TYPE_ARRAY_FLOAT_RW",
      "TYPE_SCALAR_UCHAR_RO",
      "TYPE_SCALAR_UCHAR_RW",
      "TYPE_ARRAY_UCHAR_RO",
      "TYPE_ARRAY_UCHAR_RW",
      "TYPE_SCALAR_SHORT_RO",
      "TYPE_SCALAR_SHORT_RW",
      "TYPE_ARRAY_SHORT_RO",
      "TYPE_ARRAY_SHORT_RW",
      "TYPE_SCALAR_USHORT_RO",
      "TYPE_SCALAR_USHORT_RW",
      "TYPE_ARRAY_USHORT_RO",
      "TYPE_ARRAY_USHORT_RW",
      "TYPE_SCALAR_LONG_RO",
      "TYPE_SCALAR_LONG_RW",
      "TYPE_ARRAY_LONG_RO",
      "TYPE_ARRAY_LONG_RW",
      "TYPE_SCALAR_ULONG_RO",
      "TYPE_SCALAR_ULONG_RW",
      "TYPE_ARRAY_ULONG_RO",
      "TYPE_ARRAY_ULONG_RW",
      "TYPE_SCALAR_STATE_RO",
      "TYPE_SCALAR_STATE_RW",
      "TYPE_ARRAY_STATE_RO",
      "TYPE_ARRAY_STATE_RW",
      "TYPE_SCALAR_BOOLEAN_RO",
      "TYPE_SCALAR_BOOLEAN_RW",
      "TYPE_ARRAY_BOOLEAN_RO",
      "TYPE_ARRAY_BOOLEAN_RW",
      "TYPE_SCALAR_ENCODED_RO",
      "TYPE_SCALAR_ENCODED_RW",
      "TYPE_ARRAY_ENCODED_RO",
      "TYPE_ARRAY_ENCODED_RW",
      "TYPE_SCALAR_ULONG64_RO",
      "TYPE_SCALAR_ULONG64_RW",
      "TYPE_ARRAY_ULONG64_RO",
      "TYPE_ARRAY_ULONG64_RW"
  };

  protected HdbSigInfo(SignalInfo parent)
  {
    super(parent);
    this.type = sigInfoToType.getOrDefault(parent.dataType, 0);
  }

  public boolean isStateType()
  {
    return Type.isState(dataType);
  }

  /**
   * Returns true if type is a state type
   * @param type Attribute type
   */
  public static boolean isStateType(int type) {

    switch (type) {
      case TYPE_SCALAR_STATE_RO:
      case TYPE_SCALAR_STATE_RW:
      case TYPE_ARRAY_STATE_RO:
      case TYPE_ARRAY_STATE_RW:
        return true;
      default:
        return false;
    }

  }

  public boolean isIntegerType() {
    return Type.isInteger(dataType);
  }
  /**
   * Returns true if type is an integer type
   * @param type Attribute type
   */
  public static boolean isIntegerType(int type) {

    switch(type) {
      case TYPE_ARRAY_LONG64_RO:
      case TYPE_ARRAY_CHAR_RO:
      case TYPE_ARRAY_LONG64_RW:
      case TYPE_ARRAY_CHAR_RW:
      case TYPE_ARRAY_UCHAR_RO:
      case TYPE_ARRAY_UCHAR_RW:
      case TYPE_ARRAY_SHORT_RO:
      case TYPE_ARRAY_SHORT_RW:
      case TYPE_ARRAY_USHORT_RO:
      case TYPE_ARRAY_USHORT_RW:
      case TYPE_ARRAY_LONG_RO:
      case TYPE_ARRAY_LONG_RW:
      case TYPE_ARRAY_ULONG_RO:
      case TYPE_ARRAY_ULONG_RW:
      case TYPE_ARRAY_STATE_RO:
      case TYPE_ARRAY_STATE_RW:
      case TYPE_ARRAY_BOOLEAN_RO:
      case TYPE_ARRAY_BOOLEAN_RW:
      case TYPE_ARRAY_ULONG64_RO:
      case TYPE_ARRAY_ULONG64_RW:
      case TYPE_SCALAR_LONG64_RO:
      case TYPE_SCALAR_CHAR_RO:
      case TYPE_SCALAR_LONG64_RW:
      case TYPE_SCALAR_CHAR_RW:
      case TYPE_SCALAR_UCHAR_RO:
      case TYPE_SCALAR_UCHAR_RW:
      case TYPE_SCALAR_SHORT_RO:
      case TYPE_SCALAR_SHORT_RW:
      case TYPE_SCALAR_USHORT_RO:
      case TYPE_SCALAR_USHORT_RW:
      case TYPE_SCALAR_LONG_RO:
      case TYPE_SCALAR_LONG_RW:
      case TYPE_SCALAR_ULONG_RO:
      case TYPE_SCALAR_ULONG_RW:
      case TYPE_SCALAR_STATE_RO:
      case TYPE_SCALAR_STATE_RW:
      case TYPE_SCALAR_BOOLEAN_RO:
      case TYPE_SCALAR_BOOLEAN_RW:
      case TYPE_SCALAR_ULONG64_RO:
      case TYPE_SCALAR_ULONG64_RW:
        return true;
      default:
        return false;
    }

  }

  public boolean isStringType()
  {
    return Type.isString(dataType);
  }

  /**
   * Returns true if type is a string type
   * @param type Attribute type
   */
  public static boolean isStringType(int type) {

    switch(type) {
      case TYPE_SCALAR_STRING_RO:
      case TYPE_SCALAR_STRING_RW:
      case TYPE_ARRAY_STRING_RO:
      case TYPE_ARRAY_STRING_RW:
        return true;
      default:
        return false;
    }

  }

  public boolean isNumericType()
  {
    return Type.isNumeric(dataType);
  }
  /**
  * Returns true if type is a numeric type
  * @param type Attribute type
  */
  public static boolean isNumericType(int type) {

    switch(type) {
      case TYPE_SCALAR_ENCODED_RO:
      case TYPE_SCALAR_ENCODED_RW:
      case TYPE_ARRAY_ENCODED_RO:
      case TYPE_ARRAY_ENCODED_RW:
      case TYPE_SCALAR_STRING_RO:
      case TYPE_SCALAR_STRING_RW:
      case TYPE_ARRAY_STRING_RO:
      case TYPE_ARRAY_STRING_RW:
        return false;
      default:
        return true;
    }

  }

  public boolean isRWType()
  {
    return Access.isRW(access);
  }

  /**
   * Returns true if type is a Read/Write type
   * @param type Attribute type
   */
  public static boolean isRWType(int type) {

    switch(type) {
      case TYPE_SCALAR_DOUBLE_RW:
      case TYPE_ARRAY_DOUBLE_RW:
      case TYPE_SCALAR_LONG64_RW:
      case TYPE_ARRAY_LONG64_RW:
      case TYPE_SCALAR_CHAR_RW:
      case TYPE_ARRAY_CHAR_RW:
      case TYPE_SCALAR_STRING_RW:
      case TYPE_ARRAY_STRING_RW:
      case TYPE_SCALAR_FLOAT_RW:
      case TYPE_ARRAY_FLOAT_RW:
      case TYPE_SCALAR_UCHAR_RW:
      case TYPE_ARRAY_UCHAR_RW:
      case TYPE_SCALAR_SHORT_RW:
      case TYPE_ARRAY_SHORT_RW:
      case TYPE_SCALAR_USHORT_RW:
      case TYPE_ARRAY_USHORT_RW:
      case TYPE_SCALAR_LONG_RW:
      case TYPE_ARRAY_LONG_RW:
      case TYPE_SCALAR_ULONG_RW:
      case TYPE_ARRAY_ULONG_RW:
      case TYPE_SCALAR_STATE_RW:
      case TYPE_ARRAY_STATE_RW:
      case TYPE_SCALAR_BOOLEAN_RW:
      case TYPE_ARRAY_BOOLEAN_RW:
      case TYPE_SCALAR_ENCODED_RW:
      case TYPE_ARRAY_ENCODED_RW:
      case TYPE_SCALAR_ULONG64_RW:
      case TYPE_ARRAY_ULONG64_RW:
        return true;
      default:
        return false;
    }

  }

  public boolean isArrayType()
  {
    return Format.isArray(format);
  }

  /**
   * Returns true if type is an array type
   * @param type Attribute type
   */
  public static boolean isArrayType(int type) {

    switch(type) {
      case TYPE_ARRAY_DOUBLE_RO:
      case TYPE_ARRAY_LONG64_RO:
      case TYPE_ARRAY_CHAR_RO:
      case TYPE_ARRAY_STRING_RO:
      case TYPE_ARRAY_DOUBLE_RW:
      case TYPE_ARRAY_LONG64_RW:
      case TYPE_ARRAY_CHAR_RW:
      case TYPE_ARRAY_STRING_RW:
      case TYPE_ARRAY_FLOAT_RO:
      case TYPE_ARRAY_FLOAT_RW:
      case TYPE_ARRAY_UCHAR_RO:
      case TYPE_ARRAY_UCHAR_RW:
      case TYPE_ARRAY_SHORT_RO:
      case TYPE_ARRAY_SHORT_RW:
      case TYPE_ARRAY_USHORT_RO:
      case TYPE_ARRAY_USHORT_RW:
      case TYPE_ARRAY_LONG_RO:
      case TYPE_ARRAY_LONG_RW:
      case TYPE_ARRAY_ULONG_RO:
      case TYPE_ARRAY_ULONG_RW:
      case TYPE_ARRAY_STATE_RO:
      case TYPE_ARRAY_STATE_RW:
      case TYPE_ARRAY_BOOLEAN_RO:
      case TYPE_ARRAY_BOOLEAN_RW:
      case TYPE_ARRAY_ENCODED_RO:
      case TYPE_ARRAY_ENCODED_RW:
      case TYPE_ARRAY_ULONG64_RO:
      case TYPE_ARRAY_ULONG64_RW:
        return true;
      default:
        return false;
    }

  }

  public void setTypeAccessFormatFromName(String stype) throws HdbFailed
  {
    boolean error = false;
    String[] confs = stype.toLowerCase().split("_");
    if(confs[0].equalsIgnoreCase("scalar"))
    {
      format = Format.SCALAR;
    }
    else if (confs[0].equalsIgnoreCase("array"))
    {
      format = Format.SPECTRUM;
    }
    else
    {
      error = true;
      format = Format.UNKNOWN;
    }

    if(confs[2].equalsIgnoreCase("ro"))
    {
      access = Access.RO;
    }
    else if (confs[2].equalsIgnoreCase("rw"))
    {
      access = Access.RW;
    }
    else
    {
      error = true;
      access = Access.UNKNOWN;
    }

    if (stype.equalsIgnoreCase("devulong64")) {
      dataType = Type.ULONG64;
    } else if (stype.equalsIgnoreCase("devstring")) {
      dataType = Type.STRING;
    } else if (stype.equalsIgnoreCase("devlong64")) {
      dataType = Type.LONG64;
    } else if (stype.equalsIgnoreCase("devfloat")) {
      dataType = Type.FLOAT;
    } else if (stype.equalsIgnoreCase("devdouble")) {
      dataType = Type.DOUBLE;
    } else if (stype.equalsIgnoreCase("devlong")) {
      dataType = Type.LONG;
    } else if (stype.equalsIgnoreCase("devuchar")) {
      dataType = Type.UCHAR;
    } else if (stype.equalsIgnoreCase("devencoded")) {
      dataType = Type.ENCODED;
    } else if (stype.equalsIgnoreCase("devushort")) {
      dataType = Type.USHORT;
    } else if (stype.equalsIgnoreCase("devboolean")) {
      dataType = Type.BOOLEAN;
    } else if (stype.equalsIgnoreCase("devstate")) {
      dataType = Type.STATE;
    } else if (stype.equalsIgnoreCase("devshort")) {
      dataType = Type.SHORT;
    }  else if (stype.equalsIgnoreCase("devulong")) {
      dataType = Type.ULONG;
    } else if (stype.equalsIgnoreCase("devchar")) {
      dataType = Type.CHAR;
    } else if (stype.equalsIgnoreCase("devenum")) {
      dataType = Type.ENUM;
    } else {
      dataType = Type.UNKNOWN;
      error = true;
    }
    if(error)
    {
      throw new HdbFailed("'" + stype + "' : Unknown type");
    }
  }

  /**
   * Convert a Type name from HDB att_conf to int
   * @param type Type name
   */
  public static int typeFromName(String type) throws HdbFailed {

    if (type.equalsIgnoreCase("scalar_devulong64_rw")) {
      return TYPE_SCALAR_ULONG64_RW;
    } else if (type.equalsIgnoreCase("array_devstring_rw")) {
      return TYPE_ARRAY_STRING_RW;
    } else if (type.equalsIgnoreCase("scalar_devlong64_ro")) {
      return TYPE_SCALAR_LONG64_RO;
    } else if (type.equalsIgnoreCase("scalar_devfloat_rw")) {
      return TYPE_SCALAR_FLOAT_RW;
    } else if (type.equalsIgnoreCase("array_devfloat_ro")) {
      return TYPE_ARRAY_FLOAT_RO;
    } else if (type.equalsIgnoreCase("array_devdouble_ro")) {
      return TYPE_ARRAY_DOUBLE_RO;
    } else if (type.equalsIgnoreCase("scalar_devstring_ro")) {
      return TYPE_SCALAR_STRING_RO;
    } else if (type.equalsIgnoreCase("array_devlong_ro")) {
      return TYPE_ARRAY_LONG_RO;
    } else if (type.equalsIgnoreCase("scalar_devuchar_ro")) {
      return TYPE_SCALAR_UCHAR_RO;
    } else if (type.equalsIgnoreCase("scalar_devlong_ro")) {
      return TYPE_SCALAR_LONG_RO;
    } else if (type.equalsIgnoreCase("array_devencoded_ro")) {
      return TYPE_ARRAY_ENCODED_RO;
    } else if (type.equalsIgnoreCase("array_devlong64_rw")) {
      return TYPE_ARRAY_LONG64_RW;
    } else if (type.equalsIgnoreCase("array_devlong_rw")) {
      return TYPE_ARRAY_LONG_RW;
    } else if (type.equalsIgnoreCase("array_devushort_rw")) {
      return TYPE_ARRAY_USHORT_RW;
    } else if (type.equalsIgnoreCase("scalar_devlong64_rw")) {
      return TYPE_SCALAR_LONG64_RW;
    } else if (type.equalsIgnoreCase("scalar_devboolean_ro")) {
      return TYPE_SCALAR_BOOLEAN_RO;
    } else if (type.equalsIgnoreCase("array_devstate_rw")) {
      return TYPE_ARRAY_STATE_RW;
    } else if (type.equalsIgnoreCase("scalar_devdouble_rw")) {
      return TYPE_SCALAR_DOUBLE_RW;
    } else if (type.equalsIgnoreCase("scalar_devencoded_ro")) {
      return TYPE_SCALAR_ENCODED_RO;
    } else if (type.equalsIgnoreCase("array_devdouble_rw")) {
      return TYPE_ARRAY_DOUBLE_RW;
    } else if (type.equalsIgnoreCase("scalar_devshort_rw")) {
      return TYPE_SCALAR_SHORT_RW;
    } else if (type.equalsIgnoreCase("scalar_devlong_rw")) {
      return TYPE_SCALAR_LONG_RW;
    } else if (type.equalsIgnoreCase("scalar_devushort_rw")) {
      return TYPE_SCALAR_USHORT_RW;
    } else if (type.equalsIgnoreCase("array_devulong64_ro")) {
      return TYPE_ARRAY_ULONG64_RO;
    } else if (type.equalsIgnoreCase("scalar_devulong64_ro")) {
      return TYPE_SCALAR_ULONG64_RO;
    } else if (type.equalsIgnoreCase("array_devulong_rw")) {
      return TYPE_ARRAY_ULONG_RW;
    } else if (type.equalsIgnoreCase("array_devlong64_ro")) {
      return TYPE_ARRAY_LONG64_RO;
    } else if (type.equalsIgnoreCase("scalar_devfloat_ro")) {
      return TYPE_SCALAR_FLOAT_RO;
    } else if (type.equalsIgnoreCase("array_devuchar_rw")) {
      return TYPE_ARRAY_UCHAR_RW;
    } else if (type.equalsIgnoreCase("scalar_devdouble_ro")) {
      return TYPE_SCALAR_DOUBLE_RO;
    } else if (type.equalsIgnoreCase("scalar_devstring_rw")) {
      return TYPE_SCALAR_STRING_RW;
    } else if (type.equalsIgnoreCase("array_devstring_ro")) {
      return TYPE_ARRAY_STRING_RO;
    } else if (type.equalsIgnoreCase("scalar_devshort_ro")) {
      return TYPE_SCALAR_SHORT_RO;
    } else if (type.equalsIgnoreCase("scalar_devboolean_rw")) {
      return TYPE_SCALAR_BOOLEAN_RW;
    } else if (type.equalsIgnoreCase("scalar_devulong_ro")) {
      return TYPE_SCALAR_ULONG_RO;
    } else if (type.equalsIgnoreCase("array_devulong64_rw")) {
      return TYPE_ARRAY_ULONG64_RW;
    } else if (type.equalsIgnoreCase("array_devencoded_rw")) {
      return TYPE_ARRAY_ENCODED_RW;
    } else if (type.equalsIgnoreCase("scalar_devushort_ro")) {
      return TYPE_SCALAR_USHORT_RO;
    } else if (type.equalsIgnoreCase("array_devshort_ro")) {
      return TYPE_ARRAY_SHORT_RO;
    } else if (type.equalsIgnoreCase("scalar_devstate_ro")) {
      return TYPE_SCALAR_STATE_RO;
    } else if (type.equalsIgnoreCase("scalar_devuchar_rw")) {
      return TYPE_SCALAR_UCHAR_RW;
    } else if (type.equalsIgnoreCase("array_devfloat_rw")) {
      return TYPE_ARRAY_FLOAT_RW;
    } else if (type.equalsIgnoreCase("scalar_devstate_rw")) {
      return TYPE_SCALAR_STATE_RW;
    } else if (type.equalsIgnoreCase("array_devulong_ro")) {
      return TYPE_ARRAY_ULONG_RO;
    } else if (type.equalsIgnoreCase("array_devboolean_ro")) {
      return TYPE_ARRAY_BOOLEAN_RO;
    } else if (type.equalsIgnoreCase("array_devshort_rw")) {
      return TYPE_ARRAY_SHORT_RW;
    } else if (type.equalsIgnoreCase("array_devuchar_ro")) {
      return TYPE_ARRAY_UCHAR_RO;
    } else if (type.equalsIgnoreCase("scalar_devulong_rw")) {
      return TYPE_SCALAR_ULONG_RW;
    } else if (type.equalsIgnoreCase("array_devboolean_rw")) {
      return TYPE_ARRAY_BOOLEAN_RW;
    } else if (type.equalsIgnoreCase("array_devushort_ro")) {
      return TYPE_ARRAY_USHORT_RO;
    } else if (type.equalsIgnoreCase("array_devstate_ro")) {
      return TYPE_ARRAY_STATE_RO;
    } else if (type.equalsIgnoreCase("scalar_devencoded_rw")) {
      return TYPE_SCALAR_ENCODED_RW;
    } else {
      throw new HdbFailed("'" + type + "' : Unknown type");
    }

  }

  public int     type;      // Data type
}
