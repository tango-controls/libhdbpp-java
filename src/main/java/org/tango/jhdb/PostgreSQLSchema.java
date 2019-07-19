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

import org.tango.jhdb.data.HdbData;
import org.tango.jhdb.data.HdbDataSet;

import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

/**
 * PostgreSQL database access
 */
public class PostgreSQLSchema extends HdbReader {

  public static final String DEFAULT_DB_URL_PREFIX = "jdbc:postgresql://";
  public static final int    DEFAULT_DB_PORT = 5432;

  // Notify every PROGRESS_NBROW rows
  private final static int PROGRESS_NBROW =10000;
  private Connection connection;
  private AttributeBrowser browser=null;
  private String dbURL;

  /**
   * Connects to a MySQL HDB.
   * @param host MySQL hostname
   * @param db Database name (default is "hdb")
   * @param user MySQL user name
   * @param passwd MySQL user password
   * @param port MySQL databse port (pass 0 for default Mysql port)
   * @throws HdbFailed in case of failure
   */

  public PostgreSQLSchema(String host,String db,String user,String passwd,short port) throws HdbFailed {

    if(host==null || host.isEmpty()) {
      host = System.getenv("HDB_POSTGRESQL_HOST");
      if (host==null || host.isEmpty()) {
        host = System.getProperty("HDB_POSTGRESQL_HOST");
        if (host==null || host.isEmpty())
          throw new HdbFailed("host input parameter cannot be null if HDB_POSTGRESQL_HOST variable is not defined");
      }
    }

    if(user==null || user.isEmpty()) {
      user = System.getenv("HDB_USER");
      if (user==null || user.isEmpty()) {
        user = System.getProperty("HDB_USER");
        if (user==null || user.isEmpty())
          user = DEFAULT_DB_USER;
      }
    }

    if(passwd==null || passwd.isEmpty()) {
      passwd = System.getenv("HDB_PASSWORD");
      if (passwd==null || passwd.isEmpty()) {
        passwd = System.getProperty("HDB_PASSWORD");
        if (passwd==null || passwd.isEmpty())
          passwd = DEFAULT_DB_PASSWORD;
      }
    }

    if(db==null || db.isEmpty()) {
      db = System.getenv("HDB_NAME");
      if (db==null || db.isEmpty()) {
        db = System.getProperty("HDB_NAME");
        if (db==null || db.isEmpty())
          db = DEFAULT_DB_NAME;
      }
    }

    if(port==0) {
      String pStr = System.getenv("HDB_POSTGRESQL_PORT");
      if(pStr==null || passwd.isEmpty())
        port = DEFAULT_DB_PORT;
      else {
        try {
          port = (short)Integer.parseInt(pStr);
        } catch (NumberFormatException e) {
          throw new HdbFailed("Invalid HDB_POSTGRESQL_PORT variable " + e.getMessage());
        }
      }
    }

    try {

      Properties connectProperties = new Properties();
      connectProperties.setProperty("user", user);
      connectProperties.setProperty("password", passwd);
      connectProperties.setProperty("loginTimeout", Integer.toString(10));
      connectProperties.setProperty("tcpKeepAlive ", "true"); //Enable TCP keep-alive probe

      // URL example: jdbc:postgresql://host:port/database
      dbURL = DEFAULT_DB_URL_PREFIX + host + ":" +
          Integer.toString(port) + "/" + db;

      connection = DriverManager.getConnection(dbURL, connectProperties);

    } catch (SQLException e) {
      throw new HdbFailed("Failed to connect to PostgreSQL: "+e.getMessage());
    }

  }

  public void disconnect () {
    try {
      connection.close();
    } catch (SQLException e) {
      System.out.println("Warning closing connection : " + e.getMessage());
    }
  }

  public String getInfo() throws HdbFailed {

    String version =  "PostgreSQL HDB++ API v" + Hdb.getVersion() + "\n";
    String url = "URL:" + dbURL;
    return version + url;

  }

  public String[] getAttributeList() throws HdbFailed {

    ArrayList<String> list = new ArrayList<String>();

    String query = "SELECT att_name FROM att_conf ORDER BY att_name";

    try {
      Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
      ResultSet resultSet = statement.executeQuery(query);
      while (resultSet.next())
        list.add(resultSet.getString(1));
      statement.close();
    } catch (SQLException e) {
      throw new HdbFailed("Failed to retrieve attribute list: "+e.getMessage());
    }

    String[] retStr = new String[list.size()];
    for(int i=0;i<retStr.length;i++)
      retStr[i]=list.get(i);

    return retStr;

  }

  private void constructBrowser() throws HdbFailed {
    if( browser==null )
      browser = AttributeBrowser.constructBrowser(this);
  }

  public String[] getHosts() throws HdbFailed {
    constructBrowser();
    return browser.getHosts();
  }

  public String[] getDomains(String host) throws HdbFailed {
    constructBrowser();
    return browser.getDomains(host);
  }

  public String[] getFamilies(String host,String domain) throws HdbFailed {
    constructBrowser();
    return browser.getFamilies(host, domain);
  }

  public String[] getMembers(String host,String domain,String family) throws HdbFailed {
    constructBrowser();
    return browser.getMembers(host, domain, family);
  }

  public String[] getNames(String host,String domain,String family,String member) throws HdbFailed {
    constructBrowser();
    return browser.getNames(host, domain, family, member);
  }

  public HdbSigInfo getSigInfo(String attName) throws HdbFailed {

    HdbSigInfo ret = prepareSigInfo(attName);
    attName = ret.name;

    String query = "SELECT att_conf_id,table_name,att_conf_write_id FROM att_conf WHERE att_name='" + attName + "'";

    try {
      Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
      ResultSet resultSet = statement.executeQuery(query);
      if(resultSet.next()) {
        ret.sigId = resultSet.getString(1);
        ret.tableName = resultSet.getString(2);
        ret.isWO = false;
        int rw = resultSet.getInt(3);
        String typeName = ret.tableName.substring(4); // remove "att_"
        switch(rw) {
          case 1: // RO
            typeName += "_ro";
            break;
          case 3: // WO
            typeName += "_rw";
            ret.isWO = true;
            break;
          default: // RW
            typeName += "_rw";
            break;
        }
        ret.type = HdbSigInfo.typeFromName(typeName);
      } else {
        throw new HdbFailed("Signal not found");
      }
      statement.close();
    } catch (SQLException e) {
      throw new HdbFailed("Failed to retrieve signal id: "+e.getMessage());
    }

    return ret;

  }

  HdbDataSet getDataFromDB(HdbSigInfo sigInfo,
                           String start_date,
                           String stop_date) throws HdbFailed {

    if (sigInfo == null)
      throw new HdbFailed("sigInfo input parameters is null");

    checkDates(start_date, stop_date);

    boolean isRW = HdbSigInfo.isRWType(sigInfo.type);
    boolean isWO = sigInfo.isWO;

    String query;
    int queryCount = 0;

    if (hasProgressListener()) {

      // Get a count of the request
      query = "SELECT count(*) FROM " + sigInfo.tableName +
          " WHERE att_conf_id='" + sigInfo.type + "'" +
          " AND data_time>='" + toDBDate(start_date) + "'" +
          " AND data_time<='" + toDBDate(stop_date) + "'";

      try {

        Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

        ResultSet rs = statement.executeQuery(query);
        rs.next();
        queryCount = rs.getInt(1);
        statement.close();

      } catch (SQLException e) {
        throw new HdbFailed("Failed to get data: " + e.getMessage());
      }

    }

    // Fetch data

    String rwField = isRW ? ",value_w" : "";
    query = "SELECT data_time,att_error_desc.error_desc as error_desc,quality,value_r" + rwField +
        " FROM " + sigInfo.tableName +
        " left outer join att_error_desc on " + sigInfo.tableName + ".att_error_desc_id = att_error_desc.att_error_desc_id" +
        " WHERE att_conf_id='" + sigInfo.sigId + "'" +
        " AND data_time>='" + toDBDate(start_date) + "'" +
        " AND data_time<='" + toDBDate(stop_date) + "'" +
        " ORDER BY data_time ASC";

    ArrayList<HdbData> ret = new ArrayList<HdbData>();
    ArrayList<Object> value = new ArrayList<Object>();
    ArrayList<Object> wvalue = null;
    if(isRW) wvalue = new ArrayList<Object>();

    try {

      long dTime = 0;
      String errorMsg = null;
      int quality = 0;
      int nbRow = 0;

      Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      statement.setFetchSize(0);
      ResultSet rs = statement.executeQuery(query);
      while (rs.next()) {

        dTime = timeValue(rs.getTimestamp(1));
        errorMsg = rs.getString(2);
        quality = rs.getInt(3);
        value.clear();
        if(isRW)
          wvalue.clear();

        switch (sigInfo.type) {

          case HdbSigInfo.TYPE_SCALAR_BOOLEAN_RO:
          case HdbSigInfo.TYPE_SCALAR_BOOLEAN_RW:
            if(!isWO) value.add(rs.getBoolean(4));
            if(isRW) wvalue.add(rs.getBoolean(5));
            break;

          case HdbSigInfo.TYPE_ARRAY_BOOLEAN_RO:
          case HdbSigInfo.TYPE_ARRAY_BOOLEAN_RW:
            if(!isWO) convertArray(value, rs.getArray(4));
            if(isRW) convertArray(wvalue, rs.getArray(5));
            break;

          case HdbSigInfo.TYPE_SCALAR_SHORT_RO:
          case HdbSigInfo.TYPE_SCALAR_SHORT_RW:
          case HdbSigInfo.TYPE_SCALAR_UCHAR_RO:
          case HdbSigInfo.TYPE_SCALAR_UCHAR_RW:
          case HdbSigInfo.TYPE_SCALAR_LONG_RO:
          case HdbSigInfo.TYPE_SCALAR_LONG_RW:
          case HdbSigInfo.TYPE_SCALAR_USHORT_RO:
          case HdbSigInfo.TYPE_SCALAR_USHORT_RW:
          case HdbSigInfo.TYPE_SCALAR_STATE_RO:
          case HdbSigInfo.TYPE_SCALAR_STATE_RW:
          case HdbSigInfo.TYPE_SCALAR_LONG64_RO:
          case HdbSigInfo.TYPE_SCALAR_LONG64_RW:
          case HdbSigInfo.TYPE_SCALAR_ULONG_RO:
          case HdbSigInfo.TYPE_SCALAR_ULONG_RW:
            if(!isWO) value.add(rs.getLong(4));
            if(isRW) wvalue.add(rs.getLong(5));
            break;

          case HdbSigInfo.TYPE_ARRAY_SHORT_RO:
          case HdbSigInfo.TYPE_ARRAY_SHORT_RW:
          case HdbSigInfo.TYPE_ARRAY_UCHAR_RO:
          case HdbSigInfo.TYPE_ARRAY_UCHAR_RW:
          case HdbSigInfo.TYPE_ARRAY_LONG_RO:
          case HdbSigInfo.TYPE_ARRAY_LONG_RW:
          case HdbSigInfo.TYPE_ARRAY_USHORT_RO:
          case HdbSigInfo.TYPE_ARRAY_USHORT_RW:
          case HdbSigInfo.TYPE_ARRAY_STATE_RO:
          case HdbSigInfo.TYPE_ARRAY_STATE_RW:
          case HdbSigInfo.TYPE_ARRAY_LONG64_RO:
          case HdbSigInfo.TYPE_ARRAY_LONG64_RW:
          case HdbSigInfo.TYPE_ARRAY_ULONG_RO:
          case HdbSigInfo.TYPE_ARRAY_ULONG_RW:
            if(!isWO) convertArray(value, rs.getArray(4));
            if(isRW) convertArray(wvalue, rs.getArray(5));
            break;

          case HdbSigInfo.TYPE_SCALAR_DOUBLE_RO:
          case HdbSigInfo.TYPE_SCALAR_DOUBLE_RW:
            if(!isWO) value.add(rs.getDouble(4));
            if(isRW) wvalue.add(rs.getDouble(5));
            break;

          case HdbSigInfo.TYPE_ARRAY_DOUBLE_RO:
          case HdbSigInfo.TYPE_ARRAY_DOUBLE_RW:
            if(!isWO) convertArray(value, rs.getArray(4));
            if(isRW) convertArray(wvalue, rs.getArray(5));
            break;

          case HdbSigInfo.TYPE_SCALAR_FLOAT_RO:
          case HdbSigInfo.TYPE_SCALAR_FLOAT_RW:
            if(!isWO) value.add(rs.getFloat(4));
            if(isRW) wvalue.add(rs.getFloat(5));
            break;

          case HdbSigInfo.TYPE_ARRAY_FLOAT_RO:
          case HdbSigInfo.TYPE_ARRAY_FLOAT_RW:
            if(!isWO) convertArray(value, rs.getArray(4));
            if(isRW) convertArray(wvalue, rs.getArray(5));
            break;

          case HdbSigInfo.TYPE_SCALAR_STRING_RO:
          case HdbSigInfo.TYPE_SCALAR_STRING_RW:
            if(!isWO) value.add(rs.getString(4));
            if(isRW) wvalue.add(rs.getString(5));
            break;

          case HdbSigInfo.TYPE_ARRAY_STRING_RO:
          case HdbSigInfo.TYPE_ARRAY_STRING_RW:
            if(!isWO) convertArray(value, rs.getArray(4));
            if(isRW) convertArray(wvalue, rs.getArray(5));
            break;


        }

        // Write only attribute, copy write data to read data
        if(isWO) value.addAll(wvalue);

        HdbData hd = HdbData.createData(sigInfo.type);

        hd.parse(
            dTime,     //Tango timestamp
            0,         // Event receive timestamp
            0,         // Recording timestamp
            errorMsg,  // Error string
            quality,   // Quality value
            value, // Read value
            wvalue // Write value
        );
        ret.add(hd);

        if (hasProgressListener() && (nbRow % PROGRESS_NBROW == 0))
          fireProgressListener((double) nbRow / (double) queryCount);

        nbRow++;

      }

      statement.close();

    } catch (SQLException e) {
      throw new HdbFailed("Failed to get data: " + e.getMessage());
    }

    return new HdbDataSet(ret);


  }

  public  HdbSigParam getLastParam(HdbSigInfo sigInfo) throws HdbFailed {

    String query = "SELECT recv_time,label,unit,standard_unit,display_unit,format,"+
        "archive_rel_change,archive_abs_change,archive_period,description" +
        " FROM att_parameter " +
        " WHERE att_conf_id='" + sigInfo.sigId + "'" +
        " ORDER BY recv_time DESC limit 1";

    HdbSigParam ret = new HdbSigParam();

    try {

      Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
      ResultSet rs = statement.executeQuery(query);
      if(rs.next()) {

        ret.recvTime = timeValue(rs.getTimestamp(1));
        ret.insertTime = 0;
        ret.label = rs.getString(2);
        ret.unit = rs.getString(3);
        try {
          ret.standard_unit = Double.parseDouble(rs.getString(4));
        } catch (NumberFormatException e) {
          ret.standard_unit = 1.0;
        }
        try {
          ret.display_unit = Double.parseDouble( rs.getString(5));
        } catch (NumberFormatException e) {
          ret.display_unit = 1.0;
        }
        ret.format = rs.getString(6);
        ret.archive_rel_change = rs.getString(7);
        ret.archive_abs_change = rs.getString(8);
        ret.archive_period = rs.getString(9);
        ret.description = rs.getString(10);

      } else {
        throw new HdbFailed("Cannot get parameter for " + sigInfo.name);
      }

      statement.close();

    } catch (SQLException e) {
      throw new HdbFailed("Failed to get parameter history: "+e.getMessage());
    }

    return ret;

  }

  public ArrayList<HdbSigParam> getParams(String attName,
                                          String start_date,
                                          String stop_date) throws HdbFailed {
    HdbSigInfo sigInfo = getSigInfo(attName);
    return getParams(sigInfo,start_date,stop_date);
  }

  public ArrayList<HdbSigParam> getParams(HdbSigInfo sigInfo,
                                          String start_date,
                                          String stop_date) throws HdbFailed {

    checkDates(start_date,stop_date);

    String query = "SELECT recv_time,label,unit,standard_unit,display_unit,format,"+
        "archive_rel_change,archive_abs_change,archive_period,description" +
        " FROM att_parameter " +
        " WHERE att_conf_id='" + sigInfo.sigId + "'" +
        " AND recv_time>='" + toDBDate(start_date) + "'" +
        " AND recv_time<='" + toDBDate(stop_date) + "'" +
        " ORDER BY recv_time ASC";

    ArrayList<HdbSigParam> ret = new ArrayList<HdbSigParam>();

    try {

      Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
      ResultSet rs = statement.executeQuery(query);
      while(rs.next()) {

        HdbSigParam hd = new HdbSigParam();
        hd.recvTime = timeValue(rs.getTimestamp(1));
        hd.insertTime = 0;
        hd.label = rs.getString(2);
        hd.unit = rs.getString(3);
        try {
          hd.standard_unit = Double.parseDouble(rs.getString(4));
        } catch (NumberFormatException e) {
          hd.standard_unit = 1.0;
        }
        try {
          hd.display_unit = Double.parseDouble( rs.getString(5));
        } catch (NumberFormatException e) {
          hd.display_unit = 1.0;
        }
        hd.format = rs.getString(6);
        hd.archive_rel_change = rs.getString(7);
        hd.archive_abs_change = rs.getString(8);
        hd.archive_period = rs.getString(9);
        hd.description = rs.getString(10);

        ret.add(hd);

      }

      statement.close();

    } catch (SQLException e) {
      throw new HdbFailed("Failed to get parameter history: "+e.getMessage());
    }

    return ret;
  }

  public HdbDataSet findErrors(String attName,
                               String start_date,
                               String stop_date) throws HdbFailed {
    throw new HdbFailed("Not implemented");
  }

  // ---------------------------------------------------------------------------------------

  private void convertArray(ArrayList<Object> v,Array a) throws SQLException {

    if(a!=null) {
      Object[] objects = (Object[]) a.getArray();
      for(int i=0;i<objects.length;i++)
        v.add(objects[i]);
    }

  }

  private long timeValue(Timestamp ts) {

    if(ts==null)
      return 0;

    long ret = ts.getTime();
    ret = ret / 1000;
    long us = ts.getNanos();
    us = us / 1000;
    ret = ret * 1000000;
    ret += us;
    return ret;

  }

  private String toDBDate(String date) {

    // In:   09/07/2015 12:00:00
    // Out:  2015-07-09 12:00:00
    return date.substring(6,10) + "-" + date.substring(3,5) + "-" +
        date.substring(0,2) + " " + date.substring(11,19);

  }

}
