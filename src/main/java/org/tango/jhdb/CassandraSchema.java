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

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import org.tango.jhdb.data.HdbData;
import org.tango.jhdb.data.HdbDataSet;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Cassandra database access
 */
public class CassandraSchema extends HdbReader {

  public static final String[] DEFAULT_CONTACT_POINTS = {"hdbr1","hdbr2","hdbr3"};
  public static final boolean extraTimestamp = false;

  private Session session;
  private Cluster cluster;

  private final static String[] tableNames = {

      "",
      "att_scalar_devdouble_ro",
      "att_scalar_devdouble_rw",
      "att_array_devdouble_ro",
      "att_array_devdouble_rw",
      "att_scalar_devlong64_ro",
      "att_scalar_devlong64_rw",
      "att_array_devlong64_ro",
      "att_array_devlong64_rw",
      // OLD INT8 type (no longer used)
      "",
      "",
      "",
      "",
      "att_scalar_devstring_ro",
      "att_scalar_devstring_rw",
      "att_array_devstring_ro",
      "att_array_devstring_rw",
      "att_scalar_devfloat_ro",
      "att_scalar_devfloat_rw",
      "att_array_devfloat_ro",
      "att_array_devfloat_rw",
      "att_scalar_devuchar_ro",
      "att_scalar_devuchar_rw",
      "att_array_devuchar_ro",
      "att_array_devuchar_rw",
      "att_scalar_devshort_ro",
      "att_scalar_devshort_rw",
      "att_array_devshort_ro",
      "att_array_devshort_rw",
      "att_scalar_devushort_ro",
      "att_scalar_devushort_rw",
      "att_array_devushort_ro",
      "att_array_devushort_rw",
      "att_scalar_devlong_ro",
      "att_scalar_devlong_rw",
      "att_array_devlong_ro",
      "att_array_devlong_rw",
      "att_scalar_devulong_ro",
      "att_scalar_devulong_rw",
      "att_array_devulong_ro",
      "att_array_devulong_rw",
      "att_scalar_devstate_ro",
      "att_scalar_devstate_rw",
      "att_array_devstate_ro",
      "att_array_devstate_rw",
      "att_scalar_devboolean_ro",
      "att_scalar_devboolean_rw",
      "att_array_devboolean_ro",
      "att_array_devboolean_rw",
      "att_scalar_devencoded_ro",
      "att_scalar_devencoded_rw",
      "att_array_devencoded_ro",
      "att_array_devencoded_rw",
      "att_scalar_devulong64_ro",
      "att_scalar_devulong64_rw",
      "att_array_devulong64_ro",
      "att_array_devulong64_rw"

  };

  // Maximum number of asynchronous call launched simultaneously
  private final static int  MAX_ASYNCH_CALL = 6;

  // Prepared queries for getting data
  private static PreparedStatement[] prepQueries = new PreparedStatement[tableNames.length*2];


  public CassandraSchema(String[] contacts,String db,String user,String passwd) throws HdbFailed {

    // Contact points
    if(contacts==null || contacts.length==0) {

      // Try to get contact points from environment variable
      String str = System.getenv("HDB_CONTACT_POINTS");

      if (str!=null && !str.isEmpty()) {
        StringTokenizer stk = new StringTokenizer(str, ",");
        contacts = new String[stk.countTokens()];
        int i=0;
        while (stk.hasMoreTokens())
          contacts[i++] = stk.nextToken();
      } else {
        contacts = DEFAULT_CONTACT_POINTS;
      }

    }

    if(user==null || user.isEmpty()) {
      user = System.getenv("HDB_USER");
      if (user==null || user.isEmpty())
        user = DEFAULT_DB_USER;
    }

    if(passwd==null || passwd.isEmpty()) {
      passwd = System.getenv("HDB_PASSWORD");
      if (passwd==null || passwd.isEmpty())
        passwd = DEFAULT_DB_PASSWORD;
    }

    // Databse name
    if(db==null || db.isEmpty()) {
      db = System.getenv("HDB_NAME");
      if (db==null || db.isEmpty())
        db = DEFAULT_DB_NAME;
    }

    //  Build cluster from contact points
    try {

      Cluster.Builder builder;

      if(user.equalsIgnoreCase("anonymous"))
        builder = Cluster.builder();
      else
        builder = Cluster.builder().withCredentials(user,passwd);

      for (String contactPoint : contacts)
        builder.addContactPoint(contactPoint);
      cluster = builder.build();

      //  Set protocol
      cluster.getConfiguration()
          .getProtocolOptions()
          .setCompression(ProtocolOptions.Compression.LZ4);

      // Sets the timeout
      //SocketOptions socketOptions = cluster.getConfiguration().getSocketOptions();
      //socketOptions.setConnectTimeoutMillis(30000);
      //socketOptions.setReadTimeoutMillis(30000);

      //  Build session on database
      session = cluster.connect(db);

    } catch (Exception e) {
      throw new HdbFailed(e.getMessage());
    }

    for(int i=0;i<prepQueries.length;i++)
      prepQueries[i] = null;

  }

  public void disconnect() {
    session.close();
    cluster.close();
  }

  private PreparedStatement getPreparedQuery(int type,boolean fullPeriod) throws HdbFailed {

    int statementIdx = (fullPeriod?2*type+1:2*type);

    if(type<0 || type>=tableNames.length)
      throw new HdbFailed("Invalid type code=" + type);

    if( prepQueries[statementIdx]!=null )
      // Query has been already prepared
      return prepQueries[statementIdx];

    boolean isRW = HdbSigInfo.isRWType(type);
    String rwField = isRW?",value_w":"";
    String tableName = tableNames[type];
    if(!tableName.isEmpty()) {

      String query;
      if( fullPeriod ) {

        // Full period query
        if( extraTimestamp ) {
          query = "SELECT data_time,data_time_us,recv_time,recv_time_us,insert_time,insert_time_us,error_desc,quality,value_r"+rwField+
          " FROM " + tableName +
          " WHERE att_conf_id = ?" +
          " AND period = ?";
        } else {
          query = "SELECT data_time,data_time_us,error_desc,quality,value_r"+rwField+
              " FROM " + tableName +
              " WHERE att_conf_id = ?" +
              " AND period = ?";
        }

      } else {

        // Query for a part of the period
        if( extraTimestamp ) {
          query = "SELECT data_time,data_time_us,recv_time,recv_time_us,insert_time,insert_time_us,error_desc,quality,value_r"+rwField+
          " FROM " + tableName +
          " WHERE att_conf_id = ?" +
          " AND period = ?" +
          " AND data_time >= ?" +
          " AND data_time <= ?";
        } else {
          query = "SELECT data_time,data_time_us,error_desc,quality,value_r"+rwField+
              " FROM " + tableName +
              " WHERE att_conf_id = ?" +
              " AND period = ?" +
              " AND data_time >= ?" +
              " AND data_time <= ?";
        }

      }

      prepQueries[statementIdx] = session.prepare(query);

    } else {
      throw new HdbFailed("Invalid request on a not supported type " + HdbSigInfo.typeStr[type]);
    }

    return prepQueries[statementIdx];

  }

  public String getInfo() throws HdbFailed {

    String version =  "Cassandra HDB++ API v" + Hdb.getVersion() + "\n";
    String url = "Cluster:" + session.getCluster().getClusterName() + "\n";
    String ctimeout = "connect Timeout: "+  session.getCluster().getConfiguration().getSocketOptions().getConnectTimeoutMillis() + " ms\n";
    String rtimeout = "Read Timeout: "+  session.getCluster().getConfiguration().getSocketOptions().getReadTimeoutMillis() + " ms";
    return version + url + ctimeout + rtimeout;

  }

  public String[] getAttributeList() throws HdbFailed {

    ArrayList<String> list = new ArrayList<String>();

    String query = "SELECT cs_name,att_name FROM att_conf";

    ResultSet resultSet;
    try {
      resultSet = session.execute(query);
      for (Row row : resultSet) {
        String csName = row.getString("cs_name");
        String attName = row.getString("att_name");
        list.add("tango://"+csName+"/"+attName);
      }
    } catch (DriverException e) {
      throw new HdbFailed(e.getMessage());
    }

    String[] retStr = new String[list.size()];
    for(int i=0;i<retStr.length;i++)
      retStr[i]=list.get(i);

    return retStr;

  }

  private String[] getList(String query) throws HdbFailed {

    ArrayList<String> restStr = new ArrayList<String>();

    ResultSet resultSet;
    try {

      resultSet = session.execute(query);
      for(Row rw:resultSet)
        restStr.add(rw.getString(0));

    } catch (DriverException e) {
      throw new HdbFailed(e.getMessage());
    }

    String[] ret = new String[restStr.size()];
    for(int i=0;i<ret.length;i++)
      ret[i] = restStr.get(i);

    return ret;

  }

  public String[] getHosts() throws HdbFailed {

    return getList("select distinct cs_name from att_conf");

  }

  public String[] getDomains(String host) throws HdbFailed {

    return getList("select domain from domains where cs_name='"+host+"'");

  }

  public String[] getFamilies(String host,String domain) throws HdbFailed {

    return getList("select family from families where cs_name='"+host+
                   "' and domain='" + domain + "'");

  }

  public String[] getMembers(String host,String domain,String family) throws HdbFailed {

    return getList("select member from members where cs_name='"+host+
                   "' and domain='" + domain + "' and family='" + family + "'");

  }

  public String[] getNames(String host,String domain,String family,String member) throws HdbFailed {

    return getList("select name from att_names where cs_name='"+host+
        "' and domain='" + domain + "' and family='" + family + "' and member='" + member + "'");

  }

  public HdbSigInfo getSigInfo(String attName) throws HdbFailed {

    HdbSigInfo ret = prepareSigInfo(attName);

    attName = ret.name.substring(8);
    String[] fields = attName.split("/");
    if(fields.length!=5)
      throw new HdbFailed("Invalid attribute name syntax (eg:tango://hostname:port/domain/family/member/name)");

    String csName = fields[0];
    String shortAttName = fields[1] + "/" + fields[2] + "/" + fields[3] + "/" + fields[4];

    String query = "SELECT att_conf_id,data_type FROM att_conf WHERE cs_name='"+csName+
                   "' AND att_name='" + shortAttName + "'";

    ResultSet resultSet;
    try {

      resultSet = session.execute(query);
      Row row = resultSet.one();

      if(row==null)
       throw new HdbFailed("Signal not found");

      ret.sigId = row.getUUID("att_conf_id").toString();
      ret.type = HdbSigInfo.typeFromName(row.getString("data_type"));

    } catch (DriverException e) {
      throw new HdbFailed(e.getMessage());
    }

    return ret;

  }

  public HdbDataSet findErrors(String attName,
                               String start_date,
                               String stop_date) throws HdbFailed {
    throw new HdbFailed("Not implemented");
  }

  public  HdbSigParam getLastParam(HdbSigInfo sigInfo) throws HdbFailed {

    String query = "SELECT recv_time,recv_time_us,label,unit,standard_unit,display_unit,format,"+
        "archive_rel_change,archive_abs_change,archive_period,description" +
        " FROM att_parameter " +
        " WHERE att_conf_id=" + UUID.fromString(sigInfo.sigId) +
        " ORDER BY recv_time desc limit 1;";

    HdbSigParam ret = new HdbSigParam();

    ResultSet resultSet;
    try {

      resultSet = session.execute(query);
      Row rw = resultSet.one();

      if(rw!=null) {

        ret.recvTime = timeValue(rw.getTimestamp(0), rw.getInt(1));
        ret.insertTime = 0;
        ret.label = rw.getString(2);
        ret.unit = rw.getString(3);
        try {
          ret.standard_unit = Double.parseDouble(rw.getString(4));
        } catch (NumberFormatException e) {
          ret.standard_unit = 1.0;
        }
        try {
          ret.display_unit = Double.parseDouble(rw.getString(5));
        } catch (NumberFormatException e) {
          ret.display_unit = 1.0;
        }
        ret.format = rw.getString(6);
        ret.archive_rel_change = rw.getString(7);
        ret.archive_abs_change = rw.getString(8);
        ret.archive_period = rw.getString(9);
        ret.description = rw.getString(10);

      } else {
        throw new HdbFailed("Cannot get parameter for " + sigInfo.name);
      }

    } catch (DriverException e) {
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

    String query = "SELECT recv_time,recv_time_us,label,unit,standard_unit,display_unit,format,"+
        "archive_rel_change,archive_abs_change,archive_period,description" +
        " FROM att_parameter " +
        " WHERE att_conf_id=" + UUID.fromString(sigInfo.sigId) +
        " AND recv_time>='" + toDBDate(start_date) + "'" +
        " AND recv_time<='" + toDBDate(stop_date) + "'";

    ArrayList<HdbSigParam> ret = new ArrayList<HdbSigParam>();

    ResultSet resultSet;
    try {

      resultSet = session.execute(query);

      for(Row rw:resultSet) {

        HdbSigParam hd = new HdbSigParam();
        hd.recvTime = timeValue(rw.getTimestamp(0), rw.getInt(1));
        hd.insertTime = 0;
        hd.label = rw.getString(2);
        hd.unit = rw.getString(3);
        try {
          hd.standard_unit = Double.parseDouble(rw.getString(4));
        } catch (NumberFormatException e) {
          hd.standard_unit = 1.0;
        }
        try {
          hd.display_unit = Double.parseDouble(rw.getString(5));
        } catch (NumberFormatException e) {
          hd.display_unit = 1.0;
        }
        hd.format = rw.getString(6);
        hd.archive_rel_change = rw.getString(7);
        hd.archive_abs_change = rw.getString(8);
        hd.archive_period = rw.getString(9);
        hd.description = rw.getString(10);

        ret.add(hd);

      }

    } catch (DriverException e) {
      throw new HdbFailed("Failed to get parameter history: "+e.getMessage());
    }

    return ret;

  }

  HdbDataSet getDataFromDB(HdbSigInfo sigInfo,
                           String start_date,
                           String stop_date) throws HdbFailed {

    String errorStr = null;

    if(sigInfo==null)
      throw new HdbFailed("sigInfo input parameters is null");

    checkDates(start_date,stop_date);

    boolean isRW = HdbSigInfo.isRWType(sigInfo.type);

    // Compute periods according to HDB partitioning
    ArrayList<Period> periods = Period.getPeriods(start_date,stop_date);
    int nbPeriod = periods.size();

    ArrayList<HdbData> ret = new ArrayList<HdbData>();
    ArrayList<Object> value = new ArrayList<Object>();
    ArrayList<Object> wvalue = null;
    if(isRW) wvalue = new ArrayList<Object>();

    int fetchSize = 5000;

    for(int i=0;i<nbPeriod;i+=MAX_ASYNCH_CALL) {

      ArrayList<ResultSetFuture> resultSetFutures = new ArrayList<ResultSetFuture>();

      int j;
      for(j=0;j<MAX_ASYNCH_CALL && j+i<nbPeriod;j++) {

        // Get prepared statements
        Period p = periods.get(i+j);

        BoundStatement boundStatement;

        if(p.isFull) {
          boundStatement = getPreparedQuery(sigInfo.type,p.isFull).bind(
              UUID.fromString(sigInfo.sigId),
              p.partitionDate);
        } else {
          boundStatement = getPreparedQuery(sigInfo.type,p.isFull).bind(
              UUID.fromString(sigInfo.sigId),
              p.partitionDate,
              p.start,
              p.end);
        }

        // Launch asynchronous calls
        boundStatement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        boundStatement.setFetchSize(fetchSize);
        resultSetFutures.add(session.executeAsync(boundStatement));
      }

      // Wait end of result
      ArrayList<ResultSet> resultSets = new ArrayList<ResultSet>();

      try {
        for(ResultSetFuture sf: resultSetFutures) {
          try {
            resultSets.add(sf.getUninterruptibly());
          } catch (QueryExecutionException e2) {
            // We may ignore this to work around tombstones.
            errorStr = "Error (QueryExecution): " + e2.getMessage();
          }
        }
      } catch (NoHostAvailableException e1) {
        throw new HdbFailed("Error (NoHostAvailable): " + e1.getMessage());
      } catch (QueryValidationException e3) {
        throw new HdbFailed("Error (QueryValidation): " + e3.getMessage());
      }

      if(hasProgressListener())
        fireProgressListener((double)(i+j)/nbPeriod);

      // Build HdbDataSet
      try {

        for (ResultSet rs : resultSets) {
          int remainingInPage = rs.getAvailableWithoutFetching();
          for (Row rw : rs) {

            HdbData hd = HdbData.createData(sigInfo.type);

            switch (sigInfo.type) {

              case HdbSigInfo.TYPE_SCALAR_BOOLEAN_RO:
              case HdbSigInfo.TYPE_SCALAR_BOOLEAN_RW:
                if(extraTimestamp) {
                  setValue(value, rw.getBool(8));
                  if (isRW) setValue(wvalue, rw.getBool(9));
                } else {
                  setValue(value, rw.getBool(4));
                  if (isRW) setValue(wvalue, rw.getBool(5));
                }
                break;

              case HdbSigInfo.TYPE_ARRAY_BOOLEAN_RO:
              case HdbSigInfo.TYPE_ARRAY_BOOLEAN_RW:
                if(extraTimestamp) {
                  setValueBoolean(value, rw.getList(8, Boolean.class));
                  if (isRW) setValueBoolean(wvalue, rw.getList(9, Boolean.class));
                } else {
                  setValueBoolean(value, rw.getList(4, Boolean.class));
                  if (isRW) setValueBoolean(wvalue, rw.getList(5, Boolean.class));
                }
                break;

              case HdbSigInfo.TYPE_SCALAR_SHORT_RO:
              case HdbSigInfo.TYPE_SCALAR_SHORT_RW:
              case HdbSigInfo.TYPE_SCALAR_UCHAR_RO:
              case HdbSigInfo.TYPE_SCALAR_UCHAR_RW:
                if(extraTimestamp) {
                  setValue(value, (short) rw.getInt(8));
                  if (isRW) setValue(wvalue, (short) rw.getInt(9));
                } else {
                  setValue(value, (short) rw.getInt(4));
                  if (isRW) setValue(wvalue, (short) rw.getInt(5));
                }
                break;

              case HdbSigInfo.TYPE_ARRAY_SHORT_RO:
              case HdbSigInfo.TYPE_ARRAY_SHORT_RW:
              case HdbSigInfo.TYPE_ARRAY_UCHAR_RO:
              case HdbSigInfo.TYPE_ARRAY_UCHAR_RW:
                if(extraTimestamp) {
                  setValueShort(value, rw.getList(8, Integer.class));
                  if (isRW) setValueShort(wvalue, rw.getList(9, Integer.class));
                } else {
                  setValueShort(value, rw.getList(4, Integer.class));
                  if (isRW) setValueShort(wvalue, rw.getList(5, Integer.class));
                }
                break;

              case HdbSigInfo.TYPE_SCALAR_LONG_RO:
              case HdbSigInfo.TYPE_SCALAR_LONG_RW:
              case HdbSigInfo.TYPE_SCALAR_USHORT_RO:
              case HdbSigInfo.TYPE_SCALAR_USHORT_RW:
              case HdbSigInfo.TYPE_SCALAR_STATE_RO:
              case HdbSigInfo.TYPE_SCALAR_STATE_RW:
                if(extraTimestamp) {
                  setValue(value, rw.getInt(8));
                  if (isRW) setValue(wvalue, rw.getInt(9));
                } else {
                  setValue(value, rw.getInt(4));
                  if (isRW) setValue(wvalue, rw.getInt(5));
                }
                break;

              case HdbSigInfo.TYPE_ARRAY_LONG_RO:
              case HdbSigInfo.TYPE_ARRAY_LONG_RW:
              case HdbSigInfo.TYPE_ARRAY_USHORT_RO:
              case HdbSigInfo.TYPE_ARRAY_USHORT_RW:
              case HdbSigInfo.TYPE_ARRAY_STATE_RO:
              case HdbSigInfo.TYPE_ARRAY_STATE_RW:
                if(extraTimestamp) {
                  setValueInteger(value, rw.getList(8, Integer.class));
                  if (isRW) setValueInteger(wvalue, rw.getList(9, Integer.class));
                } else {
                  setValueInteger(value, rw.getList(4, Integer.class));
                  if (isRW) setValueInteger(wvalue, rw.getList(5, Integer.class));
                }
                break;

              case HdbSigInfo.TYPE_SCALAR_LONG64_RO:
              case HdbSigInfo.TYPE_SCALAR_LONG64_RW:
              case HdbSigInfo.TYPE_SCALAR_ULONG_RO:
              case HdbSigInfo.TYPE_SCALAR_ULONG_RW:
                if(extraTimestamp) {
                  setValue(value, rw.getLong(8));
                  if (isRW) setValue(wvalue, rw.getLong(9));
                } else {
                  setValue(value, rw.getLong(4));
                  if (isRW) setValue(wvalue, rw.getLong(5));
                }
                break;

              case HdbSigInfo.TYPE_ARRAY_LONG64_RO:
              case HdbSigInfo.TYPE_ARRAY_LONG64_RW:
              case HdbSigInfo.TYPE_ARRAY_ULONG_RO:
              case HdbSigInfo.TYPE_ARRAY_ULONG_RW:
                if(extraTimestamp) {
                  setValueLong(value, rw.getList(8, Long.class));
                  if (isRW) setValueLong(wvalue, rw.getList(9, Long.class));
                } else {
                  setValueLong(value, rw.getList(4, Long.class));
                  if (isRW) setValueLong(wvalue, rw.getList(5, Long.class));
                }
                break;

              case HdbSigInfo.TYPE_SCALAR_DOUBLE_RO:
              case HdbSigInfo.TYPE_SCALAR_DOUBLE_RW:
                if(extraTimestamp) {
                  setValue(value, rw.getDouble(8));
                  if (isRW) setValue(wvalue, rw.getDouble(9));
                } else {
                  setValue(value, rw.getDouble(4));
                  if (isRW) setValue(wvalue, rw.getDouble(5));
                }
                break;

              case HdbSigInfo.TYPE_ARRAY_DOUBLE_RO:
              case HdbSigInfo.TYPE_ARRAY_DOUBLE_RW:
                if(extraTimestamp) {
                  setValueDouble(value, rw.getList(8, Double.class));
                  if (isRW) setValueDouble(wvalue, rw.getList(9, Double.class));
                } else {
                  setValueDouble(value, rw.getList(4, Double.class));
                  if (isRW) setValueDouble(wvalue, rw.getList(5, Double.class));
                }
                break;

              case HdbSigInfo.TYPE_SCALAR_FLOAT_RO:
              case HdbSigInfo.TYPE_SCALAR_FLOAT_RW:
                if(extraTimestamp) {
                  setValue(value, rw.getFloat(8));
                  if (isRW) setValue(wvalue, rw.getFloat(9));
                } else {
                  setValue(value, rw.getFloat(4));
                  if (isRW) setValue(wvalue, rw.getFloat(5));
                }
                break;

              case HdbSigInfo.TYPE_ARRAY_FLOAT_RO:
              case HdbSigInfo.TYPE_ARRAY_FLOAT_RW:
                if(extraTimestamp) {
                  setValueFloat(value, rw.getList(8, Float.class));
                  if (isRW) setValueFloat(wvalue, rw.getList(9, Float.class));
                } else {
                  setValueFloat(value, rw.getList(4, Float.class));
                  if (isRW) setValueFloat(wvalue, rw.getList(5, Float.class));
                }
                break;

              case HdbSigInfo.TYPE_SCALAR_STRING_RO:
              case HdbSigInfo.TYPE_SCALAR_STRING_RW:
                if(extraTimestamp) {
                  setValue(value, rw.getString(8));
                  if (isRW) setValue(wvalue, rw.getString(9));
                } else {
                  setValue(value, rw.getString(4));
                  if (isRW) setValue(wvalue, rw.getString(5));
                }
                break;

              case HdbSigInfo.TYPE_ARRAY_STRING_RO:
              case HdbSigInfo.TYPE_ARRAY_STRING_RW:
                if(extraTimestamp) {
                  setValueString(value, rw.getList(8, String.class));
                  if (isRW) setValueString(wvalue, rw.getList(9, String.class));
                } else {
                  setValueString(value, rw.getList(4, String.class));
                  if (isRW) setValueString(wvalue, rw.getList(5, String.class));
                }
                break;

            }

            if(extraTimestamp) {
              hd.parse(
                timeValue(rw.getTimestamp(0), rw.getInt(1)), //Tango timestamp
                timeValue(rw.getTimestamp(2), rw.getInt(3)), //Event receive timestamp
                timeValue(rw.getTimestamp(4), rw.getInt(5)), //Recording timestamp
                rw.getString(6),                   // Error string
                rw.getInt(7),                      // Quality value
                value,                             // Read value
                wvalue                             // Write value
              );
            } else {
              hd.parse(
                  timeValue(rw.getTimestamp(0), rw.getInt(1)), //Tango timestamp
                  0,                                           //Event receive timestamp
                  0,                                           //Recording timestamp
                  rw.getString(2),                   // Error string
                  rw.getInt(3),                      // Quality value
                  value,                             // Read value
                  wvalue                             // Write value
              );
            }
            ret.add(hd);
            remainingInPage--;
            if((remainingInPage == 100) && !rs.isFullyFetched())
              rs.fetchMoreResults();
          }
        }

      } catch (DriverException e) {
        throw new HdbFailed("Failed to get data: " + e.getMessage());
      }

    }

    if(ret.size()==0 && errorStr!=null)
      throw new HdbFailed(errorStr);

    return new HdbDataSet(ret);

  }

  private void setValue(ArrayList<Object> value,double d) {
    value.clear();
    value.add(d);
  }

  private void setValueDouble(ArrayList<Object> value,List<Double> d) {
    value.clear();
    value.addAll(d);
  }

  private void setValue(ArrayList<Object> value,float d) {
    value.clear();
    value.add(d);
  }

  private void setValueFloat(ArrayList<Object> value,List<Float> d) {
    value.clear();
    value.addAll(d);
  }

  private void setValue(ArrayList<Object> value,boolean b) {
    value.clear();
    value.add(b);
  }

  private void setValueBoolean(ArrayList<Object> value,List<Boolean> b) {
    value.clear();
    value.addAll(b);
  }

  private void setValue(ArrayList<Object> value,int i) {
    value.clear();
    value.add(i);
  }

  private void setValueInteger(ArrayList<Object> value,List<Integer> d) {
    value.clear();
    value.addAll(d);
  }

  private void setValue(ArrayList<Object> value,long l) {
    value.clear();
    value.add(l);
  }

  private void setValueLong(ArrayList<Object> value,List<Long> l) {
    value.clear();
    value.addAll(l);
  }

  private void setValue(ArrayList<Object> value,short s) {
    value.clear();
    value.add(s);
  }

  private void setValueShort(ArrayList<Object> value,List<Integer> d) {
    value.clear();
    for(int i=0;i<d.size();i++)
      value.add(d.get(i).shortValue());
  }

  private void setValue(ArrayList<Object> value,String s) {
    value.clear();
    value.add(s);
  }

  private void setValueString(ArrayList<Object> value,List<String> d) {
    value.clear();
    value.addAll(d);
  }

  //private long timeValue(LocalDate t,int us) {
  private long timeValue(Date t,int us) {

    //long ret = t.getMillisSinceEpoch();
    long ret = t.getTime();
    ret = (ret / 1000) * 1000000;
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