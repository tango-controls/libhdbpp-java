package org.tango.jhdb;

import fr.esrf.Tango.DevFailed;
import fr.esrf.TangoApi.*;
import org.tango.jhdb.data.*;
import tacoHdb.common.*;
import tacoHdb.config.HdbSignal;
import tacoHdb.extract.sig.*;

import javax.management.Attribute;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Vector;

/**
 * Interface for Oracle HDB
 */

public class OracleSchema extends HdbReader {

  IDbConnection connection;
  HdbBrowser browser;

  public OracleSchema(int type) throws HdbFailed {

    if( type==Hdb.HDB_ORACLE ) {
      connection = HdbConnection.getInstance();
    } else if ( type==Hdb.HDB_ORACLE_ARCH ) {
      connection = ArchConnection.getInstance();
    } else if ( type==Hdb.HDB_ORACLE_OLD_ARCH ) {
      connection = OldarchConnection.getInstance();
    } else {
      throw new HdbFailed("Wrong Oracle type");
    }

    // Connect to oracle HDB
    try {
      connection.connect();
    } catch( HdbException e ) {
      throw new HdbFailed(buildMessage(e));
    }
    browser = new HdbBrowser(connection);

  }

  public void disconnect () {

    try {
      connection.getDb().close();
    } catch (SQLException e) {
      System.out.println("Warning closing connection : " + e.getMessage());
    } catch (HdbException e) {
      System.out.println("Warning closing connection : " + buildMessage(e));
    }

  }

  public String getInfo() throws HdbFailed {

    try {

      String version =  "Oracle HDB API v" + Hdb.getVersion() + "\n";
      String host = "Host:" + connection.getDb().getHost() + "\n";
      String service = "Service:" + connection.getDb().getServiceName();
      return version + host + service;

    } catch (HdbException e) {
      throw new HdbFailed(buildMessage(e));
    }

  }

  public String[] getHosts() throws HdbFailed {

   try {

     Vector<String> domains = browser.getSignalDomains();
     ArrayList<String> retHost = new ArrayList<String>();
     for(int i=0;i<domains.size();i++) {
       String nethost = browser.getNethostByDomain(domains.get(i));
       if(!retHost.contains(nethost))
         retHost.add(nethost);
     }

     String[] ret = new String[retHost.size()];
     for(int i=0;i<ret.length;i++)
       ret[i] = retHost.get(i);
     return ret;

    } catch (HdbException e) {
      throw new HdbFailed(buildMessage(e));
    }

  }

  public String[] getDomains(String host) throws HdbFailed {

    try {

      Vector<String> domains = browser.getSignalDomains();
      ArrayList<String> retDomains = new ArrayList<String>();
      for(int i=0;i<domains.size();i++) {
        String nethost = browser.getNethostByDomain(domains.get(i));
        if(nethost.equalsIgnoreCase(host))
          retDomains.add(domains.get(i));
      }

      String[] ret = new String[retDomains.size()];
      for(int i=0;i<ret.length;i++) {
        ret[i] = retDomains.get(i);
      }
      return ret;

    } catch (HdbException e) {
      throw new HdbFailed(buildMessage(e));
    }

  }

  public String[] getFamilies(String host,String domain) throws HdbFailed {

    try {

      Vector<String> families = browser.getSignalFamilies(domain);
      String[] ret = new String[families.size()];
      for(int i=0;i<ret.length;i++) {
        ret[i] = families.get(i);
      }
      return ret;

    } catch (HdbException e) {
      throw new HdbFailed(buildMessage(e));
    }

  }

  public String[] getMembers(String host,String domain,String family) throws HdbFailed {

    try {

      Vector<String> members = browser.getSignalMembers(domain,family);
      String[] ret = new String[members.size()];
      for(int i=0;i<ret.length;i++) {
        ret[i] = members.get(i);
      }
      return ret;

    } catch (HdbException e) {
      throw new HdbFailed(buildMessage(e));
    }

  }

  public String[] getNames(String host,String domain,String family,String member) throws HdbFailed {

    try {

      Vector<String> names = browser.getSignalNames(domain, family, member);
      String[] ret = new String[names.size()];
      for(int i=0;i<ret.length;i++) {
        ret[i] = names.get(i);
      }
      return ret;

    } catch (HdbException e) {
      throw new HdbFailed(buildMessage(e));
    }

  }

  public String[] getAttributeList() throws HdbFailed {

    ArrayList<String> allNames = new ArrayList<String>();

    HdbBrowser b = new HdbBrowser();

    try {

      Vector<String> domains = b.getSignalDomains();
      for(int i=0;i<domains.size();i++) {
        String nethost = b.getNethostByDomain(domains.get(i));
        Vector<String> families = b.getSignalFamilies(domains.get(i));
        for(int j=0;j<families.size();j++) {
          Vector<String> members = b.getSignalMembers(domains.get(i), families.get(j));
          for(int k=0;k<members.size();k++) {
            Vector<String> names = b.getSignalNames(domains.get(i), families.get(j), members.get(k));
            for(int l=0;l<names.size();l++) {
              allNames.add("tango://" + nethost + "/" + domains.get(i) + "/" + families.get(j)
                  + "/" + members.get(k) + "/" + names.get(l));
            }
          }
        }
      }

    } catch (HdbException e) {
      throw new HdbFailed(buildMessage(e));
    }

    String[] ret = new String[allNames.size()];
    for(int i=0;i<ret.length;i++)
      ret[i] = allNames.get(i);

    return ret;

  }

  HdbDataSet getDataFromDB(SignalInfo sigInfo,
                           String startDate,
                           String stopDate) throws HdbFailed {

    SigExtractQuery sigExt;

    // Construct query
    try {
      sigExt = new SigExtractQuery(connection.getDbId(),Long.parseLong(sigInfo.sigId));
      sigExt.setStartDate(startDate);
      sigExt.setEndDate(stopDate);
    } catch (HdbException ex) {
      throw new HdbFailed(buildMessage(ex));
    }

    ArrayList<HdbData> retList = new ArrayList<HdbData>();

    try {

      // Fetch data
      SigHistoryData data = sigExt.getSigHistoryData();

      // Convert to HDB++ format
      int recNb = (int)data.getRecNumber();
      if (recNb > 0) {

        // Convert dates
        long[] dates = data.extractLongDates();
        long[] usdates = new long[recNb];
        for (int j = 0; j < recNb; j++)
          usdates[j] = dates[j] * 1000000;

        // Convert data
        switch (sigInfo.dataType) {
          case SHORT:
            if(!sigInfo.isArray()) {
              short[] ss = ((ShortScalarHistoryData) data).extractData();
              for (int i = 0; i < recNb; i++) {
                HdbShort d = new HdbShort(sigInfo, ss[i]);
                d.setDataTime(usdates[i]);
                retList.add(d);
              }
            }
            else
            {
              short[][] as = ((ShortArrayHistoryData)data).extractData();
              for (int i = 0; i < recNb; i++) {
                HdbShortArray d = new HdbShortArray(sigInfo, as[i]);
                d.setDataTime(usdates[i]);
                retList.add(d);
              }
            }
            break;
          case LONG64:
            if(!sigInfo.isArray()) {
              long[] sl = ((LongScalarHistoryData) data).extractData();
              for (int i = 0; i < recNb; i++) {
                HdbLong64 d = new HdbLong64(sigInfo, sl[i]);
                d.setDataTime(usdates[i]);
                retList.add(d);
              }
            }
            else {
              long[][] al = ((LongArrayHistoryData)data).extractData();
              for (int i = 0; i < recNb; i++) {
                HdbLong64Array d = new HdbLong64Array(sigInfo, al[i]);
                d.setDataTime(usdates[i]);
                retList.add(d);
              }
            }
            break;
          case FLOAT:
            if(!sigInfo.isArray()) {
              float[] sf = ((FloatScalarHistoryData) data).extractData();
              for (int i = 0; i < recNb; i++) {
                HdbFloat d = new HdbFloat(sigInfo, sf[i]);
                d.setDataTime(usdates[i]);
                retList.add(d);
              }
            }
            else
            {
              float[][] af = ((FloatArrayHistoryData)data).extractData();
              for (int i = 0; i < recNb; i++) {
                HdbFloatArray d = new HdbFloatArray(sigInfo, af[i]);
                d.setDataTime(usdates[i]);
                retList.add(d);
              }
            }
            break;
          case DOUBLE:
            if(!sigInfo.isArray()) {
              double[] sd = ((DoubleScalarHistoryData) data).extractData();
              for (int i = 0; i < recNb; i++) {
                HdbDouble d = new HdbDouble(sigInfo, sd[i]);
                d.setDataTime(usdates[i]);
                retList.add(d);
              }
            }
            else {
              double[][] ad = ((DoubleArrayHistoryData)data).extractData();
              for (int i = 0; i < recNb; i++) {
                HdbDoubleArray d = new HdbDoubleArray(sigInfo, ad[i]);
                d.setDataTime(usdates[i]);
                retList.add(d);
              }
            }
            break;
          case STRING:
            if(!sigInfo.isArray()) {
              String[] sst = ((StringScalarHistoryData) data).extractData();
              for (int i = 0; i < recNb; i++) {
                HdbString d = new HdbString(sigInfo, sst[i]);
                d.setDataTime(usdates[i]);
                retList.add(d);
              }
            }
            break;
          case CHAR:
            if(sigInfo.isArray()) {
              byte[][] ac = ((CharArrayHistoryData) data).extractData();
              for (int i = 0; i < recNb; i++) {
                HdbByteArray d = new HdbByteArray(sigInfo, ac[i]);
                d.setDataTime(usdates[i]);
                retList.add(d);
              }
            }
            break;
        }

      }

    } catch (HdbException ex) {
      throw new HdbFailed(buildMessage(ex));
    }

    return new HdbDataSet(retList);

  }

  public HdbSigInfo getSigInfo(String attName) throws HdbFailed {

    SignalInfo ret = prepareSigInfo(attName);
    attName = ret.name.substring(6).toLowerCase();

    HdbSignal s = new HdbSignal(connection);
    s.setFullName(attName);
    try {

      int kind = s.getDefinition();
      ret.sigId = Long.toString(s.getId());

      switch(s.getDataType()) {

        case HdbConst.D_BOOLEAN_TYPE:
          throw new HdbFailed("D_BOOLEAN_TYPE not supported");
        case HdbConst.D_USHORT_TYPE:
          throw new HdbFailed("D_USHORT_TYPE not supported");
        case HdbConst.D_SHORT_TYPE:
          ret.dataType = HdbSigInfo.Type.SHORT;
          ret.format = HdbSigInfo.Format.SCALAR;
          ret.access = HdbSigInfo.Access.RO;
          break;
        case HdbConst.D_ULONG_TYPE:
          throw new HdbFailed("D_ULONG_TYPE not supported");
        case HdbConst.D_LONG_TYPE:
          ret.dataType = HdbSigInfo.Type.LONG64;
          ret.format = HdbSigInfo.Format.SCALAR;
          ret.access = HdbSigInfo.Access.RO;
          break;
        case HdbConst.D_FLOAT_TYPE:
          ret.dataType = HdbSigInfo.Type.FLOAT;
          ret.format = HdbSigInfo.Format.SCALAR;
          ret.access = HdbSigInfo.Access.RO;
          break;
        case HdbConst.D_DOUBLE_TYPE:
          ret.dataType = HdbSigInfo.Type.DOUBLE;
          ret.format = HdbSigInfo.Format.SCALAR;
          ret.access = HdbSigInfo.Access.RO;
          break;
        case HdbConst.D_STRING_TYPE:
          ret.dataType = HdbSigInfo.Type.STRING;
          ret.format = HdbSigInfo.Format.SCALAR;
          ret.access = HdbSigInfo.Access.RO;
          break;
        case HdbConst.D_FLOAT_READPOINT:
          throw new HdbFailed("D_FLOAT_READPOINT not supported");
        case HdbConst.D_STATE_FLOAT_READPOINT:
          throw new HdbFailed("D_STATE_FLOAT_READPOINT not supported");
        case HdbConst.D_LONG_READPOINT:
          throw new HdbFailed("D_LONG_READPOINT not supported");
        case HdbConst.D_DOUBLE_READPOINT:
          throw new HdbFailed("D_DOUBLE_READPOINT not supported");
        case HdbConst.D_VAR_CHARARR:
          ret.dataType = HdbSigInfo.Type.CHAR;
          ret.format = HdbSigInfo.Format.SPECTRUM;
          ret.access = HdbSigInfo.Access.RO;
          break;
        case HdbConst.D_VAR_STRINGARR:
          throw new HdbFailed("D_VAR_STRINGARR not supported");
        case HdbConst.D_VAR_USHORTARR:
          throw new HdbFailed("D_VAR_USHORTARR not supported");
        case HdbConst.D_VAR_SHORTARR:
          ret.dataType = HdbSigInfo.Type.SHORT;
          ret.format = HdbSigInfo.Format.SPECTRUM;
          ret.access = HdbSigInfo.Access.RO;
          break;
        case HdbConst.D_VAR_ULONGARR:
          throw new HdbFailed("D_VAR_ULONGARR not supported");
        case HdbConst.D_VAR_LONGARR:
          ret.dataType = HdbSigInfo.Type.LONG64;
          ret.format = HdbSigInfo.Format.SPECTRUM;
          ret.access = HdbSigInfo.Access.RO;
          break;
        case HdbConst.D_VAR_FLOATARR:
          ret.dataType = HdbSigInfo.Type.FLOAT;
          ret.format = HdbSigInfo.Format.SPECTRUM;
          ret.access = HdbSigInfo.Access.RO;
          break;
        case HdbConst.D_VAR_DOUBLEARR:
          ret.dataType = HdbSigInfo.Type.DOUBLE;
          ret.format = HdbSigInfo.Format.SPECTRUM;
          ret.access = HdbSigInfo.Access.RO;
          break;
        case HdbConst.D_VAR_FRPARR:
          throw new HdbFailed("D_VAR_FRPARR not supported");
        case HdbConst.D_VAR_SFRPARR:
          throw new HdbFailed("D_VAR_SFRPARR not supported");
        case HdbConst.D_VAR_LRPARR:
          throw new HdbFailed("D_VAR_LRPARR not supported");
        case HdbConst.D_OPAQUE_TYPE:
          throw new HdbFailed("D_OPAQUE_TYPE not supported");
        default:
          throw new HdbFailed("Unknown dataType code " + s.getDataType());

      }

      return new HdbSigInfo(ret);

    } catch (Exception e) {
      throw new HdbFailed("tacoHDB.HdbSignal.getDefinition() failed.\n" + attName + "\n" + e.getMessage());
    }

  }

  public  HdbSigParam getLastParam(SignalInfo sigInfo) throws HdbFailed {

    // We do not have this information in the DB
    // Got it from Tango
    HdbSigParam ret = new HdbSigParam(sigInfo);
    String attName = sigInfo.name;

    try {

      // Hack (to avoid mismatch between taco and tango)
      if(attName.startsWith("tango://aries/"))
        attName = attName.substring(14);

      AttributeProxy a = new AttributeProxy(attName);
      AttributeInfo ai = a.get_info();
      ret.label = ai.label;
      ret.unit = ai.unit;
      try {
        ret.standard_unit = Double.parseDouble(ai.standard_unit);
      } catch (NumberFormatException e) {
        ret.standard_unit = 1.0;
      }
      try {
        ret.display_unit = Double.parseDouble(ai.display_unit);
      } catch (NumberFormatException e) {
        ret.display_unit = 1.0;
      }
      ret.format = ai.format;
      ret.description = ai.description;

    } catch (DevFailed e) {
      throw new HdbFailed("Cannot get parameter for " + attName + "\n" + e.errors[0].desc);
    }

    return ret;

  }

  public ArrayList<HdbSigParam> getParams(String attName, String startDate, String stopDate) throws HdbFailed {
    throw new HdbFailed("getParams() not supported on Oracle HDB");
  }

  public ArrayList<HdbSigParam> getParams(SignalInfo sigInfo, String startDate, String stopDate) throws HdbFailed {
    throw new HdbFailed("getParams() not supported on Oracle HDB");
  }

  public HdbDataSet findErrors(String attName, String startDate, String stopDate) throws HdbFailed {
    return null;
  }

  private String buildMessage(HdbException ex) {
    String msg = "Class:" + ex.getClassName() + "\n" + "Method:" + ex.getMethod() + "\n" + ex.getMessage() + "\n" + ex.getSystemMessage();
    return msg;
  }

}

