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
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 *  HdbData aggregate data
 */
public class HdbAggregateData extends HdbData {

  public class AggregateData
  {

    long count_rows;
    long count_errors;
    ArrayList<Long> count_r = new ArrayList<>();
    ArrayList<Long> count_nan_r = new ArrayList<>();
    ArrayList<Double> mean_r = new ArrayList<>();
    ArrayList<Number> min_r = new ArrayList<>();
    ArrayList<Number> max_r = new ArrayList<>();
    ArrayList<Double> stddev_r = new ArrayList<>();
    ArrayList<Long> count_w = new ArrayList<>();
    ArrayList<Long> count_nan_w = new ArrayList<>();
    ArrayList<Double> mean_w = new ArrayList<>();
    ArrayList<Number> min_w = new ArrayList<>();
    ArrayList<Number> max_w = new ArrayList<>();
    ArrayList<Double> stddev_w = new ArrayList<>();

    public AggregateData()
    {

    }

    public AggregateData(long count_rows, long count_errors
            , ArrayList<Long> count_r, ArrayList<Long> count_nan_r, ArrayList<Double> mean_r, ArrayList<Number> min_r, ArrayList<Number> max_r, ArrayList<Double> stddev_r
            , ArrayList<Long> count_w, ArrayList<Long> count_nan_w, ArrayList<Double> mean_w, ArrayList<Number> min_w, ArrayList<Number> max_w, ArrayList<Double> stddev_w) {
      this.count_rows = count_rows;
      this.count_errors = count_errors;
      this.count_r.addAll(count_r);
      this.count_nan_r.addAll(count_nan_r);
      this.mean_r.addAll(mean_r);
      this.min_r.addAll(min_r);
      this.max_r.addAll(max_r);
      this.stddev_r.addAll(stddev_r);
      this.count_w.addAll(count_w);
      this.count_nan_w.addAll(count_nan_w);
      this.mean_w.addAll(mean_w);
      this.min_w.addAll(min_w);
      this.max_w.addAll(max_w);
      this.stddev_w.addAll(stddev_w);
    }

    public long getRowCount()
    {
      return count_rows;
    }

    public long getErrorCount()
    {
      return count_errors;
    }

    public ArrayList<Long> getReadCount() throws HdbFailed
    {
      return count_r;
    }

    public ArrayList<Long> getReadNanCount() throws HdbFailed
    {
      if(info.isFloating())
        return count_nan_r;
      else
        throw new HdbFailed("There is no NaN aggregate for this attribute.");
    }

    public ArrayList<Double> getReadMean()
    {
      return mean_r;
    }

    public ArrayList<Number> getReadMin()
    {
      return min_r;
    }

    public ArrayList<Number> getReadMax()
    {
      return max_r;
    }

    public ArrayList<Double> getReadStddev()
    {
      return stddev_r;
    }

    public ArrayList<Long> getWriteCount() throws HdbFailed
    {
      return count_w;
    }

    public ArrayList<Long> getWriteNanCount() throws HdbFailed
    {
      if(info.isFloating())
        return count_nan_w;
      else
        throw new HdbFailed("There is no NaN aggregate for this attribute.");
    }

    public ArrayList<Double> getWriteMean()
    {
      return mean_r;
    }

    public ArrayList<Number> getWriteMin()
    {
      return min_r;
    }

    public ArrayList<Number> getWriteMax()
    {
      return max_r;
    }

    public ArrayList<Double> getWriteStddev()
    {
      return stddev_r;
    }

    public AggregateData clone()
    {
      return new AggregateData(count_rows, count_errors
            , count_r, count_nan_r, mean_r, min_r, max_r, stddev_r
            , count_w, count_nan_w, mean_w, min_w, max_w, stddev_w);
    }
  }

  public HdbAggregateData(SignalInfo info) {
    super(info);
  }

  private AggregateData agg;
  private Map<Aggregate, List<Number>> aggregateCache;

  /**
   * Returns the value as double if it can be converted.
   * @throws HdbFailed In case of failure
   */
  public double getValueAsDouble() throws HdbFailed
  {
    throw new HdbFailed("Not supported.");
  }

  /**
   * Returns the value as double array it can be converted.
   * @throws HdbFailed In case of failure
   */
  public double[] getValueAsDoubleArray() throws HdbFailed
  {
    throw new HdbFailed("Not supported.");
  }

  /**
   * Returns the write value as double if it can be converted.
   * @throws HdbFailed In case of failure
   */
  public double getWriteValueAsDouble() throws HdbFailed
  {
    throw new HdbFailed("Not supported.");
  }

  /**
   * Returns the write value as double array it can be converted.
   * @throws HdbFailed In case of failure
   */
  public double[] getWriteValueAsDoubleArray() throws HdbFailed
  {
    throw new HdbFailed("Not supported.");
  }

  /**
   * Returns the value as long if it can be converted.
   * @throws HdbFailed In case of failure
   */
  public long getValueAsLong() throws HdbFailed
  {
    throw new HdbFailed("Not supported.");
  }

  /**
   * Returns the value as long array it can be converted.
   * @throws HdbFailed In case of failure
   */
  public long[] getValueAsLongArray() throws HdbFailed
  {
    throw new HdbFailed("Not supported.");
  }

  /**
   * Returns the write value as long if it can be converted.
   * @throws HdbFailed In case of failure
   */
  public long getWriteValueAsLong() throws HdbFailed
  {
    throw new HdbFailed("Not supported.");
  }

  /**
   * Returns the write value as long array it can be converted.
   * @throws HdbFailed In case of failure
   */
  public long[] getWriteValueAsLongArray() throws HdbFailed
  {
    throw new HdbFailed("Not supported.");
  }

  /**
   * Parse value
   * @param value Value to be parsed
   */
  public void parseValue(ArrayList<Object> value) throws HdbFailed
  {
    throw new HdbFailed("Not supported.");
  }

  /**
   * Parse write value
   * @param value Value to be parsed
   */
  public void parseWriteValue(ArrayList<Object> value) throws HdbFailed
  {
    throw new HdbFailed("Not supported.");
  }

  /**
   * Returns the value as a String
   */
  public String getValueAsString()
  {
    return "";
  }

  /**
   * Returns the write value as a String
   */
  public String getWriteValueAsString()
  {
    return "";
  }

  // Convenience functions
  int dataSize()
  {
    return 0;
  }
  int dataSizeW()
  {
    return 0;
  }

  @Override
  HdbData copyData() {
    HdbAggregateData ret = new HdbAggregateData(info);
    ret.agg = agg.clone();
    return ret;
  }

  public void applyConversionFactor(double f)
  {
  }

  public Map<Aggregate, List<Number>> getAggregate() throws HdbFailed
  {
    if(aggregateCache == null) {
      aggregateCache = new EnumMap<>(Aggregate.class);
      if (agg != null) {
        ArrayList<Number> rows_count = new ArrayList<>();
        rows_count.add(agg.getRowCount());
        aggregateCache.put(Aggregate.ROWS_COUNT, rows_count);
        ArrayList<Number> errors_count = new ArrayList<>();
        errors_count.add(agg.getErrorCount());
        aggregateCache.put(Aggregate.ERRORS_COUNT, errors_count);
        if (info.access == SignalInfo.Access.RO || info.access == SignalInfo.Access.RW) {
          if (info.isFloating()) {
            ArrayList<Number> nan_r_count = new ArrayList<>();
            nan_r_count.addAll(agg.getReadNanCount());
            aggregateCache.put(Aggregate.NAN_COUNT_R, nan_r_count);
          }
          ArrayList<Number> count = new ArrayList<>();
          count.addAll(agg.getReadCount());
          aggregateCache.put(Aggregate.COUNT_R, count);
          ArrayList<Number> mean_r = new ArrayList<>();
          mean_r.addAll(agg.getReadMean());
          aggregateCache.put(Aggregate.MEAN_R, mean_r);
          ArrayList<Number> min_r = new ArrayList<>();
          min_r.addAll(agg.getReadMin());
          aggregateCache.put(Aggregate.MIN_R, min_r);
          ArrayList<Number> max_r = new ArrayList<>();
          max_r.addAll(agg.getReadMax());
          aggregateCache.put(Aggregate.MAX_R, max_r);
          ArrayList<Number> stddev_r = new ArrayList<>();
          stddev_r.addAll(agg.getReadStddev());
          aggregateCache.put(Aggregate.STDDEV_R, stddev_r);
        }
        if (info.access == SignalInfo.Access.WO || info.access == SignalInfo.Access.RW) {
          if (info.isFloating()) {
            ArrayList<Number> nan_w_count = new ArrayList<>();
            nan_w_count.addAll(agg.getWriteNanCount());
            aggregateCache.put(Aggregate.NAN_COUNT_W, nan_w_count);
          }
          ArrayList<Number> count = new ArrayList<>();
          count.addAll(agg.getWriteCount());
          aggregateCache.put(Aggregate.COUNT_W, count);
          ArrayList<Number> mean_w = new ArrayList<>();
          mean_w.addAll(agg.getWriteMean());
          aggregateCache.put(Aggregate.MEAN_W, mean_w);
          ArrayList<Number> min_w = new ArrayList<>();
          min_w.addAll(agg.getWriteMin());
          aggregateCache.put(Aggregate.MIN_W, min_w);
          ArrayList<Number> max_w = new ArrayList<>();
          max_w.addAll(agg.getWriteMax());
          aggregateCache.put(Aggregate.MAX_W, max_w);
          ArrayList<Number> stddev_w = new ArrayList<>();
          stddev_w.addAll(agg.getWriteStddev());
          aggregateCache.put(Aggregate.STDDEV_W, stddev_w);
        }
      }
    }
    return aggregateCache;
  }

  protected void doParseAggregate(long count_rows, long count_errors
          , ArrayList<Long> count_r, ArrayList<Long> count_nan_r, ArrayList<Double> mean_r, ArrayList<Number> min_r, ArrayList<Number> max_r, ArrayList<Double> stddev_r
          , ArrayList<Long> count_w, ArrayList<Long> count_nan_w, ArrayList<Double> mean_w, ArrayList<Number> min_w, ArrayList<Number> max_w, ArrayList<Double> stddev_w)
  {
    agg = new AggregateData(count_rows, count_errors, count_r, count_nan_r, mean_r, min_r, max_r, stddev_r, count_w, count_nan_w, mean_w, min_w, max_w, stddev_w);
  }

  @Override
  public String toString()
  {
    StringBuffer ret = new StringBuffer();
    ret.append("Aggregate data for parameter:\n");
    ret.append(info.toString());
    ret.append("\n");
    try {
      for(Map.Entry<Aggregate, List<Number>> aggregate : getAggregate().entrySet())
      {
        final String agg = aggregate.getKey().toString();
        ret.append(agg);
        ret.append(": ");
        String val = aggregate.getValue().size()>1?"[":"";
        for(Number n : aggregate.getValue())
        {
          val += n + ", ";
        }
        if(val.length() > 2)
          val = val.substring(0, val.length()-2);
        val += aggregate.getValue().size()>1?"]":"";
        ret.append(val);
        ret.append("\n");
      }
    } catch (HdbFailed hdbFailed) {
    }
    return ret.toString();
  }
}
