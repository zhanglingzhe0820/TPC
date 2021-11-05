package com.yahoo.ycsb.generator;

import com.yahoo.ycsb.Utils;

public class UniformIntegerGenerator extends NumberGenerator {
  private final int _lb;

  private final int _ub;

  private final int _interval;

  public UniformIntegerGenerator(int lb, int ub) {
    this._lb = lb;
    this._ub = ub;
    this._interval = this._ub - this._lb + 1;
  }

  public Integer nextValue() {
    int ret = Utils.random().nextInt(this._interval) + this._lb;
    setLastValue(Integer.valueOf(ret));
    return Integer.valueOf(ret);
  }

  public double mean() {
    return (this._lb + this._ub) / 2.0D;
  }
}
