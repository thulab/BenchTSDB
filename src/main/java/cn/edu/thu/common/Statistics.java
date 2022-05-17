package cn.edu.thu.common;

import java.util.concurrent.atomic.AtomicLong;

public class Statistics {

    public AtomicLong fileNum = new AtomicLong(0);
    public AtomicLong recordNum = new AtomicLong(0);
    public AtomicLong nonNullPointNum = new AtomicLong(0);
    public AtomicLong allPointNum = new AtomicLong(0);
    public AtomicLong timeCost = new AtomicLong(0);

    public Statistics(){

    }

    /**
     * @return points / s
     */
    public double speed() {
        return ((double) nonNullPointNum.get()) / ((double) timeCost.get()) * 1000_000_000L;
    }
}
