package com.seism.util;

import java.io.Serializable;

public class Point implements Serializable {
    private static final long serialVersionUID = 1L;

    public double x;
    public double y;

    public Point () {

    }
    public Point (double x, double y) {
        this.x = x;
        this.y = y;
    }
    @Override
    public String toString() {
        return "Point{" +
                "lon=" + x +
                ", lat=" + y +
                '}';
    }
}
