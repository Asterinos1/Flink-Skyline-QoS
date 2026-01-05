package org.main;

import java.io.Serializable;
import java.util.Arrays;

public class ServiceTuple implements Serializable {
    public String id;
    public double[] values;
    //temporary helper to track which partition sent this tuple (for debugging/tracking)
    public int originPartition = -1;

    public ServiceTuple() {}

    public ServiceTuple(String id, double[] values) {
        this.id = id;
        this.values = values;
    }

    public boolean dominates(ServiceTuple other) {
        boolean betterInAtLeastOne = false;
        for (int i = 0; i < values.length; i++) {
            if (this.values[i] > other.values[i]) return false;
            if (this.values[i] < other.values[i]) betterInAtLeastOne = true;
        }
        return betterInAtLeastOne;
    }

    //method for CSV parsing
    public static ServiceTuple fromString(String s) {
        try {
            String[] p = s.split(",");
            if (p.length < 2) return null;
            double[] vals = new double[p.length - 1];
            for (int i = 1; i < p.length; i++) {
                vals[i - 1] = Double.parseDouble(p[i]);
            }
            return new ServiceTuple(p[0], vals);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return "ID:" + id + " " + Arrays.toString(values);
    }
}
