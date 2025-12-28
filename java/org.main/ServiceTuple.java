package org.main;
import java.io.Serializable;
import java.util.Arrays;

public class ServiceTuple implements Serializable {
    public String id;
    public double[] values;

    public ServiceTuple() {
    }
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

    @Override
    public String toString() {
        return "ID: " + id + " " + Arrays.toString(values);
    }
}