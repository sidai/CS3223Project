package qp.operators;

import qp.utils.Aggregation;

import java.util.Vector;

public class GroupBy extends SortMerge {

    Aggregation aggregation;

    public GroupBy(Operator base, Vector as, int type) {
        super(base, as, type);
        this.aggregation = aggregation;
    }

    public Aggregation getAggregation() {
        return aggregation;
    }
}