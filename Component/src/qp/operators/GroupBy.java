package qp.operators;

import qp.utils.Aggregation;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

import java.util.Vector;

public class GroupBy extends SortMerge {

    Aggregation aggregation;

    Batch inbatch;
    Batch outbatch;
    boolean eos;    // Indicate whether end of stream is reached or not
    Tuple lastTuple;
    int start;       // Cursor position in the input buffer

    public GroupBy(Operator base, Vector as, Aggregation aggregation, int type) {
        super(base, as, type);
        this.aggregation = aggregation;
    }


    @Override
    public boolean open() {
        eos = false;
        start = 0;
        lastTuple = null;
        return super.open();
    }

    @Override
    public Batch next() {
        int i;
        if (eos) {
            super.close();
            return null;
        }

        /** An output buffer is initiated**/
        outbatch = new Batch(batchSize);

        /** no aggregation required, treated as distinct **/
        if (this.aggregation == null) {
            /** keep on checking the incoming pages until the output buffer is full **/
            while (!outbatch.isFull()) {
                if (start == 0) {
                    inbatch = super.next();
                    /** There is no more incoming pages from base operator **/
                    if (inbatch == null) {
                        eos = true;
                        return outbatch;
                    }
                }
                /** Continue this for loop until this page is fully observed or the output buffer is full **/
                for (i = start; i < inbatch.size() && (!outbatch.isFull()); i++) {
                    Tuple present = inbatch.elementAt(i);
//                System.out.println("tuple: " + present.dataAt(0) + " " + present.dataAt(1)
//                        + " " + present.dataAt(2) + " " + present.dataAt(3));
                    if (lastTuple == null || Tuple.compareTuples(lastTuple, present, attrIndex) != 0) {
                        outbatch.add(present);
                        lastTuple = present;
                    }
                }
                /** Modify the cursor to the position required when the base operator is called next time**/
                if (i == inbatch.size())
                    start = 0;
                else
                    start = i;
            }
        }
        // no group by attribute, return the only aggregated value that loops over the whole table as a group
        else if (this.attrSet.isEmpty()) {
            int aggregationType = aggregation.getAggregationType();
            Attribute attr = aggregation.getAttribute();
            if(attr.getType() != Attribute.INT && aggregationType != Aggregation.COUNT) {
                System.out.println("Aggregation of " + aggregation.getName() + " required a integer object");
                System.exit(1);
            }
            /** int[] param: [count, sum, max, min] **/
            int[] param = {0, 0, 0, Integer.MAX_VALUE};
            while (true) {
                inbatch = super.next();
                if (inbatch == null) {
                    eos = true;
                    Tuple tuple = new Tuple();
                    appendAggregatedValue(tuple, param, aggregationType);
                    outbatch.add(tuple);
                    return outbatch;
                }
                for (i = 0; i < inbatch.size(); i++) {
                    Tuple tuple = inbatch.elementAt(i);
                    doAggregation(tuple, attr, param, aggregationType);
                }
            }
        }
        /** aggregation and group by attribute **/
        else {
            /** keep on checking the incoming pages until the output buffer is full **/
            int aggregationType = aggregation.getAggregationType();
            Attribute attr = aggregation.getAttribute();
            if(attr.getType() != Attribute.INT && aggregationType != Aggregation.COUNT) {
                System.out.println("Aggregation of " + aggregation.getName() + " required a integer object");
                System.exit(1);
            }
            /** int[] param: [count, sum, max, min] **/
            int[] param = {0, 0, 0, Integer.MAX_VALUE};
            while (!outbatch.isFull()) {
                if (start == 0) {
                    inbatch = super.next();
                    /** There is no more incoming pages from base operator **/
                    if (inbatch == null) {
                        eos = true;
                        return outbatch;
                    }
                }
                /** Continue this for loop until this page is fully observed or the output buffer is full **/
                for (i = start; i < inbatch.size() && (!outbatch.isFull()); i++) {
                    Tuple present = inbatch.elementAt(i);
                    if (lastTuple == null || Tuple.compareTuples(lastTuple, present, attrIndex) != 0) { //different
                        doAggregation(present, attr, param, aggregationType);
                    } else {
                        appendAggregatedValue(present, param, aggregationType);
                        outbatch.add(present);
                        lastTuple = present;
                    }
                }
                /** Modify the cursor to the position required when the base operator is called next time**/
                if (i == inbatch.size())
                    start = 0;
                else
                    start = i;
            }
        }
        return outbatch;
    }

    protected void appendAggregatedValue(Tuple tuple, int[] param, int aggregationType) {
        if (aggregationType == Aggregation.COUNT) {
            tuple.appendAggregatedValue(param[0]);
        } else if ((aggregationType == Aggregation.AVG)) {
            tuple.appendAggregatedValue(param[1]/(double)param[0]);
        } else if ((aggregationType == Aggregation.MAX)) {
            tuple.appendAggregatedValue(param[2]);
        } else if ((aggregationType == Aggregation.MIN)) {
            tuple.appendAggregatedValue(param[3]);
        } else if ((aggregationType == Aggregation.SUM)) {
            tuple.appendAggregatedValue(param[1]);
        }
    }
    /** int[] param: [count, sum, max, min] **/
    protected void doAggregation (Tuple tuple, Attribute attr, int[] param, int aggregationType) {
        if (aggregationType == Aggregation.COUNT) {
            param[0]++;
        } else {
            int value = (Integer) tuple.dataAt(base.getSchema().indexOf(attr));
            if (aggregationType == Aggregation.AVG) {
                param[1] += value;
                param[0]++;
            } else if (aggregationType == Aggregation.MAX) {
                if (param[2] < value) {
                    param[2] = value;
                }
            } else if (aggregationType == Aggregation.MIN) {
                if (param[3] > value) {
                    param[3] = value;
                }
            } else if (aggregationType == Aggregation.MAX) {
                param[1] += value;
            }
        }
    }
}