/**
 * To gather the tuples with the same value of specified attribute or attribute set into one group
 * Often used with aggregation statement
 * Sort the table and compare with neighboring tuples
 */
package qp.operators;

import qp.utils.Aggregation;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.Vector;

public class GroupBy extends SortMerge {
    Aggregation aggregation;

    Batch inbatch;
    Batch outbatch;
    boolean eos;     // Indicate whether end of stream is reached or not
    Tuple lastTuple;
    int start;       // Cursor position in the input buffer
    int aggrIndex;

    public GroupBy(Operator base, Vector as, Aggregation aggregation, int type) {
        super(base, as, type);
        this.aggregation = aggregation;
    }
    
    /**
     * Set the schema and initialize the cursor and eos status
     *
     * @return whether it is okay to start fetching the result of GroupBy
     */
    @Override
    public boolean open() {
        eos = false;
        start = 0;
        lastTuple = null;
        schema = new Schema(attrSet);
        return super.open();
    }

    @Override
    public Batch next() {
        int i;
        if (eos) {
            super.close();
            return null;
        }

        // An output buffer is initiated
        outbatch = new Batch(batchSize);
        
        // no aggregation required, treated as distinct
        if (this.aggregation == null) {
            // keep on checking the incoming pages until the output buffer is full
            while (!outbatch.isFull()) {
                if (start == 0) {
                    inbatch = super.next();
                    // There is no more incoming pages from base operator
                    if (inbatch == null) {
                        eos = true;
                        return outbatch;
                    }
                }
                // Continue this for loop until this page is fully observed or the output buffer is full
                for (i = start; i < inbatch.size() && (!outbatch.isFull()); i++) {
                    Tuple basetuple = inbatch.elementAt(i);
                    if (lastTuple == null || Tuple.compareTuples(lastTuple, basetuple, attrIndex) != 0) {
                        Vector present = new Vector();
                        for (int j = 0; j < attrSet.size(); j++) {
                            Object data = basetuple.dataAt(attrIndex[j]);
                            present.add(data);
                        }
                        outbatch.add(new Tuple(present));
                        lastTuple = basetuple;
                    }
                }
                // Modify the cursor to the position required when the base operator is called next time
                if (i == inbatch.size())
                    start = 0;
                else
                    start = i;
            }
        }
        // no group by attribute, only group by, return the only aggregated value that loops over the whole table as a group
        else if (this.attrSet.isEmpty()) {
            int aggregationType = aggregation.getAggregationType();
            Attribute attr = aggregation.getAttribute();
            
            // int[] param: [count, sum, max, min]
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
        //aggregation and group by attribute
        else {
            // keep on checking the incoming pages until the output buffer is full
            int aggregationType = aggregation.getAggregationType();
            Attribute attr = aggregation.getAttribute();
            
            // int[] param: [count, sum, max, min]
            int[] param = {0, 0, 0, Integer.MAX_VALUE};
            while (!outbatch.isFull()) {
                if (start == 0) {
                    inbatch = super.next();
                    // There is no more incoming pages from base operator
                    if (inbatch == null) {
                        appendAggregatedValue(lastTuple, param, aggregationType);
                        outbatch.add(lastTuple);
                        eos = true;
                        return outbatch;
                    }
                }
                // Continue this for loop until this page is fully observed or the output buffer is full
                for (i = start; i < inbatch.size() && (!outbatch.isFull()); i++) {
                    Tuple present = inbatch.elementAt(i);
                    if (lastTuple == null || Tuple.compareTuples(lastTuple, present, attrIndex) == 0) { //same or start point
                        if(lastTuple == null) {
                            lastTuple = present;
                        }
                        doAggregation(present, attr, param, aggregationType);
                    } else {
                        appendAggregatedValue(lastTuple, param, aggregationType);
                        outbatch.add(lastTuple);
                        param[0] = param[1] = param[2];
                        param[3] = Integer.MAX_VALUE;
                        lastTuple = present;
                    }
                }
                // Modify the cursor to the position required when the base operator is called next time
                if (i == inbatch.size())
                    start = 0;
                else
                    start = i;
            }
        }
        
        // print out the result batch to check whether group by is implemented correctly
        System.out.println("========GROUP BY==============");
        for (int j = 0; j < outbatch.size(); j++) {
            Tuple present = outbatch.elementAt(j);
            System.out.print("tuple: ");
            for(int k=0; k<present._data.size(); k++) {
                System.out.print(present.dataAt(k) + " ");
            }
            System.out.println();
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
    /* int[] param: [count, sum, max, min]
     * Do the aggregation
     * When SUM ==> increase the sum value
     * MAX, MIN ==> compare current value with previous limit value
     * AVG ==> increase sum and size
     */
    protected void doAggregation (Tuple tuple, Attribute attr, int[] param, int aggregationType) {
        if (aggregationType == Aggregation.COUNT) {
            param[0]++;
        } else {
            try {
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
                } else if (aggregationType == Aggregation.SUM) {
                    param[1] += value;
                }
            } catch (Exception e) {
                System.out.println("Attribute is not INT type, cannot do " + aggregation.getName());
            }
        }
    }

    public Aggregation getAggregation() {
        return aggregation;
    }

    public Vector getAttrSet() {
        return this.attrSet;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        Vector newAttr = (Vector) attrSet.clone();
        Aggregation newAggr = (Aggregation) aggregation.clone();
        GroupBy newGroupBy = new GroupBy(newbase, newAttr, newAggr, optype);
        newGroupBy.setSchema(newbase.getSchema());
        return newGroupBy;
    }
}