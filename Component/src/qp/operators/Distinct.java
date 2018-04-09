/**
 * To projec out the required attributes from the result
 * It implements External Sort to the whole tuples and compares neighboring tuples to check whether they have duplicate value
 */

package qp.operators;

import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.util.Vector;

public class Distinct extends SortMerge{
    Batch inbatch;
    Batch outbatch;
    boolean eos;     // Indicate whether end of stream is reached or not
    Tuple lastTuple;
    int start;       // Cursor position in the input buffer

    public Distinct(Operator base, Vector as, int type) {
        super(base, as, type);
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

        // An output buffer is initiated
        outbatch = new Batch(batchSize);

        /* keep on checking the incoming pages until
         * the output buffer is full
         */
        while (!outbatch.isFull()) {
            if (start == 0) {
                inbatch= super.next();
                // There is no more incoming pages from base operator
                if (inbatch == null) {
                    eos = true;
                    return outbatch;
                }
            }

            /* Continue this for loop until this page is fully observed
             * or the output buffer is full
             */
            for (i = start; i < inbatch.size() && (!outbatch.isFull()); i++) {
                Tuple present = inbatch.elementAt(i);
                // If the condition is satisfied then this tuple is added to the output buffer
                if (lastTuple == null || Tuple.compareTuples(lastTuple, present, attrIndex) != 0) {
                    outbatch.add(present);
                    lastTuple = present;
                }
            }
            
            /* Modify the cursor to the position required
             * when the base operator is called next time
             */
            if (i == inbatch.size())
                start = 0;
            else
                start = i;
        }
        return outbatch;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        Vector newAttr = (Vector) attrSet.clone();
        Distinct newDistinct = new Distinct(newbase, newAttr, optype);
        newDistinct.setSchema(newbase.getSchema());
        return newDistinct;
    }
}
