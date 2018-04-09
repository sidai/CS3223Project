/**
 * Sort Merge Join Algorithm
 * This algorithm use SortMerge to sort the left and right tables according to join attribute
 * and get one output page each time next() method is called
 */
package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.RandNumb;
import qp.utils.Tuple;
import java.util.Vector;

public class SortMergeJoin extends Join {
    int batchsize;  // Number of tuples per out batch

    int leftindex;     // Index of the join attribute in left table
    int rightindex;    // Index of the join attribute in right table
    Attribute leftattr;
    Attribute rightattr;

    int leftBatchSize;
    int rightBatchSize;

    Batch outbatch;   // Output buffer
    Batch leftbatch;  // Buffer for left input stream
    Batch rightbatch;  // Buffer for right input stream
    Vector tempBlock;

    Tuple leftTuple;
    Tuple rightTuple;
    Tuple refTuple;

    int lcurs;    // Cursor for left side buffer
    int rcurs;    // Cursor for right side buffer
    int tempcurs;
    boolean eos;  // Whether end of stream (left table or right table) is reached

    SortMerge sortedLeft;
    SortMerge sortedRight;

    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
    }


    /** During open finds the index of the join attributes
     **  Materializes the right hand side into a file
     **  Opens the connections
     **/

    @Override
    public boolean open() {
//        System.out.println("SortMergeJoin:-----------------in open--------------");
        Debug.PPrint(schema);
        // select number of tuples per batch and number of batches per block
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        leftattr = con.getLhs();
        Vector<Attribute> leftSet = new Vector<>();
        leftSet.add(leftattr);

        rightattr = (Attribute) con.getRhs();
        Vector<Attribute> rightSet = new Vector<>();
        rightSet.add(rightattr);

        leftindex = left.getSchema().indexOf(leftattr);
        int leftTupleSize = left.getSchema().getTupleSize();
        leftBatchSize = Batch.getPageSize() / leftTupleSize;

        rightindex = right.getSchema().indexOf(rightattr);
        int rightTupleSize = right.getSchema().getTupleSize();
        rightBatchSize = Batch.getPageSize() / rightTupleSize;

        sortedLeft = new SortMerge(left, leftSet, optype, numBuff, "left" + RandNumb.randInt(0, 10000));
        sortedRight = new SortMerge(right, rightSet, optype, numBuff, "right" + RandNumb.randInt(0,10000));

        // open base operator for getting the base batch
        if (!left.open() || !right.open()) {
            return false;
        }

        // open sort merge operator to sort the result
        if(!sortedLeft.open() || !sortedRight.open()) {
            return false;
        }

        eos = false;

        lcurs = 0;
        rcurs = 0;

        tempBlock = new Vector();
        tempcurs = 0;

        // Store the first group of tuples from right operation with same merge attribute into the block
        rightbatch = sortedRight.next();
        for (int j = 0; j < rightbatch.size(); j++) {
            Tuple present = rightbatch.elementAt(j);
        }

        refTuple = rightbatch.elementAt(0);
        while(rightbatch != null) {
            rightTuple = rightbatch.elementAt(rcurs);

            if(Tuple.compareTuples(refTuple, rightTuple, rightindex) == 0) {
                tempBlock.add(rightTuple);
                rcurs++;
                // move to next tuple and add current tuple into block if it has same merge attribute
                if(rcurs == rightbatch.size()) {
                    rcurs = 0;
                    rightbatch = sortedRight.next();
                    if(rightbatch == null) {
                        eos = true;
                        break;
                    }
                    for (int j = 0; j < rightbatch.size(); j++) {
                        Tuple present = rightbatch.elementAt(j);
                    }
                }
            } else {
                break;
            }
        }
        
        //preload a left batch
        leftbatch = sortedLeft.next();
        return true;
    }
    
    
    /** from input buffers selects the tuples satisfying join condition
     * And returns a page of output tuples
     */
    public Batch next() {
        System.out.println("SortMergeJoin:-----------------in next--------------");

        outbatch = new Batch(batchsize);
        while (!outbatch.isFull()) {
            // right or left batch has proceeded, handle remaining data in the tempBlock if exists
            if (eos) {
                if(tempBlock != null && !tempBlock.isEmpty()) {
                    while (!outbatch.isFull()) {
                        leftTuple = leftbatch.elementAt(lcurs);
                        int diff = Tuple.compareTuples(leftTuple, refTuple, leftindex, rightindex);
                        if (diff < 0) {
                            lcurs++;
                            if (lcurs == leftbatch.size()) {
                                leftbatch = sortedLeft.next();
                                lcurs = 0;
                                if (leftbatch == null) {
                                    eos = true;
                                    // complete, clear all buffer
                                    tempBlock.clear();
                                    return outbatch;
                                }
                            }
                        } else if (diff == 0) {
                            System.out.println("size: " + tempcurs + " " + tempBlock.size());
                            while (tempcurs < tempBlock.size()) {
                                outbatch.add(leftTuple.joinWith((Tuple) tempBlock.get(tempcurs)));
                                tempcurs++;
                                // return when outbatch is full, leftover will be handled at the start of the next run
                                if (outbatch.isFull()) {
                                    return outbatch;
                                }
                            }
                            if (tempcurs == tempBlock.size()) {
                                tempcurs = 0;
                                lcurs++;
                                if (lcurs == leftbatch.size()) {
                                    leftbatch = sortedLeft.next();
                                    lcurs = 0;
                                    if (leftbatch == null) {
                                        eos = true;
                                        // complete, clear all buffer
                                        tempBlock.clear();
                                        return outbatch;
                                    }
                                }
                            }
                        } else if (diff > 0) {
                            tempBlock.clear();
                            tempcurs = 0;
                            return outbatch;
                        }
                    }
                } else {
                    close();
                    return null;
                }
            }

            leftTuple = leftbatch.elementAt(lcurs);
            int diff = Tuple.compareTuples(leftTuple, refTuple, leftindex, rightindex);
            
            // left tuple and right tuple have the same attribute value
            if (diff == 0) {
                // join leftTuple with all satisfied tuples in tempBlock and add to outbatch
                while (tempcurs < tempBlock.size()) {
                    outbatch.add(leftTuple.joinWith((Tuple) tempBlock.get(tempcurs)));
                    tempcurs++;
                    
                    // return when outbatch is full, leftover will be handled at the start of the next run
                    if (outbatch.isFull()) {
                        return outbatch;
                    }
                }
                // complete join with all satisfied tuple in rightbatch, move left cursor to next tuple
                if (tempcurs == tempBlock.size()) {
                    tempcurs = 0;
                    lcurs++;
                    // left batch reach the end, start loading a new batch
                    if (lcurs == leftbatch.size()) {
                        leftbatch = sortedLeft.next();
                        lcurs = 0;
                        if (leftbatch == null) {
                            eos = true;
                            // complete, clear all buffer
                            tempBlock.clear();
                            return outbatch;
                        }
                    }
                }
            } else if (diff < 0) {
                // move left to next tuple since it is small than last rightTuple in the temp batch
                lcurs++;
                // left batch reach the end, start loading a new batch
                if (lcurs == leftbatch.size()) {
                    leftbatch = sortedLeft.next();
                    lcurs = 0;
                    if (leftbatch == null) {
                        eos = true;
                        // complete, clear all buffer
                        tempBlock.clear();
                        return outbatch;
                    }
                }
            } else if (diff > 0) {
                // leftTuple has a larger value, discard all of the stored tuple and move the right cursor till
                // the next rightTuple whose value is not smaller than leftTuple
                tempBlock.clear();
                tempcurs = 0;
                while (Tuple.compareTuples(leftTuple, rightTuple, leftindex, rightindex) > 0) {
                    rcurs++;
                    if (rcurs == rightbatch.size()) {
//                        System.out.println("size equal: " + rcurs + " " + rightbatch.size());
                        rcurs = 0;
                        rightbatch = sortedRight.next();
                        if(rightbatch == null) {
                            eos = true;
                            break;
                        }
                    }
                    rightTuple = rightbatch.elementAt(rcurs);
                }
                // no more right batch data smaller than left batch, break outer loop to return results
                if(rightbatch == null) {
                    eos = true;
                    break;
                }
                refTuple = rightbatch.elementAt(rcurs);
                // store the next group of tuples from right operation that are not smaller than leftTuple
                while (true) {
                    rightTuple = rightbatch.elementAt(rcurs);

                    // add current tuple into block if it has same merge attribute
                    if (Tuple.compareTuples(refTuple, rightTuple, rightindex) == 0) {
                        tempBlock.add(rightTuple);
                        rcurs++;
                        if (rcurs == rightbatch.size()) {
                            rcurs = 0;
                            rightbatch = sortedRight.next();
                            if(rightbatch == null) {
                                eos = true;
                                break;
                            }
                        }
                    } else {
                        break;
                    }
                }
            }
        }
        return outbatch;
    }

    /** Close the operator */
    public boolean close() {
        sortedLeft.close();
        sortedRight.close();
        return true;
    }
}