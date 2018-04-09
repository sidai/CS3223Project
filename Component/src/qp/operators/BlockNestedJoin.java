/**
 * block nested join algorithm
 * This class implements the Block Nested Join algorithm. It will read one block(formed by several pages) from left base table each time
 * and compare with right base table. It will reduce a lot of IO cost compared to naive(page) nested join because the
 * number of scanning right table will be decreased a lot.
 */

package qp.operators;

import qp.utils.*;

import java.io.*;
import java.util.Vector;


public class BlockNestedJoin extends Join {
    static int filenum = 0;    // To get unique filenum for this operation
    int batchsize;             // Number of tuples per out batch
    int blocksize;             // number of batches per block
    int leftindex;             // Index of the join attribute in left table
    int rightindex;            // Index of the join attribute in right table
    
    String rfname;             // The file name where the right table is materialize
    
    Batch outbatch;            // Output buffer
    Batch leftbatch;           // Buffer for left input stream
    Block leftblock;
    
    Batch rightbatch;          // Buffer for right input stream
    ObjectInputStream in;      // File pointer to the right hand materialized file
    
    int lcurs;                 // Cursor for left side buffer
    int rcurs;                 // Cursor for right side buffer
    boolean eosl;              // End of stream (left table)
    boolean eosr;              // End of stream (right table)
    
    public BlockNestedJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }
    
    
    /** During open phase,
     *  1. Find the index of the join attributes
     *  2. Materialize the right hand side into a file
     *  3. Open the connection with left table
     */
    public boolean open() {
        // select number of tuples per batch
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;
        blocksize = numBuff - 2;
        
        Attribute leftattr = con.getLhs();
        Attribute rightattr = (Attribute) con.getRhs();
        leftindex = left.getSchema().indexOf(leftattr);
        rightindex = right.getSchema().indexOf(rightattr);
        Batch rightpage;
        
        // initialize the cursors of input buffers
        lcurs = 0;
        rcurs = 0;
        eosl = false;
        
        /* because right stream is to be repetitively scanned
         * if it reached end, we have to start new scan
         */
        eosr = true;
        
        /* Right hand side table is to be materialized to a file
         * for the Nested join to perform with every block of left table
         */
        if (!right.open()) {
            return false;
        } else {
            /* If the right operator is not a base table then
             * Materialize the intermediate result from right
             * into a file
             */
            filenum++;
            rfname = "NJtemp-" + String.valueOf(filenum);
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
                while ((rightpage = right.next()) != null) {
                    out.writeObject(rightpage);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("NestedJoin: writing the temporary file error");
                return false;
            }
            if (!right.close())
                return false;
        }
        if (left.open())
            return true;
        else
            return false;
    }
    
    
    /** from input buffers select the tuples satisfying join condition
     * And returns a page of output tuples which satisfy join condition
     */
    public Batch next() {
        // loop indicator to loop through block from left table and right table
        int i, j;
        if (eosl) {
            close();
            return null;
        }
        outbatch = new Batch(batchsize);
        
        // end condition is out page is full or end of the stream of table
        while (!outbatch.isFull()) {
            if (lcurs == 0 && eosr == true) {
                // new left block is to be fetched
                leftblock = new Block(blocksize, batchsize);
                // scan one page each time and form several pages to a block
                while(!eosl && !leftblock.isFull()) {
                    leftbatch = (Batch) left.next();
                    if(leftbatch != null) {
                        leftblock.addBatch(leftbatch);
                    } else {
                        break;
                    }
                }
                if (leftblock.isEmpty()) {
                    eosl = true;
                    return outbatch;
                }
                // Whenever a new left block comes, we have to start the scanning of right table
                try {
                    in = new ObjectInputStream(new FileInputStream(rfname));
                    eosr = false;
                } catch (IOException io) {
                    System.err.println("NestedJoin:error in reading the file");
                    System.exit(1);
                }
                
            }
            
            while (eosr == false) {
                try {
                    if (rcurs == 0 && lcurs == 0) {
                        rightbatch = (Batch) in.readObject();
                    }
                    
                    for (i = lcurs; i < leftblock.getTupleSize(); i++) {
                        for (j = rcurs; j < rightbatch.size(); j++) {
                            Tuple lefttuple = leftblock.getTuple(i);
                            Tuple righttuple = rightbatch.elementAt(j);
                            if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) {
                                Tuple outtuple = lefttuple.joinWith(righttuple);
                                outbatch.add(outtuple);
                                if (outbatch.isFull()) {
                                    // case 1: left and right both reach the end, reset left and right cursor to 0
                                    if (i == leftblock.getTupleSize() - 1 && j == rightbatch.size() - 1) {
                                        lcurs = 0;
                                        rcurs = 0;
                                    }
                                    // case 2: left does not reach the end, right reach the end of page
                                    else if (i != leftblock.getTupleSize() - 1 && j == rightbatch.size() - 1) {
                                        lcurs = i + 1;
                                        rcurs = 0;
                                    }
                                    // case 3: left reaches the end, right does not reach the end
                                    // Left cursor does not move because it will check whether it can be joined with next tuple of right
                                    else if (i == leftblock.getTupleSize() - 1 && j != rightbatch.size() - 1) {
                                        lcurs = i;
                                        rcurs = j + 1;
                                    } else {
                                        lcurs = i;
                                        rcurs = j + 1;
                                    }
                                    return outbatch;
                                }
                            }
                        }
                        rcurs = 0;
                    }
                    lcurs = 0;
                } catch (EOFException e) {
                    try {
                        in.close();
                    } catch (IOException io) {
                        System.out.println("NestedJoin:Error in temporary file reading");
                    }
                    eosr = true;
                } catch (ClassNotFoundException c) {
                    System.out.println("NestedJoin:Some error in deserialization ");
                    System.exit(1);
                } catch (IOException io) {
                    System.out.println("NestedJoin:temporary file reading error");
                    System.exit(1);
                }
            }
        }
        return outbatch;
    }
    
    /** Close the operator */
    public boolean close() {
        File f = new File(rfname);
        f.delete();
        return true;
    }
}
