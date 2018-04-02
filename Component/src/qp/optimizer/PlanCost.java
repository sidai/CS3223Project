/**
 * This method calculates the cost of the generated plans  also estimates the statistics of the result relation
 **/
/** also estimates the statistics of the result relation **/

package qp.optimizer;

import qp.operators.*;
import qp.utils.*;

import java.util.Hashtable;
import java.util.StringTokenizer;
import java.util.Enumeration;
import java.io.*;

public class PlanCost {

    int cost;
    int numtuple;


    /** If buffers are not enough for a selected join
     ** then this plan is not feasible and return
     ** a cost of infinity
     **/

    boolean isFeasible;


    /** Hashtable stores mapping from Attribute name to
     ** number of distinct values of that attribute
     **/

    Hashtable ht;


    public PlanCost() {
        ht = new Hashtable();
        cost = 0;
    }


    /** returns the cost of the plan **/

    public int getCost(Operator root) {
        //TODO: bug: cost should be clear or multiple of getCost on a QEP will accumulate
        cost = 0;

        isFeasible = true;
        numtuple = calculateCost(root);
        if (isFeasible == true) {
            return cost;
        } else {
            return Integer.MAX_VALUE;
        }
    }


    /** get number of tuples in estimated results **/

    public int getNumTuples() {
        return numtuple;
    }


    /** returns number of tuples in the root **/

    protected int calculateCost(Operator node) {


        if (node.getOpType() == OpType.JOIN) {
            return getStatistics((Join) node);
        } else if (node.getOpType() == OpType.SELECT) {
            //System.out.println("PlanCost: line 40");
            return getStatistics((Select) node);
        } else if (node.getOpType() == OpType.PROJECT) {
            return getStatistics((Project) node);
        } else if (node.getOpType() == OpType.SCAN) {
            return getStatistics((Scan) node);

        }
        return -1;
    }


    /** projection will not change any statistics
     ** No cost involved as done on the fly
     **/

    protected int getStatistics(Project node) {
        return calculateCost(node.getBase());
    }


    /** calculates the statistics, and cost of join operation **/

    protected int getStatistics(Join node) {
        int lefttuples = calculateCost(node.getLeft());
        int righttuples = calculateCost(node.getRight());

        if (isFeasible == false) {
            return -1;
        }

        Condition con = node.getCondition();
        Schema leftschema = node.getLeft().getSchema();
        Schema rightschema = node.getRight().getSchema();


        /** get size of the tuple in output & correspondigly calculate
         ** buffer capacity, i.e., number of tuples per page **/
        int tuplesize = node.getSchema().getTupleSize();
        int outcapacity = Batch.getPageSize() / tuplesize;
        int leftuplesize = leftschema.getTupleSize();
        int righttuplesize = rightschema.getTupleSize();
        int leftcapacity, rightcapacity;

        if(Batch.getPageSize() < leftuplesize || Batch.getPageSize() < righttuplesize) {
            isFeasible = false;
            System.out.println("page size is insufficient to hold a single tuple");
            return Integer.MAX_VALUE;
        } else {
            leftcapacity = Batch.getPageSize() / leftuplesize;
            rightcapacity = Batch.getPageSize() / righttuplesize;
        }


        int leftpages = (int) Math.ceil(((double) lefttuples) / (double) leftcapacity);
        int rightpages = (int) Math.ceil(((double) righttuples) / (double) rightcapacity);


        Attribute leftjoinAttr = con.getLhs();
        Attribute rightjoinAttr = (Attribute) con.getRhs();
        int leftattrind = leftschema.indexOf(leftjoinAttr);
        int rightattrind = rightschema.indexOf(rightjoinAttr);
        leftjoinAttr = leftschema.getAttribute(leftattrind);
        rightjoinAttr = rightschema.getAttribute(rightattrind);
        /** number of distinct values of left and right join attribute **/
        int leftattrdistn = ((Integer) ht.get(leftjoinAttr)).intValue();
        int rightattrdistn = ((Integer) ht.get(rightjoinAttr)).intValue();

        int outtuples = (int) Math.ceil(((double) lefttuples * righttuples) / (double) Math.max(leftattrdistn, rightattrdistn));

        int mindistinct = Math.min(leftattrdistn, rightattrdistn);
        ht.put(leftjoinAttr, new Integer(mindistinct));
        //TODO: BUG
        //ht.put(leftjoinAttr,new Integer(mindistinct));
        ht.put(rightjoinAttr, new Integer(mindistinct));


        /** now calculate the cost of the operation**/
        int joinType = node.getJoinType();
        /** number of buffers allotted to this join**/

        int numbuff = BufferManager.getBuffersPerJoin();

        int joincost;

        //System.out.println("PlanCost: jointype="+joinType);

        switch (joinType) {
            case JoinType.NESTEDJOIN:
                //TODO: BUG
                //joincost = leftpages * rightpages;
                joincost = calculateNLJCost(leftpages, rightpages);
                break;
            case JoinType.BLOCKNESTED:
                //TODO: BUG
                //joincost = 0;
                joincost = calculateBNLJCost(leftpages, rightpages, numbuff);
                break;
            case JoinType.SORTMERGE:
                //TODO: BUG
                //joincost = 0;
                joincost = calculateSMJCost(leftpages, rightpages, numbuff);
                break;
            case JoinType.HASHJOIN:
                //TODO: BUG
                joincost = calculateHashJoinCost(leftpages, rightpages);
                break;
            case JoinType.INDEXNESTED:
                //Assume Hash index and clustered index
                joincost = calculateINLJCost(leftpages, leftcapacity);
                break;
            default:
                joincost = 0;
                break;
        }
//        System.out.print("Join type: " + node.getJoinType() + "  Condition: ");
//        Debug.PPrint(node.getCondition());
//        System.out.println();
//        System.out.println("in_left, right: " + lefttuples + " " + righttuples);
//        System.out.print("leftSchema (tuplesize: " + leftuplesize + "): ");
//        Debug.PPrint(leftschema);
//        System.out.print("rightSchema (tuplesize: " + righttuplesize + "): ");
//        Debug.PPrint(rightschema);
//        System.out.println("capacity_left, right: " + leftcapacity + " " + rightcapacity);
//        System.out.println("leftpage, rightpages, buff: " + leftpages + " " + rightpages + " " + numbuff);
//        System.out.println("in_dist_left, right: " + leftattrdistn + " " + rightattrdistn);
//        System.out.println("joincost: " + joincost + " - outtuples: " + outtuples + " - distinct: " + mindistinct);
        cost = cost + joincost;
        return outtuples;
    }

    protected int calculateNLJCost(int leftpages, int rightpages) {
        return leftpages + leftpages * rightpages;
    }

    protected int calculateBNLJCost(int leftpages, int rightpages, int numbuff) {
        return leftpages + (int) Math.ceil(leftpages / (double) (numbuff - 2)) * rightpages;
    }

    protected int calculateSMJCost(int leftpages, int rightpages, int numbuff) {
        return calculateExternalSortCost(leftpages, numbuff) + calculateExternalSortCost(rightpages, numbuff)
                + leftpages + rightpages;
    }

    protected int calculateExternalSortCost(int pages, int numbuff) {
        return 2 * pages * (1 + (int) Math.ceil(Math.log(Math.ceil(pages / (double) numbuff)) / Math.log(numbuff - 1)));
    }

    protected int calculateHashJoinCost(int leftpages, int rightpages) {
        return 3 * (leftpages + rightpages);
    }

    protected int calculateINLJCost(int leftpages, int leftcapacity) {
        return leftpages + (int) ((leftpages * leftcapacity) * (1.2 + 1));
    }

    /** Find number of incoming tuples, Using the selectivity find # of output tuples
     ** And statistics about the attributes
     ** Selection is performed on the fly, so no cost involved
     **/

    protected int getStatistics(Select node) {
        //System.out.println("PlanCost: here at line 127");
        int intuples = calculateCost(node.getBase());

        if (isFeasible == false) {
            return Integer.MAX_VALUE;
        }

        Condition con = node.getCondition();
        Schema schema = node.getSchema();

        Attribute attr = con.getLhs();

        int index = schema.indexOf(attr);
        Attribute fullattr = schema.getAttribute(index);

        int exprtype = con.getExprType();


        /** Get number of distinct values of selection attributes **/

        Integer temp = (Integer) ht.get(fullattr);
        int numdistinct = temp.intValue();
        //int numdistinct = ((Integer)ht.get(fullattr)).intValue();
        int outtuples;

        /** calculate the number of tuples in result **/
        if (exprtype == Condition.EQUAL) {
            outtuples = (int) Math.ceil((double) intuples / (double) numdistinct);
        } else if (exprtype == Condition.NOTEQUAL) {
            outtuples = (int) Math.ceil(intuples - ((double) intuples / (double) numdistinct));
        } else {
            // TODO: Over Simplification?
            outtuples = (int) Math.ceil(0.5 * intuples);
        }

        /** Modify the number of distinct values of each attribute
         ** Assuming the values are distributed uniformly along entire
         ** relation
         **/
        for (int i = 0; i < schema.getNumCols(); i++) {
            Attribute attri = schema.getAttribute(i);
            int oldvalue = ((Integer) ht.get(attri)).intValue();
            int newvalue = (int) Math.ceil(((double) outtuples / (double) intuples) * oldvalue);
            //TODO: BUG
            //ht.put(attri, new Integer(outtuples));
            ht.put(attri, new Integer(newvalue));
        }
//        System.out.print("Select Condition: ");
//        Debug.PPrint(node.getCondition());
//        System.out.println(" - outtuples = " + outtuples);
        return outtuples;
    }


    /**  the statistics file <tablename>.stat to find the statistics
     ** about that table;
     ** This table contains number of tuples in the table
     ** number of distinct values of each attribute
     **/

    protected int getStatistics(Scan node) {
        String tablename = node.getTabName();
        String filename = tablename + ".stat";
        Schema schema = node.getSchema();
        int numAttr = schema.getNumCols();
        BufferedReader in = null;
        try {
            in = new BufferedReader(new FileReader(filename));
        } catch (IOException io) {
            System.out.println("Error in opening file" + filename);
            System.exit(1);
        }
        String line = null;

        // First line = number of tuples
        try {
            line = in.readLine();
        } catch (IOException io) {
            System.out.println("Error in readin first line of " + filename);
            System.exit(1);
        }
        StringTokenizer tokenizer = new StringTokenizer(line);
        if (tokenizer.countTokens() != 1) {
            System.out.println("incorrect format of statastics file " + filename);
            System.exit(1);
        }


        String temp = tokenizer.nextToken();
        /** number of tuples in this table; **/
        int numtuples = Integer.parseInt(temp);


        try {
            line = in.readLine();
        } catch (IOException io) {
            System.out.println("error in reading second line of " + filename);
            System.exit(1);
        }
        tokenizer = new StringTokenizer(line);
        if (tokenizer.countTokens() != numAttr) {
            System.out.println("incorrect format of statastics file " + filename);
            System.exit(1);
        }

        for (int i = 0; i < numAttr; i++) {
            Attribute attr = schema.getAttribute(i);
            temp = tokenizer.nextToken();
            Integer distinctValues = Integer.valueOf(temp);
            ht.put(attr, distinctValues);
        }
        /** number of tuples per page**/

        int tuplesize = schema.getTupleSize();
        int pagesize = Batch.getPageSize() / tuplesize;
        //Batch.capacity();
        int numpages = (int) Math.ceil((double) numtuples / (double) pagesize);
//        System.out.print("Table: " + node.getTabName());
//        System.out.println(" - scancost: " + numpages);
        cost = cost + numpages;
        try {
            in.close();
        } catch (IOException io) {
            System.out.println("error in closing the file " + filename);
            System.exit(1);
        }


        //System.out.println("Scan: tablename="+tablename+"pres cost="+numpages+"total cost="+cost);
        return numtuples;
    }

}











