/**
 * performs randomized optimization, iterative improvement algorithm
 **/


package qp.optimizer;

import javafx.util.Pair;
import qp.utils.*;
import qp.operators.*;

import java.lang.Math;
import java.util.Vector;

public class RandomOptimizer {

    /**
     * enumeration of different ways to find the neighbor plan
     **/

    public static final int METHODCHOICE = 0;  //selecting neighbor by changing a method for an operator
    public static final int COMMUTATIVE = 1;   // by rearranging the operators by commutative rule
    public static final int ASSOCIATIVE = 2;  // rearranging the operators by associative rule
    public static final int EXCHANGE = 3; // (A join B) join C -> (A join C) join B or A join (B join C) -> B join (A join C)


    /**
     * Number of altenative methods available for a node as specified above
     **/

    public static final int NUMCHOICES = 4;


    SQLQuery sqlquery;     // Vector of Vectors of Select + From + Where + GroupBy
    int numJoin;          // Number of joins in this query plan


    /**
     * constructor
     **/

    public RandomOptimizer(SQLQuery sqlquery) {
        this.sqlquery = sqlquery;
    }


    /**
     * Randomly selects a neighbour
     **/


    protected Operator getNeighbor(Operator root) {
        //Randomly select a node to be altered to get the neighbour
        int nodeNum = RandNumb.randInt(0, numJoin - 1);

        //Randomly select type of alteration: Change Method/Associative/Commutative
        int changeType = RandNumb.randInt(0, NUMCHOICES - 1);
        Operator neighbor = null;

        switch (changeType) {
            case METHODCHOICE:   // Select a neighbour by changing the method type
                neighbor = neighborMeth(root, nodeNum);
                break;
            case COMMUTATIVE:
                neighbor = neighborCommut(root, nodeNum);
                break;
            case ASSOCIATIVE:
                neighbor = neighborAssoc(root, nodeNum);
                break;
            case EXCHANGE:
                neighbor = neighborExchange(root, nodeNum);
                break;
        }
        return neighbor;
    }


    /**
     * implementation of Iterative Improvement Algorithm
     * * for Randomized optimization of Query Plan
     **/

    public Operator getOptimizedPlan() {
        PlanCost pc = new PlanCost();
        RandomInitialPlan rip = new RandomInitialPlan(sqlquery);
        numJoin = rip.getNumJoins();
        System.out.println("There are " + numJoin + " join");

        /** get an initial plan for the given sql query **/
        Operator initPlan = rip.prepareInitialPlan();
        modifySchema(initPlan);

        System.out.println();
        System.out.println();
        System.out.println("-----------initial Plan-------------");
        Debug.PPrint(initPlan);
        System.out.println();
        int initCost = pc.getCost(initPlan);
        System.out.println("init plan cost: " + initCost);
        System.out.println();

        // There is no join statement, return the only plan
        if (numJoin == 0) {
            return initPlan;
        } else {
            Operator bestPlan = iterativeImprovement(initPlan, pc);
            bestPlan = simulatedAnnealing(bestPlan, pc);
            return bestPlan;
        }
    }


    /**
     * Selects a random method choice for join with number joinNum
     * *  e.g., Nested loop join, Sort-Merge Join, Hash Join etc..,
     * * returns the modified plan
     **/

    protected Operator neighborMeth(Operator root, int joinNum) {
//        System.out.println("------------------neighbor by method change----------------");
        int numJMeth = JoinType.numJoinTypes();
        if (numJMeth > 1) {
            /** find the node that is to be altered **/
            Join node = (Join) findNodeAt(root, joinNum);
            int prevJoinMeth = node.getJoinType();
//            System.out.println("Previous Join method is " + prevJoinMeth);
            int joinMeth = RandNumb.randInt(0, numJMeth - 1);
            while (joinMeth == prevJoinMeth) {
                joinMeth = RandNumb.randInt(0, numJMeth - 1);
            }
//            System.out.println("Current Join method is " + joinMeth);
            node.setJoinType(joinMeth);
        }
        return root;
    }


    /**
     * Applies join Commutativity for the join numbered with joinNum
     * *  e.g.,  A X B  is changed as B X A
     * * returns the modifies plan
     **/

    protected Operator neighborCommut(Operator root, int joinNum) {
//        System.out.println("------------------neighbor by commutative---------------");
        /** find the node to be altered**/
        Join node = (Join) findNodeAt(root, joinNum);
        Operator left = node.getLeft();
        Operator right = node.getRight();
        node.setLeft(right);
        node.setRight(left);
        /*** also flip the condition i.e.,  A X a1b1 B   = B X b1a1 A  **/
        node.getCondition().flip();
        //Schema newschem = left.getSchema().joinWith(right.getSchema());
        // node.setSchema(newschem);

        /** modify the schema before returning the root **/
        modifySchema(root);
        return root;
    }

    /**
     * Applies join Associativity for the join numbered with joinNum
     * *  CASE: join at left node
     * *      CASE 1: (A X a1b1 B) X b4c4 C = A X a1b1 (B X b4c4 C)
     * *      CASE 2: (A X a1b1 B) X a4c4 C = B X b1a1 (A X a4c4 C)
     * *  CASE: join at right node
     * *      CASE 3: A X a1b1 (B X b4c4 C) = (A X a1b1 B) X b4c4 C
     * *      CASE 4: A X a1c1 (B X b4c4 C) = (A X a1c1 C) X c4b4 B
     * *  returns the modifies plan
     **/

    protected Operator neighborAssoc(Operator root, int joinNum) {
        /** find the node to be altered**/
        Join op = (Join) findNodeAt(root, joinNum);
        //Join op = (Join) joinOpList.elementAt(joinNum);
        Operator left = op.getLeft();
        Operator right = op.getRight();

        if (left.getOpType() == OpType.JOIN && right.getOpType() != OpType.JOIN) {
            transformLefttoRight(op, (Join) left);

        } else if (left.getOpType() != OpType.JOIN && right.getOpType() == OpType.JOIN) {
            transformRighttoLeft(op, (Join) right);
        } else if (left.getOpType() == OpType.JOIN && right.getOpType() == OpType.JOIN) {
            if (RandNumb.flipCoin())
                transformLefttoRight(op, (Join) left);
            else
                transformRighttoLeft(op, (Join) right);
        } else {
            // The join is just A X B,  therefore Association rule is not applicable
        }

        /** modify the schema before returning the root **/
        modifySchema(root);
        return root;
    }

    /**
     * Applies Join exchange for the join numbered with joinNum
     * * Equivalent to Associativity then Commutativity
     * * The Join exchange rules reduce the number of plateaux by adding direct paths that bypass them
     * *  CASE: join at left node
     * *      CASE 1: (A X a1b1 B) X b4c4 C = (B X b4c4 C) X a1b1 A
     * *      CASE 2: (A X a1b1 B) X a4c4 C = (A X a4c4 C) X b1a1 B
     * *  CASE: join at right node
     * *      CASE 3: A X a1b1 (B X b4c4 C) = C X b4c4 (A X a1b1 B)
     * *      CASE 4: A X a1c1 (B X b4c4 C) = B X c4b4 (A X a1c1 C)
     * *  returns the modifies plan
     **/

    protected Operator neighborExchange(Operator root, int joinNum) {
        root = neighborAssoc(root, joinNum);
        root = neighborCommut(root, joinNum);

        /** modify the schema before returning the root **/
        modifySchema(root);
        return root;
    }

    /**
     * This is given plan (A X B) X C
     **/


    protected void transformLefttoRight(Join op, Join left) {
//        System.out.println("------------------Left to Right neighbor--------------");
        Operator right = op.getRight();
        Operator leftleft = left.getLeft();
        Operator leftright = left.getRight();
        Attribute leftAttr = op.getCondition().getLhs();
        Join temp;

        /** CASE 1 :  ( A X a1b1 B) X b4c4  C     =  A X a1b1 (B X b4c4 C)
         ** a1b1,  b4c4 are the join conditions at that join operator
         **/

        if (leftright.getSchema().contains(leftAttr)) {
//            System.out.println("----------------CASE 1-----------------");

            temp = new Join(leftright, right, op.getCondition(), OpType.JOIN);
            temp.setJoinType(op.getJoinType());
            temp.setNodeIndex(op.getNodeIndex());
            op.setLeft(leftleft);
            op.setJoinType(left.getJoinType());
            op.setNodeIndex(left.getNodeIndex());
            op.setRight(temp);
            op.setCondition(left.getCondition());

        } else {
//            System.out.println("--------------------CASE 2---------------");
            /**CASE 2:   ( A X a1b1 B) X a4c4  C     =  B X b1a1 (A X a4c4 C)
             ** a1b1,  a4c4 are the join conditions at that join operator
             **/
            temp = new Join(leftleft, right, op.getCondition(), OpType.JOIN);
            temp.setJoinType(op.getJoinType());
            temp.setNodeIndex(op.getNodeIndex());
            op.setLeft(leftright);
            op.setRight(temp);
            op.setJoinType(left.getJoinType());
            op.setNodeIndex(left.getNodeIndex());
            Condition newcond = left.getCondition();
            newcond.flip();
            op.setCondition(newcond);
        }
    }

    protected void transformRighttoLeft(Join op, Join right) {

//        System.out.println("------------------Right to Left Neighbor------------------");
        Operator left = op.getLeft();
        Operator rightleft = right.getLeft();
        Operator rightright = right.getRight();
        Attribute rightAttr = (Attribute) op.getCondition().getRhs();
        Join temp;
        /** CASE 3 :  A X a1b1 (B X b4c4  C)     =  (A X a1b1 B ) X b4c4 C
         ** a1b1,  b4c4 are the join conditions at that join operator
         **/
        if (rightleft.getSchema().contains(rightAttr)) {
//            System.out.println("----------------------CASE 3-----------------------");
            temp = new Join(left, rightleft, op.getCondition(), OpType.JOIN);
            temp.setJoinType(op.getJoinType());
            temp.setNodeIndex(op.getNodeIndex());
            op.setLeft(temp);
            op.setRight(rightright);
            op.setJoinType(right.getJoinType());
            op.setNodeIndex(right.getNodeIndex());
            op.setCondition(right.getCondition());
        } else {
            /** CASE 4 :  A X a1c1 (B X b4c4  C)     =  (A X a1c1 C ) X c4b4 B
             ** a1b1,  b4c4 are the join conditions at that join operator
             **/
//            System.out.println("-----------------------------CASE 4-----------------");
            temp = new Join(left, rightright, op.getCondition(), OpType.JOIN);
            temp.setJoinType(op.getJoinType());
            temp.setNodeIndex(op.getNodeIndex());

            op.setLeft(temp);
            op.setRight(rightleft);
            op.setJoinType(right.getJoinType());
            op.setNodeIndex(right.getNodeIndex());
            Condition newcond = right.getCondition();
            newcond.flip();
            op.setCondition(newcond);
        }
    }


    /**
     * This method traverses through the query plan and
     * * returns the node mentioned by joinNum
     **/

    protected Operator findNodeAt(Operator node, int joinNum) {
        if (node.getOpType() == OpType.JOIN) {
            if (((Join) node).getNodeIndex() == joinNum) {
                return node;
            } else {
                Operator temp;
                temp = findNodeAt(((Join) node).getLeft(), joinNum);
                if (temp == null)
                    temp = findNodeAt(((Join) node).getRight(), joinNum);
                return temp;
            }
        } else if (node.getOpType() == OpType.SCAN) {
            return null;
        } else if (node.getOpType() == OpType.SELECT) {
            //if sort/project/select operator
            return findNodeAt(((Select) node).getBase(), joinNum);
        } else if (node.getOpType() == OpType.PROJECT) {
            return findNodeAt(((Project) node).getBase(), joinNum);
        } else if (node.getOpType() == OpType.DISTINCT) {
            return findNodeAt(((Distinct) node).getBase(), joinNum);
        } else if (node.getOpType() == OpType.GROUP_BY) {
            return findNodeAt(((GroupBy) node).getBase(), joinNum);
        } else {
            return null;
        }
    }


    /**
     * modifies the schema of operators which are modified due to selecing an alternative neighbor plan
     **/
    private void modifySchema(Operator node) {
        if (node.getOpType() == OpType.JOIN) {
            Operator left = ((Join) node).getLeft();
            Operator right = ((Join) node).getRight();
            modifySchema(left);
            modifySchema(right);
            node.setSchema(left.getSchema().joinWith(right.getSchema()));
        } else if (node.getOpType() == OpType.SELECT) {
            Operator base = ((Select) node).getBase();
            modifySchema(base);
            node.setSchema(base.getSchema());
        } else if (node.getOpType() == OpType.PROJECT) {
            Operator base = ((Project) node).getBase();
            modifySchema(base);
            Vector attrlist = ((Project) node).getProjAttr();
            node.setSchema(base.getSchema().subSchema(attrlist));
        } else if (node.getOpType() == OpType.DISTINCT) {
            Operator base = ((Distinct) node).getBase();
            modifySchema(base);
            node.setSchema(base.getSchema());
        } else if (node.getOpType() == OpType.GROUP_BY) {
            Operator base = ((GroupBy) node).getBase();
            modifySchema(base);
            node.setSchema(base.getSchema());
        }
    }


    /**
     * AFter finding a choice of method for each operator
     * prepare an execution plan by replacing the methods with
     * corresponding join operator implementation
     **/

    public static Operator makeExecPlan(Operator node) {

        if (node.getOpType() == OpType.JOIN) {
            Operator left = makeExecPlan(((Join) node).getLeft());
            Operator right = makeExecPlan(((Join) node).getRight());
            int joinType = ((Join) node).getJoinType();
            int numbuff = BufferManager.getBuffersPerJoin();
            switch (joinType) {
                case JoinType.NESTEDJOIN:

                    NestedJoin nj = new NestedJoin((Join) node);
                    nj.setLeft(left);
                    nj.setRight(right);
                    nj.setNumBuff(numbuff);
                    return nj;

                case JoinType.BLOCKNESTED:

                    BlockNestedJoin bj = new BlockNestedJoin((Join) node);
                    bj.setLeft(left);
                    bj.setRight(right);
                    bj.setNumBuff(numbuff);
                    return bj;

                case JoinType.SORTMERGE:
                 
                    SortMergeJoin sm = new SortMergeJoin((Join) node);
                    sm.setLeft(left);
                    sm.setRight(right);
                    sm.setNumBuff(numbuff);
                    return sm;

                /** Temporarity used simple nested join,
                 replace with hashjoin, if implemented **/
                case JoinType.HASHJOIN:

                    NestedJoin hj = new NestedJoin((Join) node);
                    /* + other code */
                    return hj;
                default:
                    return node;
            }
        } else if (node.getOpType() == OpType.SELECT) {
            Operator base = makeExecPlan(((Select) node).getBase());
            ((Select) node).setBase(base);
            return node;
        } else if (node.getOpType() == OpType.PROJECT) {
            Operator base = makeExecPlan(((Project) node).getBase());
            ((Project) node).setBase(base);
            return node;
        } else if (node.getOpType() == OpType.DISTINCT) {
            int totalbuff = BufferManager.getBuffers();
            Operator base = makeExecPlan(((Distinct) node).getBase());
            ((Distinct) node).setBase(base);
            ((Distinct) node).setNumBuff(totalbuff);
            return node;
        } else if (node.getOpType() == OpType.GROUP_BY) {
            int totalbuff = BufferManager.getBuffers();
            Operator base = makeExecPlan(((GroupBy) node).getBase());
            ((GroupBy) node).setBase(base);
            ((GroupBy) node).setNumBuff(totalbuff);
            return node;
        } else {
            return node;
        }
    }

    protected Operator iterativeImprovement(Operator initPlan, PlanCost pc) {
        System.out.println();
        System.out.println("-----------Iterative Improvement start-------------");
        Operator globalState = (Operator) initPlan.clone();
        Operator localState;
        Operator neighborsStage;
        // Keep getting neighbor of initPlan for NUMITER_RANDOM times before we assume the state is randomly chosen
        int NUMITER_RANDOM = 3 * numJoin;
        // Set global minimum stopping condition: found 2 * numJoin local minimum
        int NUMITER_GLOBAL = 2 * numJoin;
        // Set local minimum stopping condition: try 8 * numJoin times
        int NUMITER_LOCAL = 8 * numJoin;
        while (NUMITER_GLOBAL > 0) {
            // Start with a random state
            localState = (Operator) initPlan.clone();
            for (int i = 0; i < NUMITER_RANDOM; i++) {
                localState = getNeighbor(localState);
            }
            while (NUMITER_LOCAL > 0) {
                neighborsStage = (Operator) localState.clone();
                neighborsStage = getNeighbor(neighborsStage);
                // find a downward move, move to a better stage
                if (pc.getCost(neighborsStage) < pc.getCost(localState)) {
                    localState = neighborsStage;
                }
                NUMITER_LOCAL--;
            }
            if (pc.getCost(localState) < pc.getCost(globalState)) {
                globalState = localState;
            }
            NUMITER_GLOBAL--;
//            System.out.println("remaining tries for local minimum: " + NUMITER_GLOBAL);
        }
        Debug.PPrint(globalState);
        System.out.println();
        int globalStateCost = pc.getCost(globalState);
        System.out.println("global state cost: " + globalStateCost);
        System.out.println("-----------Iterative Improvement end-------------");
        System.out.println();
        return globalState;
    }

    protected Operator simulatedAnnealing(Operator bestPlan, PlanCost pc) {
        System.out.println();
        System.out.println("-----------Simulated Annealing start-------------");
        Operator optimalState = (Operator) bestPlan.clone();
        Operator neighborState;
        double temperature = 0.1 * pc.getCost(optimalState);
//        System.out.print("current temperature: " + temperature);
        int costDiff;
        // Set equilibrium condition: try 16 * numJoin times
        int NUMITER_EQUILIBRIUM = 16 * numJoin;
        while(temperature >= 1) {
            while(NUMITER_EQUILIBRIUM > 0) {
                neighborState = (Operator) optimalState.clone();
                neighborState = getNeighbor(neighborState);
                costDiff = pc.getCost(neighborState) - pc.getCost(optimalState);
                if(costDiff <= 0) {
                    optimalState = neighborState;
                } else {
                    double probability = Math.pow(Math.E, -costDiff/temperature);
                    if(Math.random() <= probability) {
                        optimalState = neighborState;
                    }
                }
                if(pc.getCost(optimalState) < pc.getCost(bestPlan)) {
                    bestPlan = optimalState;
                }
                NUMITER_EQUILIBRIUM--;
            }
            temperature = 0.9 * temperature;
//            System.out.println("current temperature: " + temperature);
        }
        Debug.PPrint(bestPlan);
        System.out.println();
        int finalPlanCost = pc.getCost(bestPlan);
        System.out.println("final plan cost: " + finalPlanCost);
        System.out.println("-----------Simulated Annealing end-------------");
        System.out.println();
        return bestPlan;
    }
}
    //        Operator finalPlan = null;
//        /** NUMTER is number of times random restart **/
//
//        int NUMITER;
//        if (numJoin != 0) {
//            NUMITER = 2 * numJoin;
//        } else {
//            NUMITER = 1;
//        }

//        /** Randomly restart the gradient descent until
//         ** the maximum specified number of random restarts (NUMITER)
//         ** has satisfied
//         **/

//        for (int j = 0; j < NUMITER; j++) {
//            Operator initPlan = rip.prepareInitialPlan();
//
//            modifySchema(initPlan);
//            System.out.println("-----------initial Plan-------------");
//            Debug.PPrint(initPlan);
//            System.out.println();
//            PlanCost pc = new PlanCost();
//            int initCost = pc.getCost(initPlan);
//            System.out.println("initCost: " + initCost);
//
//            boolean flag = true;
//            int minNeighborCost = initCost; //just initialization purpose;
//            Operator minNeighbor = initPlan; //just initialization purpose;
//            if (numJoin != 0) {
//                while (flag) {   // flag = false when local minimum is reached
//                    //TODO: BUG, redundant code
//                    System.out.println("---------------while--------");
//                    Operator initPlanCopy = (Operator) initPlan.clone();
//                    minNeighbor = getNeighbor(initPlanCopy);
//
//                    System.out.println("--------------------------neighbor---------------");
//                    Debug.PPrint(minNeighbor);
//                    pc = new PlanCost();
//                    minNeighborCost = pc.getCost(minNeighbor);
//                    System.out.println("  " + minNeighborCost);
//
//                    /** In this loop we consider from the
//                     ** possible neighbors (randomly selected)
//                     ** and take the minimum among for next step
//                     **/
//
//                    for (int i = 1; i < 2 * numJoin; i++) {
//                        initPlanCopy = (Operator) initPlan.clone();
//                        Operator neighbor = getNeighbor(initPlanCopy);
//                        System.out.println("------------------neighbor--------------");
//                        Debug.PPrint(neighbor);
//                        pc = new PlanCost();
//                        int neighborCost = pc.getCost(neighbor);
//                        System.out.println(neighborCost);
//
//
//                        if (neighborCost < minNeighborCost) {
//                            minNeighbor = neighbor;
//                            minNeighborCost = neighborCost;
//                        }
//                        // System.out.println("-----------------for-------------");
//                    }
//                    if (minNeighborCost < initCost) {
//                        initPlan = minNeighbor;
//                        initCost = minNeighborCost;
//                    } else {
//                        minNeighbor = initPlan;
//                        minNeighborCost = initCost;
//
//                        flag = false;   // local minimum reached
//                    }
//                }
//                System.out.println("------------------local minimum--------------");
//                Debug.PPrint(minNeighbor);
//                System.out.println(" " + minNeighborCost);
//
//            }
//            if (minNeighborCost < MINCOST) {
//                MINCOST = minNeighborCost;
//                finalPlan = minNeighbor;
//            }
//        }
//        System.out.println("\n\n\n");
//        System.out.println("---------------------------Final Plan----------------");
//        Debug.PPrint(finalPlan);
//        System.out.println();
//        System.out.println("Minimum cost: " + MINCOST);
//        return finalPlan;













