package qp.operators;

import qp.utils.*;

import java.io.*;
import java.util.*;

public class SortMerge extends Operator {
    protected Operator base;
    protected int numBuff;
    protected Vector attrSet;
    protected int[] attrIndex;

    protected int batchSize;

    protected List<File> sortedFiles;
    protected String fileName;

    protected ObjectInputStream in;

    public SortMerge(Operator base, Vector as, int opType) {
        super(opType);
        this.base = base;
        this.attrSet = as;
    }

    public SortMerge(Operator base, Vector as, int opType, int numBuff, String fileName) {
        super(opType);
        this.base = base;
        this.attrSet = as;
        this.numBuff = numBuff;
        this.fileName = fileName;
    }
    
    public boolean open() {
//        System.out.println("SortMerge:-----------------in open--------------");
        if (numBuff < 3) {
            System.out.println("insufficient buffer for sort merge");
            System.exit(1);
        }
        if(!base.open()) {
            return false;
        } else {
            // Initialization
            int tupleSize = base.getSchema().getTupleSize();
            batchSize = Batch.getPageSize() / tupleSize;

            /** The followingl loop findouts the index of the columns that
             ** are required from the base operator
             **/
            Schema baseSchema = base.getSchema();
            attrIndex = new int[attrSet.size()];
            for (int i = 0; i < attrSet.size(); i++) {
                Attribute attr = (Attribute) attrSet.elementAt(i);
                int index = baseSchema.indexOf(attr);
                attrIndex[i] = index;
            }

            // Phase 1: Generate sorted runs
            sortedFiles = new ArrayList<>();
//            System.out.println("generate sort runs");
            generateSortedRuns();

            // Phase 2: Merge sorted runs
//            System.out.println("merge sort run: ");
            mergeSortedFiles();

//            for(int i=0; i<sortedFiles.size(); i++) {
//                System.out.println("==========merge result " + i + "=============");
//                try {
//                    int count = 0;
//                    in = new ObjectInputStream(new FileInputStream(sortedFiles.get(i)));
//                    while (true) {
//                        Batch batch = getNextBatch(in);
//                        if (batch == null)
//                            break;
//                        count++;
//                        for (int j = 0; j < batch.size(); j++) {
//                            Tuple present = batch.elementAt(j);
//                            System.out.print("tuple: ");
//                            for (int k = 0; k < present._data.size(); k++) {
//                                System.out.print(present.dataAt(k) + " ");
//                            }
//                            System.out.println();
//                        }
//                    }
//                    System.out.println("==========merge result end=============" + count);
//                    System.out.println();
//                } catch (Exception e) {
//                    System.err.println(" Error reading " + i + " of " + sortedFiles.size());
//                }
//            }
            try {
                in = new ObjectInputStream(new FileInputStream(sortedFiles.get(0)));
            } catch (Exception e) {
                System.err.println(" Error reading " + sortedFiles.get(0));
                return false;
            }

            return true;
        }
    }
    
    public Batch next() {
//        System.out.println("SortMerge:-----------------in next--------------");
        if(sortedFiles.size() != 1) {
            System.out.println("There is something wrong with sort-merge process. ");
        }
        try {
            Batch batch = (Batch) in.readObject();
            return batch;
        } catch (IOException e) {
            return null;
        } catch (ClassNotFoundException e) {
            System.out.println("File not found. ");
        }
        return null;
    }
    
    public boolean close() {
        for(File file: sortedFiles)
        file.delete();
        try {
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }
    
    public void generateSortedRuns() {
        int numRuns = 0;
        Batch batch = base.next();
        while(batch != null && !batch.isEmpty()) {
            Block run = new Block(numBuff, batchSize);
            while(!run.isFull() && batch != null && !batch.isEmpty()) {
                run.addBatch(batch);
                batch = base.next();
            }
            numRuns++;
            List<Tuple> tuples = run.getTuples();
            Collections.sort(tuples, new AttrComparator(attrIndex));

            Block sortedRun = new Block(numBuff, batchSize);
            sortedRun.setTuples((Vector) tuples);
//            System.out.println("run size: " + tuples.size());
            File result = writeToFile(sortedRun, numRuns);
            sortedFiles.add(result);
        }
    }
    
    /**
     * This is the merge part of the whole sort-merge process
     * Recursively merge until there are only one run
     */
    public void mergeSortedFiles() {
        int inputNumBuff = numBuff - 1;
        int mergeTimes = 0;
        List<File> resultSortedFiles;
        while(sortedFiles.size() > 1) {
            resultSortedFiles = new ArrayList<>();
            int mergeNumRuns = 0;
            for(int i = 0; i * inputNumBuff < sortedFiles.size(); i++) {
                // every time sort $(inputNumBuff) files
                int start = i *inputNumBuff;
                int end = (i+1) * inputNumBuff;
                if(end >= sortedFiles.size()) {
                    end = sortedFiles.size();
                }
//                System.out.println("start-end: " + start + " " + end);
                List<File> currentFilesToBeSort = sortedFiles.subList(start, end);
                // merge $(inputNumBuff) runs to a longer run
                File resultFile = mergeSortedRuns(currentFilesToBeSort, mergeTimes, mergeNumRuns);
//                System.out.println("=======================");
//                try {
//                    in = new ObjectInputStream(new FileInputStream(resultFile));
//                    while (true) {
//                        Batch batch = getNextBatch(in);
//                        if (batch == null)
//                            break;
//                        for (int j = 0; j < batch.size(); j++) {
//                            Tuple present = batch.elementAt(j);
//                            System.out.println("tuple: " + present.dataAt(0) + " " + present.dataAt(1)
//                                    + " " + present.dataAt(2) + " " + present.dataAt(3));
//                        }
//                    }
//                    System.out.println();
//                } catch (Exception e) {
//                    System.err.println(" Error reading " + sortedFiles.get(0));
//                }
//                System.out.println();
                mergeNumRuns++;
                resultSortedFiles.add(resultFile);
            }
            
            for(File file : sortedFiles) {
                file.delete();
            }
            sortedFiles = resultSortedFiles;

            mergeTimes++;
//            System.out.println("mergeTime: " + mergeTimes);
        }
    }

    public File mergeSortedRuns(List<File> runs, int mergeTimes, int mergeNumRuns) {
        int inputNumBuff = numBuff - 1;
        int numRuns = runs.size();
        boolean additionalBuffer = (inputNumBuff > numRuns);
        int runNum;

        if (inputNumBuff < numRuns) {
            System.out.println("There are too many runs in input buffers. ");
            return null;
        }
        ArrayList<ObjectInputStream> inputStreams = new ArrayList<>();
        try {
            for (int i = 0; i < numRuns; i++) {
                ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream(runs.get(i)));
                inputStreams.add(inputStream);
//                System.out.println("=======================");
//                try {
//                    in = new ObjectInputStream(new FileInputStream(runs.get(i)));
//                    while (true) {
//                        Batch batch = getNextBatch(in);
//                        if (batch == null)
//                            break;
//                        for (int j = 0; j < batch.size(); j++) {
//                            Tuple present = batch.elementAt(j);
//                            System.out.println("tuple: " + present.dataAt(0) + " " + present.dataAt(1)
//                                    + " " + present.dataAt(2) + " " + present.dataAt(3));
//                        }
//                    }
//                    System.out.println("======end here=========");
//                } catch (Exception e) {
//                    System.err.println(" Error reading " + runs.get(i));
//                }
            }
        } catch (IOException e) {
            System.out.println("Reading the temporary file error");
        }

        // real merging process
        File resultFile = new File(this.fileName + "-MergedFile-" + mergeTimes + "-" + mergeNumRuns);
        ObjectOutputStream out = initObjectOutputStream(resultFile);

        // we will only use numRun buffer for input even though there could be available buffer
        // because any latter batch can only proceed after all its front batches in the same run
        // has been written to output buffer
        ArrayList<Batch> inputBatches = new ArrayList<>(numRuns);
        for (runNum = 0; runNum < numRuns; runNum++) {
            Batch batch = getNextBatch(inputStreams.get(runNum));
            inputBatches.add(runNum, batch);
            if (batch == null) {
                System.out.println("ERROR: run " + runNum + " is initially empty.");
            }
        }

        Batch outputBuffer = new Batch(batchSize);
        Batch batch;

        if(additionalBuffer) {
//            System.out.println("using Priority queue");
            Queue<Tuple> inputTuples = new PriorityQueue<>(numRuns, new AttrComparator(attrIndex));
            Map<Tuple, Integer> tupleToRunNumMap = new HashMap<>(numRuns);
            for (runNum = 0; runNum < numRuns; runNum++) {
                batch = inputBatches.get(runNum);
                Tuple tuple = batch.remove(0);
                inputTuples.add(tuple);
                tupleToRunNumMap.put(tuple, runNum);
                if (batch.isEmpty()) {
                    batch = getNextBatch(inputStreams.get(runNum));
                    inputBatches.set(runNum, batch);
                }
            }


            while (!inputTuples.isEmpty()) {
                // output minTuple to output buffer and write out result if outputBuffer is full
                Tuple minTuple = inputTuples.remove();
                outputBuffer.add(minTuple);
                if (outputBuffer.isFull()) {
                    appendToObjectOutputStream(out, outputBuffer);
                    outputBuffer.clear();
                }

                // add a new tuple from the same run into priority queue if exists
                runNum = tupleToRunNumMap.get(minTuple);
                batch = inputBatches.get(runNum);

                if (batch != null) {
                    // notEmpty of batch is ensured by the algorithm
                    Tuple tuple = batch.remove(0);
                    inputTuples.add(tuple);
                    tupleToRunNumMap.put(tuple, runNum);
                    if (batch.isEmpty()) {
                        batch = getNextBatch(inputStreams.get(runNum));
                        inputBatches.set(runNum, batch);
                    }
                }
            }
            //add the leftover in output buffer
            if (!outputBuffer.isEmpty()) {
//            System.out.println("add leftover");
                appendToObjectOutputStream(out, outputBuffer);
                outputBuffer.clear();
            }
            tupleToRunNumMap.clear();
        } else {
//            System.out.println("using iteration");
            while(!isCompleted(inputBatches)) {
                runNum = getRunNumOfMinTuple(inputBatches);
                batch = inputBatches.get(runNum);

                // output minTuple to output buffer and write out result if outputBuffer is full
                outputBuffer.add(batch.remove(0));
                if (outputBuffer.isFull()) {
                    appendToObjectOutputStream(out, outputBuffer);
                    outputBuffer.clear();
                }

                // load a new batch from the same run if the current inputBatch is empty
                if (batch.isEmpty()) {
                    batch = getNextBatch(inputStreams.get(runNum));
                    inputBatches.set(runNum, batch);
                }
            }
            //add the leftover in output buffer
            if (!outputBuffer.isEmpty()) {
//            System.out.println("add leftover");
                appendToObjectOutputStream(out, outputBuffer);
                outputBuffer.clear();
            }
        }
//        System.out.println("==========final result=============");
//        try {
//            ObjectInputStream a = new ObjectInputStream(new FileInputStream(resultFile));
//            while (true) {
//                batch = getNextBatch(a);
//                if (batch == null)
//                    break;
//                for (int j = 0; j < batch.size(); j++) {
//                    Tuple present = batch.elementAt(j);
//                    System.out.println("tuple: " + present.dataAt(0) + " " + present.dataAt(1)
//                            + " " + present.dataAt(2) + " " + present.dataAt(3));
//                }
//            }
//            System.out.println();
//        } catch (Exception e) {
//            System.err.println(" Error reading " + resultFile);
//        }
//        System.out.println();
        closeObjectOutputStream(out);
        return resultFile;
    }

    protected boolean isCompleted(ArrayList<Batch> inputBatches) {
        for(int i=0; i<inputBatches.size(); i++) {
            if(inputBatches.get(i) != null) {
                return false;
            }
        }
        return true;
    }

    protected int getRunNumOfMinTuple(ArrayList<Batch> inputBatches) {
        Tuple minTuple = null;
        int minIndex = 0;
        for(int i=0; i<inputBatches.size(); i++) {
            if(inputBatches.get(i) != null) {
                minTuple = inputBatches.get(i).elementAt(0);
                minIndex = i;
                break;
            }
        }
        for(int i=0; i<inputBatches.size(); i++) {
            if(inputBatches.get(i) != null) {
                Tuple currTuple  = inputBatches.get(i).elementAt(0);
                if(Tuple.compareTuples(currTuple, minTuple, attrIndex) < 0) {
                    minTuple = currTuple;
                    minIndex = i;
                }
            }
        }
        return minIndex;
    }


    protected Batch getNextBatch(ObjectInputStream inputStream) {
        try {
            Batch batch = (Batch) inputStream.readObject();
            if(batch == null) {
                System.out.println("batch is null");
            }
            return batch;
        } catch (EOFException e) {
            return null;
        } catch (ClassNotFoundException | IOException e) {
            System.out.println("Encounter error when read from stream");
        }
        return null;
    }

    public File writeToFile(Block run, int numRuns) {
        try {
            File temp = new File(this.fileName + "-SMTemp-" + numRuns);
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(temp));
            for(Batch batch : run.getBatches()) {
                out.writeObject(batch);
            }
            out.close();
            return temp;
        } catch (IOException io) {
            System.out.println("SortMerge: writing the temporary file error");
        }
        return null;
    }

    public ObjectOutputStream initObjectOutputStream(File file) {
        try {
            return new ObjectOutputStream(new FileOutputStream(file, true));
        } catch (IOException io) {
            System.out.println("SortMerge: cannot initialize object output stream");
        }
        return null;
    }

    public void appendToObjectOutputStream(ObjectOutputStream out, Batch batch) {
        try {
            out.writeObject(batch);
            out.reset();          //reset the ObjectOutputStream to enable appending result
//            System.out.println("=========append result==============");
//            for (int j = 0; j < batch.size(); j++) {
//                Tuple present = batch.elementAt(j);
//                System.out.println("tuple: " + present.dataAt(0) + " " + present.dataAt(1)
//                        + " " + present.dataAt(2) + " " + present.dataAt(3));
//            }
//            System.out.println("===========end============");
        } catch (IOException io) {
            System.out.println("SortMerge: encounter error when append to object output stream");
        }
    }

    public void closeObjectOutputStream(ObjectOutputStream out) {
        try {
            out.close();
        } catch (IOException io) {
            System.out.println("SortMerge: cannot close object output stream");
        }
    }
    
    class AttrComparator implements Comparator<Tuple> {
        private int[] attrIndex;
        
        public AttrComparator(int[] attrIndex) {
            this.attrIndex = attrIndex;
        }
        
        @Override
        public int compare(Tuple t1, Tuple t2) {
            return Tuple.compareTuples(t1, t2, attrIndex);
        }
    }

    /** number of buffers available to this join operator **/

    public void setNumBuff(int num) {
        this.numBuff = num;
    }

    public int getNumBuff() {
        return numBuff;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public Operator getBase() {
        return base;
    }
}
