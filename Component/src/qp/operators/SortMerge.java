package qp.operators;

import com.sun.media.jfxmedia.logging.Logger;
import qp.utils.*;

import javax.lang.model.type.ArrayType;
import java.io.*;
import java.util.*;

public class SortMerge extends Operator {
    private Operator op;
    private int numBuff;
    
    private Comparable<Attribute> comparator;
    
    private int tupleSize;
    private int batchSize;
    
    private int attrIndex;
    
    private int numRuns;
    private int mergeNumRuns;
    private int mergeTimes;
    private List<File> sortedFiles;
    
    private ObjectInputStream in;
    
    private int sortType; // 0 for sorting attribute, 1 for sorting the whole tuple (for DISTINCT)
    public SortMerge(Operator op, int numBuff, int attrIndex) {
        super(OpType.SORT);
        this.op = op;
        this.numBuff = numBuff;
        this.attrIndex = attrIndex;
    }
    
    public boolean open() {
        if(!op.open()) {
            return false;
        } else {
            // Initialization
            tupleSize = op.getSchema().getTupleSize();
            batchSize = Batch.getPageSize() / tupleSize;
            
//            Block block = new Block(batchSize, tupleSize);
            
            // Phase 1: Generate sorted runs
            sortedFiles = new ArrayList<>();
            generateSortedRuns();
            
            // Phase 2: Merge sorted runs
            mergeSortedFiles();
            
            return true;
        }
    }
    
    public Batch next() {
        if(sortedFiles.size() != 1) {
            System.out.println("There is something wrong with sort-merge process. ");
        }
        try {
            in = new ObjectInputStream(new FileInputStream(sortedFiles.get(0)));
            Batch batch = (Batch) in.readObject();
            return batch;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            System.out.println("File not found. ");
        }
        return null;
    }
    
    public boolean close() {
        sortedFiles.get(0).delete();
        try {
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }
    
    public void generateSortedRuns() {
        numRuns = 0;
        Batch batch = op.next();
        while(batch != null) {
            Block run = new Block(numBuff-1, batchSize);
            while(!run.isFull() && batch != null) {
                run.addBatch(batch);
                batch = op.next();
                numRuns++;
            }
            
            List<Tuple> tuples = run.getTuples();
            Collections.sort(tuples, new AttrComparator(attrIndex));
            
            Block sortedRun = new Block(numBuff-1, batchSize);
            sortedRun.setTuples((Vector) tuples);
            
            File result = writeToFile(sortedRun);
            sortedFiles.add(result);
        }
    }
    
    /**
     * This is the merge part of the whole sort-merge process
     * Recursively merge until there are only one run
     */
    public void mergeSortedFiles() {
        int inputNumBuff = numBuff - 1;
        mergeNumRuns = 0;
        mergeTimes = 0;
        
        List<File> resultSortedFiles = new ArrayList<>();
        while(sortedFiles.size() > 1) {
            resultSortedFiles = new ArrayList<>();
            for(int i = 0;i * inputNumBuff < sortedFiles.size();i++) {
                // every time sort $(inputNumBuff) files
                int start = i *inputNumBuff;
                int end = (i+1) * inputNumBuff;
                if(end >= sortedFiles.size()) {
                    end = sortedFiles.size();
                }
                
                List<File> currentFilesToBeSort = sortedFiles.subList(start, end);
                // merge $(inputNumBuff) runs to a longer run
                File resultFile = mergeSortedRuns(currentFilesToBeSort, mergeTimes, mergeNumRuns);
                mergeNumRuns++;
                resultSortedFiles.add(resultFile);
            }
            
            for(File file : sortedFiles) {
                file.delete();
            }
            sortedFiles = resultSortedFiles;
            mergeTimes++;
        }
    }
    
    public File mergeSortedRuns(List<File> runs, int numMerge, int numRuns) {
        int inputNumBuff = numBuff - 1;
        if(inputNumBuff < runs.size()) {
            System.out.println("There are too many runs in input buffers. ");
            return null;
        }
        ArrayList<ObjectInputStream> inputStreams = new ArrayList<>();
        try {
            for (int i = 0; i < runs.size(); i++) {
                ObjectInputStream in = new ObjectInputStream(new FileInputStream(runs.get(i)));
                inputStreams.add(in);
            }
        } catch (IOException e) {
            System.out.println("Reading the temporary file error");
        }
        
        ArrayList<Batch> inputBatches = new ArrayList<>(runs.size());
        try {
            for (int i = 0; i < inputNumBuff; i++) {
                Object obj = inputStreams.get(i).readObject();
                if(obj != null) {
                    inputBatches.add((Batch) obj);
                }
            }
        } catch (IOException e) {
            System.out.println("Reading input streams error");
        } catch (ClassNotFoundException e) {
            System.out.println("Class not found. ");
        }
        
        // real merging process
        File resultFile = new File("mergedFile-" + numMerge + "-" + numRuns);
//        ObjectOutputStream out;
//        try {
//            out = new ObjectOutputStream(new FileOutputStream(resultFile));
//        } catch (IOException e) {
//          System.out.println("Cannot create the output file stream. ");
//        }
        
        Tuple min = inputBatches.get(0).elementAt(0);
        boolean flag = true;
        boolean[] indicator = new boolean[runs.size()];
        for(int i = 0 ; i < indicator.length ; i++) {
            indicator[i] = true;
        }
        Batch outputBuffer = new Batch(batchSize);
        int minIndex = 0;
        // writeObject is the method to write object to output stream
        while(flag) {
            for(int i = 0;i < inputBatches.size();i++) {
                if(indicator[i]) {
                    Batch currBatch = inputBatches.get(i);
                    if (currBatch.isEmpty()) {
                        try {
                            currBatch = (Batch) inputStreams.get(i).readObject();
                            if (currBatch == null) {
                                indicator[i] = false;
                            }
                        } catch (IOException e) {
                            System.out.println("Reading input streams error");
                        } catch (ClassNotFoundException e) {
                            System.out.println("Class not found. ");
                        }
                    }
                    Tuple curr = currBatch.elementAt(0);
                    if (Tuple.compareTuples(min, curr, attrIndex) == 1) {
                        min = curr;
                        minIndex = i;
                    }
                }
            }
            outputBuffer.add(min);
            if(outputBuffer.isFull()) {
                try {
                    ObjectOutputStream out = new AppendableObjectOutputStream(new FileOutputStream(resultFile));
                    out.writeObject(outputBuffer);
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                outputBuffer = new Batch(batchSize);
            }
            inputBatches.get(minIndex).remove(0);
            
            // check flag
            flag = false;
            for(int i = 0; i < indicator.length; i++) {
                if(indicator[i]) {
                    flag = true;
                }
            }
        }
        return resultFile;
    }
    
    public File writeToFile(Block run) {
        try {
//            File temp = new File("temp-" + numRuns);
//            String rfname = new String("temp-" + numRuns);
            File temp = new File("temp-" + numRuns);
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
    
    class AttrComparator implements Comparator<Tuple> {
        private int attrIndex;
        
        public AttrComparator(int attrIndex) {
            this.attrIndex = attrIndex;
        }
        
        @Override
        public int compare(Tuple t1, Tuple t2) {
            return Tuple.compareTuples(t1, t2, attrIndex);
        }
    }
    
//    class TupleComparator implements
}
