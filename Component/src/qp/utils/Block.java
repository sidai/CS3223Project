package qp.utils;

import java.io.Serializable;
import java.util.Vector;

public class Block implements Serializable {
    int MAX_SIZE;
    int pageSize;
    Vector batches;
    Vector tuples;
    
    public Block(int numPage, int pageSize) {
        MAX_SIZE = numPage;
        this.pageSize = pageSize;
        batches = new Vector(MAX_SIZE);
        tuples = new Vector(MAX_SIZE * pageSize);
    }
    
    public Vector getBatches() {
        return batches;
    }
    
    public void setBatches(Vector batches) {
        this.batches = batches;
        for (int i = 0; i < batches.size(); i++) {
            for (int j = 0; j < ((Batch) batches.get(i)).size(); j++) {
                tuples.add((Tuple) ((Batch) batches.get(i)).elementAt(j));
            }
        }
    }
    
    public void addBatch(Batch batch) {
        batches.add(batch);
        for (int i = 0; i < batch.size(); i++) {
            tuples.add(batch.elementAt(i));
        }
    }
    
    public Vector getTuples() {
        return tuples;
    }
    
    public Tuple getTuple(int index) {
        return (Tuple) tuples.elementAt(index);
    }
    
    public int getBatchSize() {
        return batches.size();
    }
    
    public int getTupleSize() {
        return tuples.size();
    }
    
    public boolean isEmpty() {
        return batches.isEmpty();
    }
    
    public boolean isFull() {
        return (batches.size() >= MAX_SIZE);
    }
}