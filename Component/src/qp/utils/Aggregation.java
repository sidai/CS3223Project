/**
 * Aggregation class, implements aggregation methods
 */
package qp.utils;

public class Aggregation {

    /** enumeration of different aggregation types **/
    public static final int COUNT = 0;
    public static final int AVG = 1;
    public static final int MAX = 2;
    public static final int MIN = 3;
    public static final int SUM = 4;

    int aggregationType;
    Attribute attribute;

    public Aggregation(int aggregationType) {
        if(aggregationType != Aggregation.COUNT) {
            System.out.println("Only COUNT aggregation can be applied to *");
            System.exit(1);
        } else {
            this.aggregationType = Aggregation.COUNT;
            attribute = null;
        }
    }

    public Aggregation(Attribute attribute, int aggregationType) {
        this.attribute = attribute;
        if (aggregationType == Aggregation.COUNT) {
            this.aggregationType = Aggregation.COUNT;
        } else if (aggregationType == Aggregation.AVG) {
            this.aggregationType = Aggregation.AVG;
        } else if (aggregationType == Aggregation.MAX) {
            this.aggregationType = Aggregation.MAX;
        } else if (aggregationType == Aggregation.MIN) {
            this.aggregationType = Aggregation.MIN;
        } else if (aggregationType == Aggregation.SUM) {
            this.aggregationType = Aggregation.SUM;
        } else {
            System.out.println("Unsupported aggregation type");
            System.exit(1);
        }
    }

    public void setAggregationType(int aggregationType) {
        this.aggregationType = aggregationType;
    }

    public int getAggregationType() {
        return aggregationType;
    }

    public void setAttribute(Attribute attribute) {
        this.attribute = attribute;
    }

    public Attribute getAttribute() {
        return attribute;
    }

    public Object clone() {
        Attribute newAttribute = (Attribute) attribute.clone();

        Aggregation newAggregation = new Aggregation(newAttribute, aggregationType);
        return newAggregation;
    }

    public String getName() {
        if (aggregationType == Aggregation.COUNT) {
            return "COUNT";
        } else if (aggregationType == Aggregation.AVG) {
            return "AVG";
        } else if (aggregationType == Aggregation.MAX) {
            return "MAX";
        } else if (aggregationType == Aggregation.MIN) {
            return "MIN";
        } else if (aggregationType == Aggregation.SUM) {
            return "SUM";
        } else {
            System.out.println("Unsupported aggregation type");
            System.exit(1);
        }
        return null;
    }
}
