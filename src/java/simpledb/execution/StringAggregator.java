package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private TupleDesc td;
    private static final IntField NO_GROUP = new IntField(-1);
    private final Map<Field, StringAggregator.AggResult> aggResultMap = new LinkedHashMap<>();

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if(gbfield != Aggregator.NO_GROUPING){
            this.td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }else{
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field keyfield;
        if (gbfield == Aggregator.NO_GROUPING){
            keyfield = NO_GROUP;}
        else {
            keyfield = tup.getField(gbfield);
        }
        String newValue = ((StringField) tup.getField(afield)).getValue();
        aggResultMap.computeIfAbsent(keyfield, k -> new AggResult(what)).merge(newValue);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> tuples = new ArrayList<>(aggResultMap.size());
        for (Map.Entry<Field, StringAggregator.AggResult> entry : aggResultMap.entrySet()) {
            Tuple tp = new Tuple(td);
            if(gbfield != Aggregator.NO_GROUPING){
                tp.setField(0, entry.getKey());
                tp.setField(1, new IntField(entry.getValue().getCount()));
            }else{
                tp.setField(0, new IntField(entry.getValue().getCount()));
            }
            tuples.add(tp);
        }
        return new TupleIterator(td, tuples);
    }



    private static class AggResult {
        private int count;
        private final Op op;

        private AggResult(Op op) {
            this.op = op;
        }

        public void merge(String newValue) {
            count++;
        }

        public int getCount() {
            return count;
        }
    }
}

