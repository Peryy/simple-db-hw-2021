package simpledb.execution;

import simpledb.storage.Field;
import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    private JoinPredicate p;
    private OpIterator child1;
    private OpIterator child2;
    private Tuple child1Tuple = null;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return child1.getTupleDesc().getFieldName(p.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return child2.getTupleDesc().getFieldName(p.getField2());
    }

    /**
     * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(child1.getTupleDesc(),child2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        child1.open();
        child2.open();
    }

    public void close() {
        // some code goes here
        child1.close();
        child2.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child1.rewind();
        child2.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        TupleDesc tupleDesc = getTupleDesc();
        Tuple joinTuple = new Tuple(tupleDesc);
        while (child1.hasNext()) {
            if (!child2.hasNext()||child1Tuple == null){
                child1Tuple = child1.next();
                System.out.printf("child1\n");
                System.out.println(child1Tuple.toString());}
            while (child2.hasNext()) {
                Tuple child2Tuple = child2.next();
                System.out.println(child2Tuple.toString());
                if (p.filter(child1Tuple, child2Tuple)) {
                    int idx = 0;
                    Iterator<Field> child1Fields = child1Tuple.fields();
                    while (child1Fields.hasNext()) {
                        joinTuple.setField(idx, child1Fields.next());
                        idx++;
                    }
                    Iterator<Field> child2Fields = child2Tuple.fields();
                    while (child2Fields.hasNext()) {
                        joinTuple.setField(idx, child2Fields.next());
                        idx++;
                    }

                    return joinTuple;
                }
            }
            child2.rewind();
            child1Tuple = null;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    }

}
