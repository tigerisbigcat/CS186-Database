package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.MaterializeOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.query.SortOperator;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.databox.DataBox;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SortMergeOperator extends JoinOperator {
    public SortMergeOperator(QueryOperator leftSource,
                             QueryOperator rightSource,
                             String leftColumnName,
                             String rightColumnName,
                             TransactionContext transaction) {
        super(prepareLeft(transaction, leftSource, leftColumnName),
                prepareRight(transaction, rightSource, rightColumnName),
                leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);
        this.stats = this.estimateStats();
    }

    /**
     * If the left source is already sorted on the target column then this
     * returns the leftSource, otherwise it wraps the left source in a sort
     * operator.
     */
    private static QueryOperator prepareLeft(TransactionContext transaction,
                                             QueryOperator leftSource,
                                             String leftColumn) {
        leftColumn = leftSource.getSchema().matchFieldName(leftColumn);
        if (leftSource.sortedBy().contains(leftColumn)) return leftSource;
        return new SortOperator(transaction, leftSource, leftColumn);
    }

    /**
     * If the right source isn't sorted, wraps the right source in a sort
     * operator. Otherwise, if it isn't materialized, wraps the right source in
     * a materialize operator. Otherwise, simply returns the right source. Note
     * that the right source must be materialized since we may need to backtrack
     * over it, unlike the left source.
     */
    private static QueryOperator prepareRight(TransactionContext transaction,
                                              QueryOperator rightSource,
                                              String rightColumn) {
        rightColumn = rightSource.getSchema().matchFieldName(rightColumn);
        if (!rightSource.sortedBy().contains(rightColumn)) {
            return new SortOperator(transaction, rightSource, rightColumn);
        } else if (!rightSource.materialized()) {
            return new MaterializeOperator(rightSource, transaction);
        }
        return rightSource;
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public List<String> sortedBy() {
        return Arrays.asList(getLeftColumnName(), getRightColumnName());
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     *    See lecture slides.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might be a useful reference).
     *
     */
    private class SortMergeIterator implements Iterator<Record> {
        /**
         * Some member variables are provided for guidance, but there are many possible solutions.
         * You should implement the solution that's best for you, using any member variables you need.
         * You're free to use these member variables, but you're not obligated to.
         */
        private Iterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;

        private SortMergeIterator() {
            super();
            leftIterator = getLeftSource().iterator();
            rightIterator = getRightSource().backtrackingIterator();
            rightIterator.markNext();

            if (leftIterator.hasNext() && rightIterator.hasNext()) {
                leftRecord = leftIterator.next();
                rightRecord = rightIterator.next();
            }

            this.marked = false;
        }

        /**
         * @return true if this iterator has another record to yield, otherwise
         * false
         */
        @Override
        public boolean hasNext() {
            if (this.nextRecord == null) this.nextRecord = fetchNextRecord();
            return this.nextRecord != null;
        }

        /**
         * @return the next record from this iterator
         * @throws NoSuchElementException if there are no more records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) throw new NoSuchElementException();
            Record nextRecord = this.nextRecord;
            this.nextRecord = null;
            return nextRecord;
        }


        /**
         * Returns the next record that should be yielded from this join,
         * or null if there are no more records to join.
         */
        private Record fetchNextRecord() {
            // TODO(proj3_part1): implement
            if (leftRecord == null || rightRecord == null) {
                return null;
            }
            while (true) {
                if (marked) {
                    //already marked. advance S and check for another match
                    if (rightIterator.hasNext()) {
                        //S not at EOF. advance S
                        rightRecord = rightIterator.next();
                        if (compareRecords(leftRecord, rightRecord) == 0) {
                            //another match. return it
                            return leftRecord.concat(rightRecord);
                        }
                    }
                    //S is at EOF, or advancing S does not yield a match.
                    //reset S to marked spot, advance R, and continue
                    if (leftIterator.hasNext()) {
                        //R not at EOF. advance R and continue
                        rightIterator.reset();
                        rightRecord = rightIterator.next();
                        marked = false;
                        leftRecord = leftIterator.next();
                        continue;
                    } else {
                        //R is at EOF, and S is at EOF or only has values greater than r_i remaining.
                        //return null
                        return null;
                    }
                }
                //not marked. advance R and S until a match is found
                while (compareRecords(leftRecord, rightRecord) != 0) {
                    if (compareRecords(leftRecord, rightRecord) > 0) {
                        //s_i < r_i. advance S
                        if (rightIterator.hasNext()) {
                            //S not at EOF.
                            rightRecord = rightIterator.next();
                            continue;
                        } else
                            //S at EOF, and R only has values greater than s_i remaining. return null
                            return null;
                    }
                    if (compareRecords(leftRecord, rightRecord) < 0) {
                        //r_i < s_i. advance R
                        if (leftIterator.hasNext()) {
                            //R not at EOF.
                            leftRecord = leftIterator.next();
                        } else
                            //R at EOF, and S only has values greater than r_i remaining. return null
                            return null;
                    }
                }
                if (compareRecords(leftRecord, rightRecord) != 0) {
                    //reached EOF with no matches.
                    //given the EOF conditions inside the prior loop, this may be unreachable
                    return null;
                }
                //found a match. mark S and return matching record
                rightIterator.markPrev();
                marked = true;
                return leftRecord.concat(rightRecord);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        public int compareRecords(Record l, Record r) {
            return l.getValues().get(getLeftColumnIndex()).compareTo(r.getValues().get(getRightColumnIndex()));
        }
    }
}

//            r = first tuple in R
//            s = first tuple in S
//            while (r != eof and s != eof) {
//                // align cursors to start of next common run
//                while (r != eof and r.i < s.j) { r = next tuple in R }
//                while (s != eof and r.i > s.j) { s = next tuple in S }
//                // scan common run, generating result tuples
//                while (r != eof and r.i == s.j) {
//                    ss = s   // set to start of run
//                    while (ss != eof and ss.j == r.i) {
//                        add (r,s) to result
//                        ss = next tuple in S
//                    }
//                    r = next tuple in R
//                }
//                s = ss   // start search for next run
//            }

