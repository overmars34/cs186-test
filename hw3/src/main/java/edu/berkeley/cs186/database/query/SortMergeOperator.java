package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordIterator;

import java.util.*;

public class SortMergeOperator extends JoinOperator {

  public SortMergeOperator(QueryOperator leftSource,
           QueryOperator rightSource,
           String leftColumnName,
           String rightColumnName,
           Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new SortMergeOperator.SortMergeIterator();
  }


  /**
  * An implementation of Iterator that provides an iterator interface for this operator.
  */
  private class SortMergeIterator extends JoinIterator {
    private final RecordIterator leftIter;
    private final RecordIterator rightIter;
    private Record leftRecord;
    private Record nextRecord;
    private int rightRecord;
    private final Comparator<Record> cmp = (o1, o2) ->
        o1.getValues().get(getLeftColumnIndex())
          .compareTo(o2.getValues().get(getRightColumnIndex()));

    public SortMergeIterator() throws QueryPlanException, DatabaseException {
      super();

      Comparator<Record> lcmp =
          Comparator.comparing(o -> o.getValues().get(getLeftColumnIndex()));
      Comparator<Record> rcmp =
          Comparator.comparing(o -> o.getValues().get(getRightColumnIndex()));

      SortOperator left = new SortOperator(getTransaction(),
                                           getLeftTableName(),
                                           lcmp);
      SortOperator right = new SortOperator(getTransaction(),
                                            getRightTableName(),
                                            rcmp);
      leftIter = getRecordIterator(left.sort());
      rightIter = getRecordIterator(right.sort());
      rightRecord = -1;
    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      if (nextRecord != null) { return true; }
      if (!((leftIter.hasNext() || leftRecord != null))) {
        return false;
      }

      if (rightRecord == 0) {
        rightIter.mark();
        rightRecord = 1;
      }

      Record left = leftRecord != null ? leftRecord : leftIter.next();
      Record right;

      if (!rightIter.hasNext()) {
        if (leftIter.hasNext()) {
          leftRecord = leftIter.next();
        } else {
          return false;
        }

        rightIter.reset();
        return hasNext();
      } else {
        right = rightIter.next();
      }

      // Bootstrapping
      if (rightRecord == -1) {
        rightRecord = 1;
        rightIter.mark();
      }

      if (leftRecord == null) {
        leftRecord = left;
      }

      while (cmp.compare(left, right) != 0) {
        if (cmp.compare(left, right) < 0) {
          // Need to advance left side

          if (!leftIter.hasNext()) {
            return false;
          }

          left = leftIter.next();
          leftRecord = left;
          rightIter.reset();
          right = rightIter.next();
        } else {
          // Need to advance right side

          if (!rightIter.hasNext()) {
            return false;
          }

          right = rightIter.next();
          rightRecord = 0; // Mark the right page on the next round.
        }
      }

      // Left == right so join their values
      List<DataBox> leftValues = new ArrayList<>(left.getValues());
      List<DataBox> rightValues = new ArrayList<>(right.getValues());
      leftValues.addAll(rightValues);
      nextRecord = new Record(leftValues);
      return true;
    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
      if (nextRecord != null) {
        Record out = nextRecord;
        nextRecord = null;
        return out;
      }

      throw new NoSuchElementException("next() on exhausted iter");
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
