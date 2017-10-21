package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.util.*;


public class SortOperator  {
  private Database.Transaction transaction;
  private String tableName;
  private Comparator<Record> comparator;
  private Schema operatorSchema;
  private int numBuffers;

  public SortOperator(Database.Transaction transaction, String tableName, Comparator<Record> comparator) throws DatabaseException, QueryPlanException {
    this.transaction = transaction;
    this.tableName = tableName;
    this.comparator = comparator;
    this.operatorSchema = this.computeSchema();
    this.numBuffers = this.transaction.getNumMemoryPages();
  }

  public Schema computeSchema() throws QueryPlanException {
    try {
      return this.transaction.getFullyQualifiedSchema(this.tableName);
    } catch (DatabaseException de) {
      throw new QueryPlanException(de);
    }
  }


  public class Run {
    private String tempTableName;

    public Run() throws DatabaseException {
      this.tempTableName = SortOperator.this.transaction.createTempTable(SortOperator.this.operatorSchema);
    }

    public void addRecord(List<DataBox> values) throws DatabaseException {
      SortOperator.this.transaction.addRecord(this.tempTableName, values);
    }

    public void addRecords(List<Record> records) throws DatabaseException {
      for (Record r: records) {
        this.addRecord(r.getValues());
      }
    }

    public Iterator<Record> iterator() throws DatabaseException {
      return SortOperator.this.transaction.getRecordIterator(this.tempTableName);
    }

    public String tableName() {
      return this.tempTableName;
    }
  }


  /**
   * Returns a NEW run that is the sorted version of the input run.
   * Can do an in memory sort over all the records in this run
   * using one of Java's built-in sorting methods.
   * Note: Don't worry about modifying the original run.
   * Returning a new run would bring one extra page in memory beyond the
   * size of the buffer, but it is done this way for ease.
   */
  public Run sortRun(Run run) throws DatabaseException {
    Run r = new Run();

    // Copy the run's iterator to a list
    List<Record> records = new ArrayList<>();
    run.iterator().forEachRemaining(records::add);

    // Sort the list and add to the run
    records.sort(comparator);
    r.addRecords(records);

    return r;
  }



  /**
   * Given a list of sorted runs, returns a new run that is the result
   * of merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
   * to determine which record should be should be added to the output run next.
   * It is recommended that your Priority Queue hold Pair<Record, Integer> objects
   * where a Pair (r, i) is the Record r with the smallest value you are
   * sorting on currently unmerged from run i.
   */
  public Run mergeSortedRuns(List<Run> runs) throws DatabaseException {
    Queue<Pair<Record, Integer>> pq = new PriorityQueue<>(
        ((o1, o2) -> comparator.compare(o1.getFirst(), o2.getFirst())));

    // An iterator for each of the runs
    List<Iterator<Record>> li = new ArrayList<>();
    int i = 0;

    // Prime the queue with the first element from each run
    for (Run r : runs) {
      li.add(i, r.iterator());

      if (li.get(i).hasNext()) {
        pq.add(new Pair<>(li.get(i).next(), i));
        i++;
      }
    }

    // Consume all records from runs and emit them in globally-sorted order.
    // This routine has the capability of consuming constant space, that
    // depends on how Run.addRecord() behaves (specifically whether it emits
    // elements directly to out of core storage)). As is, mergeSortedRuns()
    // has a ~constant (and small) working set, we only hold at most
    // runs.length() elements in memory at a time (NOT the total length of all
    // the contents of the runs).
    Run r = new Run();
    while (pq.size() > 0) {

      Pair<Record, Integer> nextRecord = pq.remove();
      r.addRecord(nextRecord.getFirst().getValues());

      // Pull a new record from that run's iterator if non-empty
      if (li.get(nextRecord.getSecond()).hasNext()) {
        pq.add(new Pair<>(li.get(nextRecord.getSecond()).next(),
                          nextRecord.getSecond()));
      }
    }

    return r;
  }

  /**
   * Given a list of N sorted runs, returns a list of
   * sorted runs that is the result of merging (numBuffers - 1)
   * of the input runs at a time.
   */
  public List<Run> mergePass(List<Run> runs) throws DatabaseException {
    List<Run> out = new ArrayList<>();
    int step = numBuffers - 1;

    for (int i = 0; i < runs.size(); i += step) {
      out.add(mergeSortedRuns(runs.subList(i, i+step)));
    }

    return out;
  }


  /**
   * Does an external merge sort on the table with name tableName
   * using numBuffers.
   * Returns the name of the table that backs the final run.
   */
  public String sort() throws DatabaseException {
    BacktrackingIterator<Page> pageIter = transaction.getPageIterator(tableName);
    pageIter.next(); // consume header page.
    List<Run> sortedRuns = new ArrayList<>();

    while (pageIter.hasNext()) {
      Iterator<Record> bIter =
          transaction.getBlockIterator(tableName, pageIter, numBuffers-1);
      List<Record> records = new ArrayList<>();
      bIter.forEachRemaining(records::add);
      Run unsortedRun = new Run() {{
        addRecords(records);
      }};

      sortedRuns.add(sortRun(unsortedRun));
    }

    while (sortedRuns.size() > 1) {
      sortedRuns = mergePass(sortedRuns);
    }

    return sortedRuns.get(0).tableName();
  }

  public Run createRun() throws DatabaseException {
    return new Run();
  }
}
