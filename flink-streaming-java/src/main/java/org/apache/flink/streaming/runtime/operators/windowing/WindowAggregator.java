package org.apache.flink.streaming.runtime.operators.windowing;

import java.io.Serializable;
import java.util.List;

public interface WindowAggregator<T> extends Serializable {

    /**
     * It adds a new node in the aggregation buffer with the given id and value
     * @param id
     * @param val
     * @throws Exception
     */
    public void add(int id, T val) throws Exception;
    
    /**
     * It updates a node in the aggregation buffer with the given id and value
     * @param id
     * @param val
     * @throws Exception
     */
    public void update(int id, T val) throws Exception;

    /**
     * It adds all values given and maps them with the given ids. Mind that ids and vals should have the same size
     * since they are 1-1 mapped
     * @param ids
     * @param vals
     * @throws Exception
     */
    public void add(List<Integer> ids, List<T> vals) throws Exception;
    
    /**
     * It evicts the elements with the given ids. Currently only FIFO evictions are possible so a discretizer
     * should always invoke removals consecutively from id==HEAD.
     * 
     * @param ids
     * @throws Exception
     */
    public void remove(Integer... ids) throws Exception;

    /**
     * It evicts all elements in FIFO order up to the given id
     * @param id
     * @throws Exception
     */
    public void removeUpTo(int id) throws Exception;
    
    /**
     * Returns the aggregate of the window buffer from startid to the front of the buffer
     * @param startid
     * @return
     */
    public T aggregate(int startid) throws Exception;

    /**
     * Returns the aggregate of the window buffer from startid to the endid of the buffer
     * @param startid
     * @param endid
     * @return
     */
    public T aggregate(int startid, int endid) throws Exception;
    
    /**
     * Returns a full aggregate for the whole window buffer
     * @return
     */
    public T aggregate() throws Exception;

	int getNumberOfPartials();
         
}