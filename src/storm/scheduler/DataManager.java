/*******************************************************************************
* Copyright (c) 2013 Leonardo Aniello, Roberto Baldoni, Leonardo Querzoni.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Eclipse Public License v1.0
* which accompanies this distribution, and is available at
* http://www.eclipse.org/legal/epl-v10.html
*
* Contributors:
* Leonardo Aniello, Roberto Baldoni, Leonardo Querzoni
*******************************************************************************/
package storm.scheduler;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.log4j.Logger;

import cpuinfo.CPUInfo;

public class DataManager {

	private static DataManager instance = null;
	
	private PoolingDataSource dataSource;
	private Logger logger;
	private String nodeName;
	private int capacity; // the capacity of a node, expressed in percentage wrt the total speed
	private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	private DataManager() {
		logger = Logger.getLogger(DataManager.class);
		logger.info("Starting DataManager (working directory: " + System.getProperty("user.dir") + ")");
		
		try {
			// load configuration from file
			logger.debug("Loading configuration from file");
			Properties properties = new Properties();
			properties.load(new FileInputStream("db.ini"));
			logger.debug("Configuration loaded");
			
			// load JDBC driver
			logger.debug("Loading JDBC driver");
			String jdbc_driver = properties.getProperty("jdbc.driver").trim();
			Class.forName(jdbc_driver);
			logger.debug("Driver loaded");
			
			// set up data source
			logger.debug("Setting up pooling data source");
			String connection_uri = properties.getProperty("data.connection.uri");
			String validation_query = properties.getProperty("validation.query");
			ObjectPool connectionPool = new GenericObjectPool(null);
			ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(connection_uri, null);
			PoolableConnectionFactory poolableConnectionFactory = 
				new PoolableConnectionFactory(connectionFactory, connectionPool, null, validation_query, false, true);
			poolableConnectionFactory.hashCode();
			dataSource = new PoolingDataSource(connectionPool);
			logger.debug("Data source set up");
			
			nodeName = properties.getProperty("node-name");
			if (properties.getProperty("capacity") != null) {
				capacity = Integer.parseInt(properties.getProperty("capacity"));
				if (capacity < 1 || capacity > 100)
					throw new RuntimeException("Wrong capacity: " + capacity + ", expected in the range [1, 100]");
			}
			
			logger.info("DataManager started");
		} catch (Exception e) {
			logger.error("Error starting DataManager", e);
		}
	}
	
	public static synchronized DataManager getInstance() {
		if (instance == null)
			instance = new DataManager();
		return instance;
	}
	
	public static String formatDate(Calendar c) {
		return dateFormat.format(new Date(c.getTimeInMillis()));
	}
	
	private Connection getConnection() throws Exception {
		return dataSource.getConnection();
	}
	
	public int getTopologyId(String stormId) throws Exception {
		return checkTopology(stormId);
	}
	
	public int checkTopology(String stormId) throws Exception {
		Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;
		int id = -1;
		logger.debug("Going to check topology " + stormId);
		try {
			connection = getConnection();
			connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			connection.setAutoCommit(false);
			statement = connection.createStatement();
			
			String sql = "select id from topology where storm_id = '" + stormId + "'";
			logger.debug("SQL query: " + sql);
			resultSet = statement.executeQuery(sql);
			if (resultSet.next()) {
				id = resultSet.getInt(1);
				logger.debug("Topology found, id: " + id);
			} else {
				logger.debug("Topology not found, let's create it");
				resultSet.close();
				sql = "insert into topology(storm_id) values('" + stormId + "')";
				logger.debug("SQL script: " + sql);
				statement.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
				logger.debug("Retrieving generated id...");
				resultSet = statement.getGeneratedKeys();
				if (resultSet.next()) {
					id = resultSet.getInt(1);
					logger.debug("Ok, id: " + id);
				} else {
					throw new Exception("Cannot retrieve generated key");
				}
			}
			
			connection.commit();
			
		} catch (Exception e) {
			connection.rollback();
			logger.error("An error occurred checking a topology", e);
			throw e;
		} finally {
			if (resultSet != null)
				resultSet.close();
			if (statement != null)
				statement.close();
			if (connection != null)
				connection.close();
		}
		return id;
	}
	
	public long getTotalLoad(String topologyID) throws Exception {
		Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;
		long totalLoad = -1;
		logger.debug("Going to get total load of topology " + topologyID);
		try {
			connection = getConnection();
			statement = connection.createStatement();
			String sql = "select sum(`load`.`load`) from `load` join topology on `load`.topology_id = topology.id where topology.storm_id = '" + topologyID + "'";
			logger.debug("SQL script: " + sql);
			resultSet = statement.executeQuery(sql);
			if (resultSet.next())
				totalLoad = resultSet.getLong(1);
			else
				throw new Exception("Cannot find topology " + topologyID + " in the DB");
		} catch (Exception e) {
			logger.error("An error occurred getting total load for topology " + topologyID, e);
			throw e;
		} finally {
			if (resultSet != null)
				resultSet.close();
			if (statement != null)
				statement.close();
			if (connection != null)
				connection.close();
		}
		logger.info("Total load for topology " + topologyID + ": " + totalLoad + " Hz/s");
		return totalLoad;
	}
	
	public void storeLoad(String stormId, int beginTask, int endTask, long load) throws Exception {
		Connection connection = null;
		Statement statement = null;
		logger.debug("Going to store load stat (topology: " + stormId + ", executor: [" + beginTask + ", " + endTask + "], load: " + load + " CPU cycles per second)");
		try {
			connection = getConnection();
			statement = connection.createStatement();
			
			String sql = "update `load` set `load` = " + load + ", node = '" + nodeName + "' where topology_id = " + getTopologyId(stormId) + " and begin_task = " + beginTask + " and end_task = " + endTask;
			logger.debug("SQL script: " + sql);
			if (statement.executeUpdate(sql) == 0) {
				sql = "insert into `load`(topology_id, begin_task, end_task, `load`, node) " +
					"values(" + getTopologyId(stormId) + ", " + beginTask + ", " + endTask + ", " + load + ", '" + nodeName + "')";
				logger.debug("SQL script: " + sql);
				statement.executeUpdate(sql);
			}
		} catch (Exception e) {
			logger.error("An error occurred storing a load stat", e);
			throw e;
		} finally {
			if (statement != null)
				statement.close();
			if (connection != null)
				connection.close();
		}
	}
	
	/*public void storeAcker(String stormId, String nodeName) throws Exception {
		Connection connection = null;
		Statement statement = null;
		logger.debug("Going to store load stat (topology: " + stormId + ", executor: [" + Utils.ACKER_TAKS_ID + ", " + Utils.ACKER_TAKS_ID + "], load: " + 0 + " CPU cycles per second");
		try {
			connection = getConnection();
			statement = connection.createStatement();
			
			String sql = "update `load` set `load` = " + 0 + ", node = '" + nodeName + "' where topology_id = " + getTopologyId(stormId) + " and begin_task = " + Utils.ACKER_TAKS_ID + " and end_task = " + Utils.ACKER_TAKS_ID;
			logger.debug("SQL script: " + sql);
			if (statement.executeUpdate(sql) == 0) {
				sql = "insert into `load`(topology_id, begin_task, end_task, `load`, node) " +
					"values(" + getTopologyId(stormId) + ", " + Utils.ACKER_TAKS_ID + ", " + Utils.ACKER_TAKS_ID + ", " + 0 + ", '" + nodeName + "')";
				logger.debug("SQL script: " + sql);
				statement.executeUpdate(sql);
			}
		} catch (Exception e) {
			logger.error("An error occurred storing a load stat", e);
			throw e;
		} finally {
			if (statement != null)
				statement.close();
			if (connection != null)
				connection.close();
		}
	}*/
	
	public void storeTraffic(String stormId, int sourceTask, int destinationTask, int traffic) throws Exception {
		Connection connection = null;
		Statement statement = null;
		logger.debug("Going to store traffic stat (topology: " + stormId + ", source: " + sourceTask + ", destination: " + destinationTask + ", traffic: " + traffic + " tuples per second)");
		try {
			connection = getConnection();
			statement = connection.createStatement();
			
			String sql = "update traffic set traffic = " + traffic + " where topology_id = " + getTopologyId(stormId) + " and source_task = " + sourceTask + " and destination_task = " + destinationTask;
			logger.debug("SQL script: " + sql);
			if (statement.executeUpdate(sql) == 0) {
				sql = "insert into traffic(topology_id, source_task, destination_task, traffic) " +
						"values(" + getTopologyId(stormId) + ", " + sourceTask + ", " + destinationTask + ", " + traffic + ")";
				logger.debug("SQL script: " + sql);
				statement.executeUpdate(sql);
			}
		} catch (Exception e) {
			logger.error("An error occurred storing a traffic stat", e);
			throw e;
		} finally {
			if (statement != null)
				statement.close();
			if (connection != null)
				connection.close();
		}
	}
	
	public void StoreAssignment(String topologies, String assignment) throws Exception {
		Connection connection = null;
		Statement statement = null;
		logger.debug("Going to store an assignment (topologies: " + topologies + ", assignment: " + assignment + ")");
		try {
			connection = getConnection();
			statement = connection.createStatement();
			
			String sql = "insert into assignment(time, topologies, assignment) values(" + System.currentTimeMillis() + ", '" + topologies + "', '" + assignment + "')";
			logger.debug("SQL script: " + sql);
			statement.execute(sql);
		} catch (Exception e) {
			logger.error("An error occurred storing an assignment", e);
			throw e;
		} finally {
			if (statement != null)
				statement.close();
			if (connection != null)
				connection.close();
		}
	}
	
	public List<String> getTopologies() throws Exception {
		Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;
		List<String> topologies = new ArrayList<String>();
		logger.debug("Going to retrieve the list of topologies IDs");
		try {
			connection = getConnection();
			statement = connection.createStatement();
			String sql = "select storm_id from topology";
			logger.debug("SQL script: " + sql);
			resultSet = statement.executeQuery(sql);
			while (resultSet.next())
				topologies.add(resultSet.getString(1));
		} catch (Exception e) {
			logger.error("An error occurred retrieving topologies", e);
			throw e;
		} finally {
			if (resultSet != null)
				resultSet.close();
			if (statement != null)
				statement.close();
			if (connection != null)
				connection.close();
		}
		return topologies;
	}
	
	public void removeTopologies(List<String> topologies) throws Exception {
		Connection connection = null;
		Statement statement = null;
		logger.debug("Going to remove these topologies: " + Utils.collectionToString(topologies));
		try {
			connection = getConnection();
			connection.setAutoCommit(false);
			statement = connection.createStatement();
			for (String topology : topologies) {
				logger.debug("Removing load stats of topology " + topology);				
				String sql = "delete from `load` where topology_id in (select id from topology where storm_id = '" + topology + "')";
				logger.debug("SQL script: " + sql);
				statement.execute(sql);
				
				logger.debug("Removing traffic stats of topology " + topology);				
				sql = "delete from traffic where topology_id in (select id from topology where storm_id = '" + topology + "')";
				logger.debug("SQL script: " + sql);
				statement.execute(sql);
				
				logger.debug("Removing topology " + topology);				
				sql = "delete from topology where storm_id = '" + topology + "'";
				logger.debug("SQL script: " + sql);
				statement.execute(sql);
			}
			connection.commit();
		} catch (Exception e) {
			logger.error("An error occurred removing topologies", e);
			connection.rollback();
			throw e;
		} finally {
			if (statement != null)
				statement.close();
			if (connection != null) {
				connection.setAutoCommit(true);
				connection.close();
			}
		}
	}
	
	public void checkNode(long totalSpeed) throws Exception {
		Connection connection = null;
		Statement statement = null;
		try {
			connection = getConnection();
			statement = connection.createStatement();
			long absoluteCapacity = totalSpeed / 100 * capacity;
			String sql = "update node set capacity = " + absoluteCapacity + " where name = '" + nodeName + "'";
			logger.debug("SQL script: " + sql);
			if (statement.executeUpdate(sql) == 0) {
				sql = "insert into node(name, capacity, cores) values('" + nodeName + "', " + totalSpeed + ", " + CPUInfo.getInstance().getNumberOfCores() + ")";
				logger.debug("SQL script: " + sql);
				statement.execute(sql);
			}
		} catch (Exception e) {
			logger.error("An error occurred checking the node", e);
			throw e;
		} finally {
			if (statement != null)
				statement.close();
			if (connection != null)
				connection.close();
		} 
	}
	
	public Map<String, Node> getNodes() throws Exception {
		Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;
		Map<String, Node> nodeMap = new HashMap<String, Node>();
		try {
			connection = getConnection();
			statement = connection.createStatement();
			String sql = "select name, capacity, cores from node";
			logger.debug("SQL script: " + sql);
			resultSet = statement.executeQuery(sql);
			while (resultSet.next()) {
				String name = resultSet.getString(1);
				long capacity = resultSet.getLong(2);
				int cores = resultSet.getInt(3);
				nodeMap.put(name, new Node(name, capacity, cores));
			}
			int nodeCount = nodeMap.keySet().size();
			for (Node node : nodeMap.values())
				node.setNodeCount(nodeCount);
		} catch (Exception e) {
			logger.error("An error occurred getting the nodes", e);
			throw e;
		} finally {
			if (resultSet != null)
				resultSet.close();
			if (statement != null)
				statement.close();
			if (connection != null)
				connection.close();
		}

		return nodeMap;
	}

	public String getNodeName() {
		return nodeName;
	}
	
	/**
	 * @param topologyID
	 * @return the list of communicating executor pairs, sorted by traffic in decreasing order 
	 * @throws Exception
	 */
	public List<ExecutorPair> getInterExecutorTrafficList(String topologyID) throws Exception {
		Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;
		List<ExecutorPair> trafficStat = new ArrayList<ExecutorPair>();
		try {
			connection = getConnection();
			statement = connection.createStatement();
			
			// load executors
			List<Executor> executorList = new ArrayList<Executor>();
			String sql = "select begin_task, end_task, `load` from `load` join topology on `load`.topology_id = topology.id where storm_id = '" + topologyID + "'";
			logger.debug("SQL script: " + sql);
			resultSet = statement.executeQuery(sql);
			while (resultSet.next()) {
				Executor executor = new Executor(resultSet.getInt(1), resultSet.getInt(2));
				executor.setLoad(resultSet.getLong(3));
				executor.setTopologyID(topologyID);
				executorList.add(executor);
			}
			resultSet.close();
			logger.debug("Executor list for topology " + topologyID + ": " + Utils.collectionToString(executorList));
			
			// load tasks and create the list the executor pairs sorted by traffic desc
			// Map<Long, ExecutorPair> executorPairMap = new HashMap<Long, ExecutorPair>(); // for lookups
			// Map<Integer, ExecutorPair> executorPairMap = new HashMap<Integer, ExecutorPair>(); // for lookups
			sql = "select source_task, destination_task, traffic from traffic";
			logger.debug("SQL script: " + sql);
			resultSet = statement.executeQuery(sql);
			while (resultSet.next()) {
				// load data from DB
				int sourceTask = resultSet.getInt(1);
				int destinationTask = resultSet.getInt(2);
				int traffic = resultSet.getInt(3);
				
				// look up executor pair
				Executor source = Utils.getExecutor(sourceTask, executorList);
				logger.debug("source executor for source task " + sourceTask + ": " + source);
				Executor destination = Utils.getExecutor(destinationTask, executorList);
				logger.debug("destination executor for destination task " + destinationTask + ": " + destination);
				
				if (source != null && destination != null) {
					// long key = ExecutorPair.getKey(source, destination);
					// int key = new ExecutorPair(source, destination).hashCode();
					// ExecutorPair pair = executorPairMap.get(key);
					ExecutorPair pair = null;
					for (ExecutorPair tmp : trafficStat)
						if (tmp.getSource().equals(source) && tmp.getDestination().equals(destination)) {
							pair = tmp;
							break;
						}
					if (pair == null) {
						pair = new ExecutorPair(source, destination);
						// executorPairMap.put(key, pair);
						trafficStat.add(pair); // it's right to add it to the tail
					}
					
					// update its traffic and sort the list
					pair.addTraffic(traffic);
					int index = trafficStat.indexOf(pair);
					while (index > 0 && pair.getTraffic() > trafficStat.get(index - 1).getTraffic()) {
						ExecutorPair executorPair = trafficStat.remove(index - 1);
						trafficStat.add(index, executorPair);
						index--;
					}
				} else {
					// the DB has not been properly populated yet, return an empty list and wait
					trafficStat.clear();
					break;
				}
			}
			
		} catch (Exception e) {
			logger.error("An error occurred retrieving traffic stats for topology " + topologyID, e);
			throw e;
		} finally {
			if (resultSet != null)
				resultSet.close();
			if (statement != null)
				statement.close();
			if (connection != null)
				connection.close();
		}
		return trafficStat;
	}
	
	/**
	 * @return the list of nodes such that the total load due to Storm executors is higher than the pre-configured node capacity
	 * @throws Exception
	 */
	public List<Node> getOverloadedNodes() throws Exception {
		Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;
		List<Node> nodeList = new ArrayList<Node>();
		try {
			connection = getConnection();
			statement = connection.createStatement();
			String sql =
				"select `load`.node, sum(`load`) as total_load, node.capacity, node.cores " +
				"from `load` join node on `load`.node = node.name " +
				"group by node.name " +
				"having total_load > node.capacity";
			logger.debug("SQL script: " + sql);
			resultSet = statement.executeQuery(sql);
			while (resultSet.next()) {
				String name = resultSet.getString(1);
				long load = resultSet.getLong(2);
				long capacity = resultSet.getLong(3);
				int cores = resultSet.getInt(4);
				Node node = new Node(name, capacity, cores);
				node.addLoad(load);
				nodeList.add(node);
			}
		} catch (Exception e) {
			logger.error("An error occurred loading overloaded nodes");
			throw e;
		} finally {
			if (resultSet != null)
				resultSet.close();
			if (statement != null)
				statement.close();
			if (connection != null)
				connection.close();
		}
		return nodeList;
	}
	
	public int getCurrentInterNodeTraffic() throws Exception {
		Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;
		int currentInterNodeTraffic = 0;
		try {
			connection = getConnection();
			statement = connection.createStatement();
			
			// load executors
			List<Executor> executorList = new ArrayList<Executor>();
			String sql = "select begin_task, end_task, `load`, node from `load`";
			logger.debug("SQL script: " + sql);
			resultSet = statement.executeQuery(sql);
			while (resultSet.next()) {
				Executor executor = new Executor(resultSet.getInt(1), resultSet.getInt(2));
				executor.setLoad(resultSet.getLong(3));
				executor.setNode(resultSet.getString(4));
				executorList.add(executor);
			}
			resultSet.close();
			logger.debug("Executor list: " + Utils.collectionToString(executorList));
			
			// load tasks and create the list the executor pairs sorted by traffic desc
			sql = "select source_task, destination_task, traffic from traffic";
			logger.debug("SQL script: " + sql);
			resultSet = statement.executeQuery(sql);
			while (resultSet.next()) {
				// load data from DB
				int sourceTask = resultSet.getInt(1);
				int destinationTask = resultSet.getInt(2);
				int traffic = resultSet.getInt(3);
				
				// look up executor pair
				Executor source = Utils.getExecutor(sourceTask, executorList);
				logger.debug("source executor for source task " + sourceTask + ": " + source);
				Executor destination = Utils.getExecutor(destinationTask, executorList);
				logger.debug("destination executor for destination task " + destinationTask + ": " + destination);
				
				if (source != null && destination != null && !source.getNode().equals(destination.getNode())) {
					logger.debug(
						"Tasks " + sourceTask + " and " + destinationTask + 
						" are currently deployed on distinct nodes, so they contribute to inter-node traffic for " + traffic + " tuple/s");
					currentInterNodeTraffic += traffic;
				}
			}

		} catch (Exception e) {
			logger.error("An error occurred computing current inter-node traffic", e);
			throw e;
		} finally {
			if (resultSet != null)
				resultSet.close();
			if (statement != null)
				statement.close();
			if (connection != null)
				connection.close();
		}
		return currentInterNodeTraffic;
	}
}
