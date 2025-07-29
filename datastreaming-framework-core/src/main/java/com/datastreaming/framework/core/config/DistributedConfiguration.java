package com.datastreaming.framework.core.config;

/**
 * Configuration for distributed tracking mode
 */
public class DistributedConfiguration {
    
    private String storageType = "redis"; // redis, database
    private String connectionString = "redis://localhost:6379";
    private String keyPrefix = "streaming:";
    private boolean enableLeaderElection = true;
    private long heartbeatIntervalMs = 30000L;
    private int maxConnectionRetries = 3;
    private long connectionTimeoutMs = 5000L;
    
    // Redis specific settings
    private RedisConfiguration redis = new RedisConfiguration();
    
    // Database specific settings (for future implementation)
    private DatabaseConfiguration database = new DatabaseConfiguration();
    
    // Getters and setters
    public String getStorageType() { return storageType; }
    public void setStorageType(String storageType) { this.storageType = storageType; }
    
    public String getConnectionString() { return connectionString; }
    public void setConnectionString(String connectionString) { this.connectionString = connectionString; }
    
    public String getKeyPrefix() { return keyPrefix; }
    public void setKeyPrefix(String keyPrefix) { this.keyPrefix = keyPrefix; }
    
    public boolean isEnableLeaderElection() { return enableLeaderElection; }
    public void setEnableLeaderElection(boolean enableLeaderElection) { this.enableLeaderElection = enableLeaderElection; }
    
    public long getHeartbeatIntervalMs() { return heartbeatIntervalMs; }
    public void setHeartbeatIntervalMs(long heartbeatIntervalMs) { this.heartbeatIntervalMs = heartbeatIntervalMs; }
    
    public int getMaxConnectionRetries() { return maxConnectionRetries; }
    public void setMaxConnectionRetries(int maxConnectionRetries) { this.maxConnectionRetries = maxConnectionRetries; }
    
    public long getConnectionTimeoutMs() { return connectionTimeoutMs; }
    public void setConnectionTimeoutMs(long connectionTimeoutMs) { this.connectionTimeoutMs = connectionTimeoutMs; }
    
    public RedisConfiguration getRedis() { return redis; }
    public void setRedis(RedisConfiguration redis) { this.redis = redis; }
    
    public DatabaseConfiguration getDatabase() { return database; }
    public void setDatabase(DatabaseConfiguration database) { this.database = database; }
    
    /**
     * Redis specific configuration
     */
    public static class RedisConfiguration {
        private String host = "localhost";
        private int port = 6379;
        private String password;
        private int database = 0;
        private int maxConnections = 10;
        private int maxIdleConnections = 5;
        private long maxWaitMs = 2000L;
        private boolean enableSsl = false;
        
        // Getters and setters
        public String getHost() { return host; }
        public void setHost(String host) { this.host = host; }
        
        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
        
        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
        
        public int getDatabase() { return database; }
        public void setDatabase(int database) { this.database = database; }
        
        public int getMaxConnections() { return maxConnections; }
        public void setMaxConnections(int maxConnections) { this.maxConnections = maxConnections; }
        
        public int getMaxIdleConnections() { return maxIdleConnections; }
        public void setMaxIdleConnections(int maxIdleConnections) { this.maxIdleConnections = maxIdleConnections; }
        
        public long getMaxWaitMs() { return maxWaitMs; }
        public void setMaxWaitMs(long maxWaitMs) { this.maxWaitMs = maxWaitMs; }
        
        public boolean isEnableSsl() { return enableSsl; }
        public void setEnableSsl(boolean enableSsl) { this.enableSsl = enableSsl; }
    }
    
    /**
     * Database specific configuration (for future implementation)
     */
    public static class DatabaseConfiguration {
        private String jdbcUrl;
        private String username;
        private String password;
        private String driverClassName = "org.postgresql.Driver";
        private int maxPoolSize = 10;
        private int minIdleConnections = 2;
        private long connectionTimeoutMs = 30000L;
        
        // Getters and setters
        public String getJdbcUrl() { return jdbcUrl; }
        public void setJdbcUrl(String jdbcUrl) { this.jdbcUrl = jdbcUrl; }
        
        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }
        
        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
        
        public String getDriverClassName() { return driverClassName; }
        public void setDriverClassName(String driverClassName) { this.driverClassName = driverClassName; }
        
        public int getMaxPoolSize() { return maxPoolSize; }
        public void setMaxPoolSize(int maxPoolSize) { this.maxPoolSize = maxPoolSize; }
        
        public int getMinIdleConnections() { return minIdleConnections; }
        public void setMinIdleConnections(int minIdleConnections) { this.minIdleConnections = minIdleConnections; }
        
        public long getConnectionTimeoutMs() { return connectionTimeoutMs; }
        public void setConnectionTimeoutMs(long connectionTimeoutMs) { this.connectionTimeoutMs = connectionTimeoutMs; }
    }
}