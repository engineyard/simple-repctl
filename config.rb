module MySQLAdmin
  
  module Config
    
    # The location of the local MySQL installation.
    MYSQL_HOME = "/usr/local/mysql"

    # Define the directory where subdirectores for data will be created
    # for each MySQL server instance.  This should agree with the 
    # per server'data_dir' property in the servers.yml file, and it should
    # also agree with the 'datadir' property in the server's configuration
    # file (my*.cnf).  
    DATA_HOME = "/opt/MySQL/instances"

    # The home directory of Continuent's open source replicator.  Eventually,
    # a command will be added to this script to switch between MySQL native
    # replication and Continuent's open-source Tungsten replicator.
    REPLICATOR_HOME = "/opt/continuent/replicator"  

    # For simplicity, we're using the load-generator/benchmarker that comes
    # in the Continuent source package.  This may be replaced with sql-bench
    # from the MySQL source distribution.
    BENCHMARK = "#{REPLICATOR_HOME}/bristlecone/bin/benchmark.sh"
    BENCHMARK_PROPERTIES = File.expand_path("../bristlecone.properties", __FILE__)

    # Set this to the directory where you want dump files to be stored.
    DUMP_DIR = "#{DATA_HOME}/dump"

    # The default name of the dump file in the DUMP_DIR directory.
    DEFAULT_DUMPFILE = "dbdump.db"

    # User name and password for the replication account, used only internally by
    # the replication processes.
    REPLICATION_USER = "repl"
    REPLICATION_PASSWORD = "secret"

    # A minor convenience. 
    DEFAULT_MASTER = 1

    # Typically, this is of the form #{HOSTNAME}-relay-bin'.
    RELAY_LOG = "tethys-relay-bin"
  end
end