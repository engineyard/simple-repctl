require File.expand_path('../mysql_admin.rb', __FILE__)
require File.expand_path('../config.rb', __FILE__)

# Thor has the 'helpful' property that any Thor task gets executed only
# once.  The 'Helpers' module makes many Thor tasks available as ordinary
# Ruby functions for internal use.  The corresponding Thor tasks usually
# delegate to the corresponding helper function with 'super'.
module Helpers
  
  include MySQLAdmin::Config
  
  def start(instance)
    say "Starting instance #{instance}.", :green
    do_start(instance)
  end
  
  def start_all
    MySQLAdmin::Servers.all_instances.each do |instance|
      start(instance)
    end
  end
  
  def stop(instance)
    say "Stopping instance #{instance}.", :green
    do_stop(instance)
  end
  
  def stop_all
    MySQLAdmin::Servers.all_instances.each do |instance|
      stop(instance)
    end
  end
  
  def config(instance)
    say "Initializing new data directory for instance #{instance}."
    datadir = "#{DATA_HOME}/data#{instance}"
    remove_dir datadir
    cmd = "./scripts/mysql_install_db --datadir=#{datadir} " +
      "--user=_mysql --relay-log=tethys-relay-bin"
    inside MYSQL_HOME do
      run(cmd, :verbose => true, :capture => true)
    end
  end
  
  def config_all
    MySQLAdmin::Servers.all_instances.each do |instance|
       config(instance)
     end
  end
  
  def reset(instance)
    stop(instance)
    config(instance)
    start(instance)
  end

  def reset_all
    MySQLAdmin::Servers.all_instances.each do |instance|
       reset(instance)
     end
  end
  
  def change_master(master, slave, file, position)
    say "Changing master: master = #{master}, slave = #{slave}, file = #{file}, position = #{position}"
    do_change_master(master, slave, :file => file, :position => position)
  end
      
  def start_slave(slave)
    say "Starting slave #{slave}", :green
    run_mysql_query(slave, "START SLAVE")
  end
  
  def crash(instance)
    say "Crashing instance #{instance}", :red
    do_crash(instance)
  end

  def repl_user(instance)
    say "Creating replication account on instance #{instance}.", :green
    do_repl_user(instance)
  end

  def cluster_user(instance = 1)
    say "Installing cluster user for instance #{instance}.", :green
    do_cluster_user(instance)
  end
      
end # Module helpers

class Mysql < Thor
  
  include Thor::Actions
  include MySQLAdmin::Config
  include MySQLAdmin::Commands
  include MySQLAdmin::Servers
  include Helpers
  
  desc "start INSTANCE", "Ensure that the given MySQL server instance is running."
  def start(instance)
    super
  end
  
  desc "start_all", "Start all the MySQL instances."
  def start_all
    super
  end
  
  desc "stop INSTANCE", "Stop a running MySQL server instance."
  def stop(instance)
    super
  end
  
  desc "stop_all", "Stop all the MySQL servers."
  def stop_all
    super
  end
  
  desc "config INSTANCE", "Initialize the data directory for a new instance."
  def config(instance)
    super
  end
  
  desc "config_all", "Initialize the data directories for all instances."
  def config_all
    super
  end 
  
  desc "reset INSTANCE", "Remove database and restart MySQL server."
  def reset(instance)
    super
  end
  
  desc "reset_all", "Remove all databases and restart MySQL instances."
  def reset_all
    super
  end
  
  desc "start_slave SLAVE", "Issue START SLAVE on the SLAVE MySQL instance."
  def start_slave(slave)
    super
  end
  
  desc "change_master MASTER SLAVE FILE POSITION", "Execute CHANGE MASTER TO on the SLAVE."
  def change_master(master, slave, file, position)
    super
  end
  
  desc "crash INSTANCE", "Crash a running MySQL server."
  def crash(instance)
    super
  end
  
  desc "repl_user INSTANCE", "Create the replication user account on a MySQL instance."
  def repl_user(instance)
    super
  end
  
  desc "cluster_user INSTANCE", "Create the cluster user account on a MySQL instance."
  def cluster_user(instance)
    super
  end
  
  desc "status", "Show the status of replication."
   method_option :continuous, :aliases => "-c", :type => :numeric,
    :desc => "Continuous output at specified interval (in seconds)."
  method_option :servers, :aliases => "-s", :type => :array,
    :desc => "Only check the status of given servers."
  def status
    todos = options[:servers] || MySQLAdmin::Servers.all_instances
    header = sprintf("%-5s%-25s%-25s%-25s%-8s\n",
      "inst", "master", "received", "applied", "lag")
      
    loop do  
      say header, :blue
    
      todos.each do |i|
        coordinates = get_coordinates(i)
        slave_status = get_slave_status(i)
        is_slave = !(slave_status["Error"] == "MySQL server is not a slave.")
        master_file = coordinates[:file]
        master_pos =  coordinates[:position]
        if is_slave
          recv_file = slave_status["Master_Log_File"]
          recv_pos = slave_status["Read_Master_Log_Pos"]
          apply_file = slave_status["Relay_Master_Log_File"]
          apply_pos = slave_status["Exec_Master_Log_Pos"]
          lag = slave_status["Seconds_Behind_Master"]
        end
          
        format = "%-5d%16s:%-8d"
        if is_slave
          if lag
            lag = lag.to_s
          else
            lag = "-"
          end
          format += "%16s:%-8d%16s:%-8d%-8s"
          str = sprintf(format, i, master_file, master_pos, recv_file, recv_pos,
            apply_file, apply_pos, lag)
        else
          str = sprintf(format, i, master_file, master_pos)
        end
      
        say str + "\n", :yellow
      end
      break unless options[:continuous]
      sleep options[:continuous]
      say ""
    end
  end
    
  desc "dump INSTANCE [DUMPFILE]", "Dump all databases after FLUSH TABLES WITH READ LOCK"
  def dump(instance, dumpfile = DEFAULT_DUMPFILE)
    coordinates = do_dump(instance, dumpfile)
    file = coordinates[:file]
    position = coordinates[:position]
    puts "(#{file}, #{position})"
    [file, String(position)]
  end
  
  desc "restore INSTANCE [DUMPFILE]", "Restore INSTANCE from a \'mysqldump\' file DUMPFILE."
  def restore(slave, dumpfile = DEFAULT_DUMPFILE)
    do_restore(slave, dumpfile)
  end
  
end

class Utils < Thor
  
  include Thor::Actions
  include MySQLAdmin::Config
  include MySQLAdmin::Commands
  include MySQLAdmin::Servers
  include Helpers
  
  DEFAULT_MASTER = 1
  
  desc "bench [INSTANCE] [PROPS]", "Run the Tungsten Bristlecone benchmarker.
  The INSTANCE specifies the instance to perform all operations to, and PROPS
  is the properties file to use. The INSTANCE defaults to #{DEFAULT_MASTER} and
  the properties file defaults to #{BENCHMARK_PROPERTIES}."
  def bench(instance = DEFAULT_MASTER, props = nil)
    props ||= BENCHMARK_PROPERTIES
    invoke :create_db, [instance, "widgets"]
    run("#{BENCHMARK} -props #{props}", :verbose => true, :capture => false)
  end
  
  desc "create_db [INSTANCE] [DBNAME]", <<-EOS
    "Create a database on a MySQL instance.  INSTANCE defaults to DEFAULT_MASTER,
    and DBNAME defaults to "widgets".
  EOS
  method_option :replace, :type => :boolean, :aliases => "-r",
    :desc => "drop and recreate the database"
  def create_db(instance = DEFAULT_MASTER, dbname = "widgets")
    run_mysql_query(instance, "DROP DATABASE IF EXISTS #{dbname}") if options[:replace]
    run_mysql_query(instance, "CREATE DATABASE IF NOT EXISTS #{dbname}")
  end
  
  desc "create_tbl [INSTANCE] [DBNAME] [TBLNAME]", <<-EOS
    Create a database table. INSTANCE defaults to DEFAULT_MASTER, DBNAME defaults
    to "widgets" and TBLNAME defaults to "users".  The table schema is fixed.
  EOS
  method_option :replace, :type => :boolean, :aliases => "-r",
    :desc => "drop and recreate the table"
  def create_tbl(instance = DEFAULT_MASTER, dbname = "widgets", tblname = "users")
    invoke :create_db, [instance, dbname], :replace => false
    run_mysql_query(instance, 
      "DROP TABLE IF EXISTS #{dbname}.#{tblname}") if options[:replace]
    cmd = <<-EOS
      CREATE TABLE #{dbname}.#{tblname} (
        id	INT NOT NULL,
        last_name CHAR(30) NOT NULL,
        first_name CHAR(30) NOT NULL,
        credentials VARCHAR(32768) NOT NULL,
        PRIMARY KEY (id),
        INDEX name (last_name,first_name)
      )
    EOS
    run_mysql_query(instance, cmd)
  end
  
  desc "gen_rows [INSTANCE], [DBNAME], [TBLNAME]", <<-EOS
    Add rows to a table that was created by "utils:create_tbl". INSTANCE defaults
    to DEFAULT_MASTER, DBNAME defaults to "widgets", and TBLNAME defaults to "users".
  EOS
  method_option :delay, :type => :numeric, :aliases => "-d", :default => 0, 
    :desc => "sleep for the specified number of milliseconds between row inserts."
    method_option :count, :type => :numeric, :aliases => "-c", :default => 1000, 
       :desc => "number of rows to insert"
    method_option :size, :type => :numeric, :aliases => "-s", :default => 100, 
      :desc => "the approximate size of the record to insert (in bytes)."
    method_option :forever, :type => :boolean, :aliases => "-f",
      :desc => "run forever, ignoring COUNT option."
    method_option :verbose, :type => :boolean, :aliases => "-v",
      :desc => "print a '.' for each row inserted."
  def gen_rows(instance = DEFAULT_MASTER, dbname = "widgets", tblname = "users")
    invoke :create_tbl, [instance, dbname], :replace => true
    size = options[:size]
    size ||= 100
    size = [size, 32768].min
    data = IO.read("#{Mysql::DATA_HOME}/words.txt", size)
    id = 1
    count = 0
    
    loop do
      cmd = <<-EOS
        INSERT INTO #{dbname}.#{tblname} VALUES (
          #{id},
          'Fillmore',
          'Millard',
          '#{data}'
        )
      EOS
      run_mysql_query(instance, cmd)
      putc "." if options[:verbose]
      id += 1
      count += 1
      break if (count >= options[:count] and (not options[:forever]))
      msecs = options[:delay]
      sleep(msecs / 1000.0) if msecs > 0
    end
    
  end
  
end

class Setup < Thor
  
  include ::Thor::Actions
  include MySQLAdmin::Config
  include MySQLAdmin::Commands
  include MySQLAdmin::Servers
  include Helpers
  
  #
  # Setting Up Replication with New Master and Slaves.
  # Here, we stop all MySQL servers, remove the data directories, reinitialize
  # the data directories, restart the servers, and set up a master/slave
  # relationship.
  #
  desc "repl_pair MASTER SLAVE",
    "Set up a single master/slave replication pair from the very beginning."
  def repl_pair(master, slave)
    say "master is #{master}, slave is #{slave}", :green
    reset(master)
    reset(slave)
    cluster_user(master)
    repl_user(master)
    coordinates = get_coordinates(master)
    file = coordinates[:file]
    position = coordinates[:position]
    say "File is #{file}, Position is #{position}", :green
    change_master(master, slave, file, position)
    start_slave(slave)
  end
    
  #
  # Setting Up Replication with Existing Data using the 'mysqldump' utility. The
  # master has existing data.
  #
  desc "add_slave MASTER SLAVE", "Master has some data that is used to initialize the slave."
  method_option :populate, :type => :boolean, :default => false
  def add_slave(master, slave)
    reset(slave)
    
    if options[:populate]
      invoke "utils:bench", [master, "/opt/MySQL/b2.properties"]
    end
    file, position = invoke "mysql:dump", [master]
    
    # Slave is running, but it is not yet configured as a slave.
    # Load slave from the dump file...
    invoke "mysql:restore", [slave]
    
    change_master(master, slave, file, position)
    start_slave(slave)
  end
  
end

