require 'mysql2'
require 'fileutils'
require 'delegate'
require 'open3'

require File.expand_path('../config.rb', __FILE__)
require File.expand_path('../servers.rb', __FILE__)

module MySQLAdmin
    
  class Client < DelegateClass(Mysql2::Client)
    
    @@clients = {}
    
    def initialize(instance)
      @instance = instance
      server = Servers.instance_to_server(@instance)
      @client = Mysql2::Client.new(:host => server[:hostname],
        :username => "root", :port => server[:port])
      super(@client)
    end
    
    def self.open(instance, timeout = 60)
      begin
        instance = Integer(instance)
      rescue Mysql2::Error => e 
        puts "Instance value <#{instance}> is invalid."
      else
        timeout = Integer(timeout)
        while timeout >= 0
          begin
            @@clients[instance] ||= Client.new(instance)
            break
          rescue Mysql2::Error => e
            puts "#{e.message}, retrying connection to instance #{instance}..."
            sleep 1
            timeout -= 1
          end
        end    
      end
      @@clients[instance]
    end    
    
    def close
      @@clients[@instance] = nil
      @client.close
    end
    
    def reset
      @client.close
      @@clients[@instance] = nil
      Client.open(@instance)
    end
    
  end

  module Commands
    
    include FileUtils
    include MySQLAdmin::Config
        
    def do_start(instance)
      pid = get_mysqld_pid(instance)
      if pid
        puts "Instance #{instance} with PID #{pid} is already running."
      else 
        pid = fork()
        unless pid
          # We're in the child.
          puts "Starting instance #{instance} with PID #{Process.pid}."
          exec ["#{MYSQL_HOME}/bin/mysqld", "mysqld"], 
            "--defaults-file=#{defaults_file(instance)}", "--user=_mysql",
            "--relay-log=#{RELAY_LOG}"
        end
      end
    end
    
    def do_stop(instance)
      cmd = "#{MYSQL_HOME}/bin/mysqladmin " +
        "--defaults-file=#{defaults_file(instance)} " +
        "-u root shutdown"
      
      pid = get_mysqld_pid(instance)
      if pid
        puts "Stopping instance #{instance} with pid #{pid}."
        run_cmd(cmd, true)
      else
        puts "Instance #{instance} is not running." 
      end
    end
        
    def do_status(instance)
      status = get_coordinates(instance)
      puts status
    end

    #
    # Treat the instance as a slave and
    # process the output of "SHOW SLAVE STATUS".
    #
    def get_slave_status(instance)
      keys = [
        "Instance",
        "Error",
        "Slave_IO_State",
        "Slave_IO_Running",
        "Slave_SQL_Running",
        "Last_IO_Error",
        "Last_SQL_Error",
        "Seconds_Behind_Master",
        "Master_Log_File",
        "Read_Master_Log_Pos",
        "Relay_Master_Log_File",
        "Exec_Master_Log_Pos",
        "Relay_Log_File",
        "Relay_Log_Pos"
      ]
      results = {}
      status = do_slave_status(instance)
      keys.each do |k|
        results.merge!(k => status[k]) if (status[k] and status[k] != "")
      end
      results
    end

    def do_crash(instance) 
      pid = get_mysqld_pid(instance)
      puts "pid is #{pid}"
      if pid
        puts "Killing mysqld instance #{instance} with PID #{pid}"
        Process.kill("KILL", pid.to_i)
        while get_mysqld_pid(instance)
          puts "in looop"
          sleep 1
        end
        puts "MySQL server instance #{instance.to_i} has been killed."
      else
        puts "MySQL server instance #{instance.to_i} is not running."
      end
    end
       
    def is_master?(instance)
      get_slave_coordinates(instance).empty?
    end

    def is_slave?(instance)
      !is_master?(instance)
    end
    
    # This is an example template to create commands to issue queries.
     def do_slave_status(instance)
       client = Client.open(instance)
       if client
         results = client.query("SHOW SLAVE STATUS")
         results.each_with_index do |index, line|
           puts "#{index}: #{line}"
         end
       else
         puts "Could not open connection to MySQL instance #{instance}."
       end
     rescue Mysql2::Error => e
       puts e.message
     ensure
       client.close if client
     end

    def find_masters()
      masters = []
      Servers.all_servers.each do |s|
        masters << s if is_master?(s)
      end
      masters
    end

    #
    # From http://dev.mysql.com/doc/refman/5.0/en/lock-tables.html:
    #  
    # For a filesystem snapshot of innodb, we find that setting
    # innodb_max_dirty_pages_pct to zero; doing a 'flush tables with
    # readlock'; and then waiting for the innodb state to reach 'Main thread
    # process no. \d+, id \d+, state: waiting for server activity' is
    # sufficient to quiesce innodb.
    #
    # You will also need to issue a slave stop if you're backing up a slave
    # whose relay logs are being written to its data directory.
    # 
    #
    # select @@innodb_max_dirty_pages_pct;
    # flush tables with read lock;
    # show master status; 
    # ...freeze filesystem; do backup...
    # set global innodb_max_dirty_pages_pct = 75;
    # 
    
    def do_change_master(master, slave, coordinates)
      master_server = Servers.instance_to_server(master)
      begin
        slave_connection = Client.open(slave)
        if slave_connection
          
          # Replication on the slave can't be running if we want to execute
          # CHANGE MASTER TO.  
          slave_connection.query("STOP SLAVE") rescue Mysql2::Error
          
          raise "master_server is nil" unless master_server
          
          cmd = <<-EOT
CHANGE MASTER TO
  MASTER_HOST = \'#{master_server[:hostname]}\',
  MASTER_PORT = #{master_server[:port]},
  MASTER_USER = \'#{REPLICATION_USER}\',
  MASTER_PASSWORD = \'#{REPLICATION_PASSWORD}\',
  MASTER_LOG_FILE = \'#{coordinates[:file]}\',
  MASTER_LOG_POS = #{coordinates[:position]}
EOT
          puts "Executing: #{cmd}"
          slave_connection.query(cmd)
        else
          puts "do_change_master: Could not connnect to MySQL server."
        end
      rescue Mysql2::Error => e
          puts e.message
      ensure
        slave_connection.close if slave_connection
      end
      
    end
        
    def do_dump(instance, dumpfile)
      coordinates = get_coordinates(instance) do
        cmd = "#{MYSQL_HOME}/bin/mysqldump " +
          "--defaults-file=#{defaults_file(instance)} " +
          "--all-databases --lock-all-tables > #{DUMP_DIR}/#{dumpfile}"
        run_cmd(cmd, true)
      end
      coordinates
    end
    
    def do_restore(instance, dumpfile)
      # Assumes that the instance is running, but is not acting as a slave.
      cmd = "#{MYSQL_HOME}/bin/mysql " +
        "--defaults-file=#{defaults_file(instance)} " +
        "< #{DUMP_DIR}/#{dumpfile}"
      run_cmd(cmd, true)
    end
          
    #
    # Get the status of replication for the master and all slaves.
    # Return an array of hashes, each hash has the form:
    # {:instance => <instance id>, :error => <errrmsg>, 
    #  :master_file => <binlog-file-name>, :master_position => <binlog-position>,
    #  :slave_file => <binlog-file-name>, :slave_position => <binlog-position>}
    #
    def do_slave_status(instance)
      instance ||= DEFAULT_MASTER
      locked = false
      client = Client.open(instance, 5)
      if client
        client.query("FLUSH TABLES WITH READ LOCK")
        locked = true
        results = client.query("SHOW SLAVE STATUS")
        if results.first
          results.first.merge("Instance" => instance, "Error" => "Success")
        else
          {"Instance" => instance, "Error" => "MySQL server is not a slave."}
        end
      else
        {"Instance" => instance, "Error" => "Could not connect to MySQL server."}
      end
    rescue Mysql2::Error => e
      {:instance => instance, "Error" => e.message}
    ensure
      if client
        client.query("UNLOCK TABLES") if locked
        client.close
      end
    end
    
    # Get the master coordinates from a MySQL instance. Optionally,
    # run a block while holding the READ LOCK.
    def get_coordinates(instance)
      instance ||= DEFAULT_MASTER
      locked = false
      client = Client.open(instance)
      if client
        client.query("FLUSH TABLES WITH READ LOCK")
        locked = true
        results = client.query("SHOW MASTER STATUS")
        row = results.first
        coordinates = if row
          {:file => row["File"], :position => row["Position"]}
        else
          {}
        end
        yield coordinates if block_given?
        # You could copy data from the master to the slave at this point
      end
      coordinates
    rescue Mysql2::Error => e
      puts e.message
      # puts e.backtrace
    ensure
      if client
        client.query("UNLOCK TABLES") if locked
        client.close
      end
      # coordinates
    end

    def get_slave_coordinates(instance)
      client = Client.open(instance)
      if client
        results = client.query("SHOW SLAVE STATUS")
        row = results.first
        if row
          {:file => row["Master_Log_File"], :position => row["Read_Master_Log_Pos"]}
        else
          {}
        end
      end
    ensure
      client.close if client
    end

    def stop_slave_io_thread(instance)
      client = Client.open(instance)
      if client 
        client.query("STOP SLAVE IO_THREAD")
      end
    ensure
      client.close if client
    end

    def run_mysql_query(instance, cmd)
      client = Client.open(instance)
      if client 
        results = client.query(cmd)
      else
        puts "Could not open connection to MySQL instance."
      end
      results
    rescue Mysql2::Error => e
      puts e.message
      puts e.backtrace
    ensure
      client.close if client
    end

    def start_slave_io_thread(instance)
      client = Client.open(instance)
      if client 
        client.query("START SLAVE IO_THREAD")
      end
    ensure
      client.close if client
    end

    def promote_slave_to_master(instance)
      client = Client.open(instance)
      if client 
        client.query("STOP SLAVE")
        client.query("RESET MASTER")
      end
    ensure
      client.close if client
    end

    def drain_relay_log(instance)
      done = false
      stop_slave_io_thread(instance)
      client = Client.open(instance)
      if client
        
        # If the slave 'sql_thread' is not running, this will loop forever.
        while !done
          results = client.query("SHOW PROCESSLIST")
          results.each do |row| 
            if  row["State"] =~ /Slave has read all relay log/
              done = true
              puts "Slave has read all relay log."
              break
            end
          end
          puts "Waiting for slave to read relay log." unless done
        end  
      else
        puts "Could not open connection to instance #{instance}."
      end
    ensure
      client.close if client
    end

    # 'master' is the new master
    # 'slaves' is contains the list of slaves, one of these may be the current master.
    def switch_master_to(master, slaves)

      slaves = Array(slaves)
      
      # Step 1. Make sure all slaves have completely processed their
      # Relay Log.
      slaves.each do |s|
        puts "Draining relay log for slave instance #{s}"
        drain_relay_log(s) if is_slave?(s)
      end
      
      # Step 2. For the slave being promoted to master, issue STOP SLAVE
      # and RESET MASTER.
      client = Client.open(master)
      client.query("STOP SLAVE")
      client.query("RESET MASTER")
      client.close

      # Step 3.  Change the master for the other slaves (and the former master?)
      # XXX this is not complete -- what about the coordinates?
      master_server = Servers.instance_to_server(master)
      master_host = 'localhost' # should be master_server[:hostname]
      master_user = 'repl'
      master_password = 'har526'
      master_port = master_server[:port]
      slaves.each do |s|
        client = Client.open(s)
        cmd = <<-EOT
          CHANGE MASTER TO
            MASTER_HOST=\'#{master_host}\',
            MASTER_PORT=#{master_port},
            MASTER_USER=\'#{master_user}\',
            MASTER_PASSWORD=\'#{master_password}\'
        EOT
        client.query(cmd)
        client.query("START SLAVE")
        client.close
      end
    end
    
    def do_repl_user(instance)
      hostname = "127.0.0.1"
      
      # CREATE USER 'repl'@'%.mydomain.com' IDENTIFIED BY 'slavepass';
      # GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%.mydomain.com';
      client = Client.open(instance)
      
      cmd = "DROP USER \'#{REPLICATION_USER}\'@\'#{hostname}\'"
      client.query(cmd) rescue Mysql2::Error
      
      if client
        cmd = "CREATE USER \'#{REPLICATION_USER}\'@\'#{hostname}\' IDENTIFIED BY \'#{REPLICATION_PASSWORD}\'"
        puts cmd
        client.query(cmd)
        cmd = "GRANT REPLICATION SLAVE ON *.* TO \'#{REPLICATION_USER}\'@\'#{hostname}\'"
        puts cmd
        client.query(cmd)
        client.query("FLUSH PRIVILEGES")
      else
        puts "Could not open connection to MySQL instance #{instance}."
      end
    rescue Mysql2::Error => e
      puts e.message
      puts e.backtrace
    ensure
      client.close if client
    end
    
    
    def do_cluster_user(instance)
      puts "entered do_cluster_user"
      client = Client.open(instance)
      puts "client is #{client}"
     
      cmd = "DROP USER \'cluster\'@\'localhost\'"
      client.query(cmd) rescue Mysql2::Error
 
      cmd = "DROP USER \'cluster\'@\'%\'"
      client.query(cmd) rescue Mysql2::Error
      
      if client
        cmd = "CREATE USER \'cluster\'@\'localhost\' IDENTIFIED BY \'secret\'"
        client.query(cmd)
        cmd = "GRANT ALL PRIVILEGES ON *.* TO \'cluster\'@'\localhost\'"
        client.query(cmd)
        cmd = "CREATE USER \'cluster\'@\'%\' IDENTIFIED BY \'secret\'"
        client.query(cmd)
        cmd = "GRANT ALL PRIVILEGES ON *.* TO \'cluster\'@\'%\'"
        client.query(cmd)
      else
        puts "Could not open connection to MySQL instance #{instance}."
      end
    rescue Mysql2::Error => e
      puts e.message
      puts e.backtrace
    ensure
      client.close if client
    end
    
    # Create the 'widgets' database.
    def do_create_widgets(instance)
       client = Client.open(instance)
       if client
         client.query("drop database if exists widgets")
         client.query("create database widgets")
       else
         puts "Could not open connection to MySQL instance #{instance}."
       end
     rescue Mysql2::Error => e
       puts e.message
       puts e.backtrace
     ensure
       client.close if client
     end
     
     # This is an example template to create commands to issue queries.
     def template(instance)
       client = Client.open(instance)
       if client
         client.query("some SQL statement")
       else
         puts "Could not open connection to MySQL instance #{instance}."
       end
     rescue Mysql2::Error => e
       puts e.message
       puts e.backtrace
     ensure
       client.close if client
     end

     def defaults_file(instance)
       "#{DATA_HOME}/my#{instance.to_s}.cnf"
     end
   
    private
    
    def run_cmd(cmd, verbose)
      puts cmd if verbose
      cmd += " > /dev/null 2>&1" unless verbose
      output = %x[#{cmd}]
      puts output if verbose
      exit_code = $?.exitstatus
      if exit_code == 0
        puts "OK"
      else 
        "FAIL: exit code is #{exit_code}"
      end
    end
       
    # Return the process ID (pid) for an instance.
    def get_mysqld_pid(instance)
      io = IO.popen(["sh", "-c", "ps alxww"])
      processes = io.readlines  
      instances = 
        processes.select {|p| p =~ /mysqld .*my#{instance.to_s}.cnf/ }
      if instances.empty?
        return nil
      else
        /^\s*\d+\s*(\d+)/.match(instances[0])[1]
      end
    end
    
    
  end
end

# STOP SLAVE
# RESET MASTER
# CHANGE MASTER TO

class RunExamples
  include MySQLAdmin::Commands
  
  def runtest
      
    switch_master_to(3)
=begin
    (1..4).each {|i| ensure_running(i)}
    puts get_master_coordinates(1)
    (2..4).each {|i| puts get_slave_coordinates(i)}
    (1..4).each {|i| puts is_master?(i) }
    puts "Master is #{find_master}"
    drain_relay_log(2)
    # sleep(10)
    # (1..4).each {|i| crash(i)}
=end
  end
end



