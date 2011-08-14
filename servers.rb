require 'yaml'

module MySQLAdmin
  module Servers
    @@servers = nil
    
    def self.all_servers
      unless @@servers
        @@servers = []
        servers = File.open('servers.yml') { |yf| YAML::load( yf ) }
        servers.each do |s|
          s.keys.each do |key|
            s[key.to_sym] = s.delete(key)
          end
          @@servers << s
        end
      else
        @@servers
      end
    end
    
    def self.all_instances
      instances = []
      all_servers.each do |s|
        instances << s[:instance]
      end
      instances
    end
    
    def self.instance_to_server(instance)
      all_servers.select {|s| s[:instance] == Integer(instance)}.shift
    end
 end
end
