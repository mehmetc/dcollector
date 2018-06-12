#encoding: UTF-8
require 'active_support/core_ext/hash'
require 'jsonpath'
require 'logger'

require 'data_collect/input'
require 'data_collect/output'
require 'data_collect/config_file'

class DataCollect
  attr_reader :input, :output

  def initialize
    Encoding.default_external = "UTF-8"
    @logger = Logger.new(STDOUT)
  end

  def runner(rule_file_name)
    @time_start = Time.now
    prg = self
    prg.instance_eval(File.read(rule_file_name))
    prg
  rescue Exception => e
    puts e.message
    puts e.backtrace.join("\n")
  ensure
#    output.tar_file.close unless output.tar_file.closed?
    @logger.info("Finished in #{((Time.now - @time_start)*1000).to_i} ms")
  end

  #These functions are available to your rules file
  private
  # Read input from an URI
  # example:  input.from_uri("http://www.libis.be")
  #           input.from_uri("file://hello.txt")
  def input
    @input ||= Input.new
  end

  # Output is an object you can store data that needs to be written to an output stream
  # output[:name] = 'John'
  # output[:last_name] = 'Doe'
  #
  # Write output to a file, string use an ERB file as a template
  # example:
  # test.erb
  #   <names>
  #     <combined><%= data[:name] %> <%= data[:last_name] %></combined>
  #     <%= print data, :name, :first_name %>
  #     <%= print data, :last_name %>
  #   </names>
  #
  # will produce
  #   <names>
  #     <combined>John Doe</combined>
  #     <first_name>John</first_name>
  #     <last_name>Doe</last_name>
  #   </names>
  #
  # Into a variable
  # result = output.to_s("test.erb")
  # Into a file stored in records dir
  # output.to_file("test.erb")
  # Into a tar file stored in data
  # output.to_file("test.erb", "my_data.tar.gz")
  # Into a temp directory 
  # output.to_tmp_file("test.erb","directory")
  def output
    @output ||= Output.new
  end

  # evaluator http://jsonpath.com/
  # uitleg http://goessner.net/articles/JsonPath/index.html
  def filter(data, filter_path)
    filtered = []
    if filter_path.is_a?(Array) && data.is_a?(Array)
      filtered = data.map{|m| m.select{|k,v| filter_path.include?(k.to_sym)}}
    elsif filter_path.is_a?(String)
      filtered = JsonPath.on(data, filter_path)
    end

    filtered = [filtered] unless filtered.is_a?(Array)
    filtered = filtered.first if filtered.length == 1 && filtered.first.is_a?(Array)
    #filtered = filtered.empty? ? nil : filtered

    filtered
  rescue Exception => e
    @logger.error("#{filter_path} failed: #{e.message}")
    return []
  end

  def config
    @config ||= ConfigFile
  end

  def log(message)
    @logger.info(message)
  end

end