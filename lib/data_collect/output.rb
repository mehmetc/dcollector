#encoding: UTF-8
require 'nokogiri'
require 'erb'
require 'date'
require 'minitar'
require 'zlib'
require 'cgi'
require 'active_support/core_ext/hash'

class Output
  include Enumerable
  attr_reader :data, :tar_file

  def initialize(data = {})
    @data = data
  end

  def each
    @data.each do |d|
      yield d
    end
  end

  def [](k,v = nil)
    data[k]
  end

  def []=(k,v = nil)
    unless v.nil? || v.empty?
      if data.has_key?(k)
        data[k] << v
      else
        data[k] = v
      end
    end

    #data[k] = data[k].flatten.compact
    data
  end

  def to_s(erb_file)
    data = @data
    def print(data, symbol, to_symbol = nil)
      tag = to_symbol ? to_symbol.to_s : symbol.to_s

      if data.with_indifferent_access[symbol]
        if data.with_indifferent_access[symbol].is_a?(Array)
          r = []
          data.with_indifferent_access[symbol].each do |d|
            r << "<#{tag}>#{CGI.escapeHTML(d)}</#{tag}>"
          end
          r.join("\n")
        elsif data.with_indifferent_access[symbol].is_a?(Hash)
          r = []
          r << "<#{tag}>"
          data.with_indifferent_access[symbol].keys.each do |k|
            r << print(data.with_indifferent_access[symbol], k)
          end
          r << "</#{tag}>"
          r.join("\n")
        else
          "<#{tag}>#{CGI.escapeHTML(data.with_indifferent_access[symbol])}</#{tag}>"
        end
      else
        nil
      end
    rescue Exception => e
      @logger.error("unable to print data '#{symbol}'")
    end

    data[:response_date] = DateTime.now.xmlschema

    result = ERB.new(File.read(erb_file), 0, '>').result(binding)
    @data = {}
    result
  rescue Exception => e
    raise "unable to transform to text: #{e.message}"
    ""
  end


  def to_file(erb_file, tar_file_name = nil)
    id = data[:id].first rescue 'unknown'
    result = to_s(erb_file)

    xml_result = Nokogiri::XML(result, nil, 'UTF-8') do |config|
      config.noblanks
    end

    if tar_file_name.nil?
      file_name = "records/#{id}_#{rand(1000)}.xml"
      File.open(file_name, 'wb:UTF-8') do |f|
        f.puts xml_result.to_xml
      end

      return file_name
    else

      Minitar::Output.open(Zlib::GzipWriter.new(File.open("records/#{tar_file_name}", 'wb:UTF-8'))) do |f|
        xml_data = xml_result.to_xml
        f.tar.add_file_simple("#{id}_#{rand(1000)}.xml", data: xml_data, size: xml_data.size, mtime: Time.now.to_i)
      end

      return tar_file_name
    end

  rescue Exception => e
    raise "unable to save to file: #{e.message}"
  end

  private
  def tar_file(tar_file_name)
    @tar_file ||= Minitar::Output.open(File.open("records/#{tar_file_name}", "a+b"))
  end
end
