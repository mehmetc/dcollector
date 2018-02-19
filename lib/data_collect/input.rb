#encoding: UTF-8
require 'http'
require 'open-uri'
require 'nokogiri'
require 'json'
require 'nori'
require 'uri'
require 'logger'
require 'cgi'
require 'mime/types'
require 'active_support/core_ext/hash'

class Input
  def initialize
    @logger = Logger.new(STDOUT)
  end

  def from_uri(source, options = {})
    source = CGI.unescapeHTML(source)
    @logger.info("Loading #{source}")
    uri = URI(source)
    begin
      data = nil
      case uri.scheme
        when 'http'
          data = from_http(uri, options)
        when 'https'
          data = from_https(uri, options)
        when 'file'
          data = from_file(uri, options)
        else
          raise "Do not know how to process #{source}"
      end

      data = data.nil? ? 'no data found' : data

      if block_given?
        yield data
      else
        data
      end
    rescue => e
      @logger.info(e.message)
      puts e.backtrace.join("\n")
      nil
    end
  end

  private
  def from_http(uri, options = {})
    from_https(uri, options)
  end

  def from_https(uri, options = {})
    data = nil
    raise "User or Password parameter not found" unless options.keys.include?(:user) && options.keys.include?(:password)
    user = options[:user]
    password = options[:password]
    http_response = HTTP.basic_auth(user: user, pass: password).get(escape_uri(uri))
    nori = Nori.new(parser: :nokogiri, strip_namespaces: true, convert_tags_to: lambda {|tag| tag.gsub(/^@/, '_')})
    case http_response.code
      when 200
        data = http_response.body.to_s

        # File.open("#{rand(1000)}.xml", 'wb') do |f|
        #   f.puts data
        # end

        file_type = file_type_from(http_response.headers)

        case file_type
          when 'applicaton/json'
            data = JSON.parse(data)
          when 'application/atom+xml'
            data = JSON.parse(nori.parse(data).to_json)
          when 'application/xml'
          when 'text/xml'
            data = JSON.parse(nori.parse(data).to_json)
          else
            data = JSON.parse(nori.parse(data).to_json)
        end
      when 401
        raise 'Unauthorized'
      when 404
        raise 'Not found'
      else
        raise "Unable to process received status code = #{http_response.code}"
    end

    data
  end

  def from_file(uri, options = {})
    data = nil
    absolute_path = File.absolute_path("#{uri.host}#{uri.path}")
    case File.extname(absolute_path)
      when '.json'
        data = JSON.parse(File.read("#{absolute_path}"))
      when '.xml'
        nori = Nori.new(parser: :nokogiri, strip_namespaces: true, convert_tags_to: lambda {|tag| tag.gsub(/^@/, '_')})
        file = File.read("#{absolute_path}")
        data = JSON.parse(nori.parse(file).to_json)
      else
        raise "Do not know how to process #{uri.to_s}"
    end

    data
  end

  private
  def escape_uri(uri)
    #"#{uri.to_s.gsub(uri.query, '')}#{CGI.escape(CGI.unescape(uri.query))}"
    uri.to_s
  end

  def file_type_from(headers)
    file_type = 'application/octet-stream'
    file_type = if headers.include?('Content-Type')
                  headers['Content-Type'].split(';').first
                else
                  MIME::Types.of(filename_from(headers)).first.content_type
                end

    return file_type
  end

end