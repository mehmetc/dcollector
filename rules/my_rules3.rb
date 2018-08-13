begin
  counter = 0
  timing_start = Time.now
  data = input.from_uri("file://mock.xml", {})
  log("Data loaded in #{((Time.now - timing_start) * 1000).to_i} ms")

  timing_start = Time.now
  #Filter on Object
  filter(data, '$..entry[*].object').each do |object|

    output[:id] = filter(object, '@._id')
    output[:updated] = filter(object, '@._last_affected_when')

    output[:keyword] = filter(object, '$..keywords.keyword').each {|d|
      #require 'debug'

      #if d.methods.include?(:attributes)
      if d.is_a?(Nori::StringWithAttributes)
        puts d.attributes.to_json
      end

      if d && d.is_a?(Array)
        d.flatten.sort.uniq.compact
      else
        [d]
      end
    }
  end


  output.to_file("./templates/test.erb", "#{Time.now.to_i}.tar.gz")
  counter += 1
rescue => e
ensure
  log("Counted #{counter} records")
end