#Get last ran date
from_date = CGI.escape(DateTime.parse(config[:last_run]).xmlschema)

begin
#Create starting URL
  url = "#{config[:base_url]}/1600776"
  #url = "#{config[:base_url]}#{from_date}"
  options = {user: config[:user], password: config[:password]}
  counter = 0

  while url
    timing_start = Time.now
    #Load data
    data = input.from_uri(url, options)
    log("Data loaded in #{((Time.now - timing_start) * 1000).to_i} ms")

    timing_start = Time.now
    #Filter on Object
    filter(data, '$..entry.object').each do |object|

      output[:id] = filter(object, '@._id')
      output[:updated] = filter(object, '@._last_affected_when')

      output[:keyword] = filter(object, '$..keywords.keyword').each {|d|
        if d && d.is_a?(Array)
          d.flatten.sort.uniq.compact
        else
          [d]
        end
      }

      #Filter on every record
      filter(object, '$..records.record').each do |record|
        output[:title] = filter(record, '$..native.field[?(@._name=="title")].text')
        output[:abstract] = filter(record, '$..native.field[?(@._name=="abstract")].text')
        output[:author] = filter(filter(record, '$..native.field[?(@._name=="authors")].people.person'), [:first_names, :last_name, :initials])
        output[:author_url] = filter(record, '$..native.field[?(@._name=="author-url")].text')
        output[:editor] = filter(filter(record, '$..native.field[?(@._name=="editors")].persons.person'), [:first_names, :last_name, :initials])
        output[:serie] = filter(record, '$..native.field[?(@._name=="series")].text')
        output[:edition] = filter(record, '$..native.field[?(@._name=="edition")].text')
        output[:volume] = filter(record, '$..native.field[?(@._name=="volume")].text')
        output[:pagination] = filter(record, '$..native.field[?(@._name=="pagination")].pagination')
        output[:publisher] = filter(record, '$..native.field[?(@._name=="publisher")].text')
        output[:publisher_url] = filter(record, '$..native.field[?(@._name=="publisher-url")].text')
        output[:place_of_publication] = filter(record, '$..native.field[?(@._name=="place-of-publication")].text')
        output[:publication_date] = filter(record, '$..native.field[?(@._name=="publication-date")].date').map {|d| "#{d['year']}-#{d['month']}-#{d['day'] || '1'}"}
        output[:isbn_10] = filter(record, '$..native.field[?(@._name=="isbn-10")].text')
        output[:isbn_13] = filter(record, '$..native.field[?(@._name=="isbn-13")].text')
        output[:doi] = filter(record, '$..native.field[?(@._name=="doi")].text')
        output[:medium] = filter(record, '$..native.field[?(@._name=="medium")].text')
        output[:publication_status] = filter(record, '$..native.field[?(@._name=="publication-status")].text')
        output[:note] = filter(record, '$..native.field[?(@._name=="notes")].text')
        output[:number] = filter(record, '$..native.field[?(@._name=="number")].text')
        output[:parent_title] = filter(record, '$..native.field[?(@._name=="parent-title")].text')
        output[:name_of_conference] = filter(record, '$..native.field[?(@._name=="name-of-conference")].text')
        output[:location] = filter(record, '$..native.field[?(@._name=="location")].text')
        output[:start_date] = filter(record, '$..native.field[?(@._name=="start-date")].date').map {|d| "#{d['year']}-#{d['month']}-#{d['day'] || '1'}"}
        output[:finish_date] = filter(record, '$..native.field[?(@._name=="finish-date")].date').map {|d| "#{d['year']}-#{d['month']}-#{d['day'] || '1'}"}
        output[:journal] = filter(record, '$..native.field[?(@._name=="journal")].text')
        output[:issue] = filter(record, '$..native.field[?(@._name=="issue")].text')
        output[:issn] = filter(record, '$..native.field[?(@._name=="issn")].text')
        output[:pii] = filter(record, '$..native.field[?(@._name=="pii")].text')
        output[:language] = filter(record, '$..native.field[?(@._name=="language")].text')
        output[:patent_number] = filter(record, '$..native.field[?(@._name=="patent-number")].text')
        output[:associated_authors] = filter(filter(record, '$..native.field[?(@._name=="associated-authors")].people.person'), [:first_names, :last_name, :initials])
        output[:filed_date] = filter(record, '$..native.field[?(@._name=="filed-date")].date').map {|d| "#{d['year']}-#{d['month']}-#{d['day'] || '1'}"}
        output[:patent_status] = filter(record, '$..native.field[?(@._name=="patent-status")].text')
        output[:commissioning_body] = filter(record, '$..native.field[?(@._name=="commissioning-body")].text')
        output[:confidential] = filter(record, '$..native.field[?(@._name=="confidential")].boolean')
        output[:number_of_pieces] = filter(record, '$..native.field[?(@._name=="number-of-pieces")].boolean')
        output[:version] = filter(record, '$..native.field[?(@._name=="version")].boolean')
        output[:eissn] = filter(record, '$..native.field[?(@._name=="eissn")].text')
        output[:external_identifiers] = filter(record, '$..native.field[?(@._name=="external-identifiers")].identifiers.identifier')
      end

      #Save to file using an ERB template
      output.to_file("./templates/test.erb")
      #output.to_file("test.erb", "#{Time.now.to_i}.tar.gz")
      counter += 1
    end

    #Filter next URL
    #require 'debug'
    url = filter(data, '$..link[?(@._rel=="next")]._href').first || nil
    log("Converted in #{((Time.now - timing_start) * 1000).to_i} ms")
  end
ensure
  log("Counted #{counter} records")
  config[:last_run] = Time.now.xmlschema
end
