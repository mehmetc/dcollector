#Get last ran date
from_date = CGI.escape(DateTime.parse(config[:last_run_updates]).xmlschema)
from_date_deleted = CGI.escape(DateTime.parse(config[:last_run_deletes]).xmlschema)

begin
#Create starting URL
  url = "#{config[:base_url]}#{from_date}"
  url_delete = "#{config[:base_delete_url]}#{from_date_deleted}"

  options = {user: config[:user], password: config[:password]}
  
  last_affected_when = config[:last_run_updates]
  deleted_when  = config[:last_run_deletes]

  records_dir = "records"
  counter = 0
  updated_records = 0
  max_updated_records = 500000
  deleted_records = 0
  max_deleted_records = 50000

  debugging = true

if debugging
  max_updated_records = 10
  max_deleted_records = 5
  url = 'file://mock.xml'
end


  # More about jsonpath
  #https://www.pluralsight.com/blog/tutorials/introduction-to-jsonpath
  
  #url = "https://lirias2test.libis.kuleuven.be/elements-cache/rest/publications?per-page=10&affected-since=2018-03-14T20%3A44%3A14%2B01%3A00"

  log("Get Data affected-since #{from_date} ")
    
  while url
    timing_start = Time.now
    #Load data
    data = input.from_uri(url, options)
    tmp_records_dir = "#{records_dir}/records_#{Time.now.to_i}"
    log("Data loaded in #{((Time.now - timing_start) * 1000).to_i} ms")

    resolver_data = {}
    resolver_authors = {}

    timing_start = Time.now
    #Filter on Object
    filter(data, '$..entry[*].object').each do |object|

      output[:id] = filter(object, '@._id')
      output[:type] = filter(object, '@._type')
      output[:updated] = filter(object, '@._last_affected_when')
      
      last_affected_when = output[:updated][0]
      #results are sorted by last_affected_when

      #log(" record id #{ output[:id] } ")

      #Filter on every keyword
      dspace_keywords = []
      virtual_collections = []
      keywords = []
      filter(object, '$..keywords.keyword').each do |keyword|
      #  log(" Keyword ---- #{ keyword } ")
        if keyword.is_a?(Nori::StringWithAttributes)
          if filter( keyword.attributes.to_json, '$[?(@.scheme=="c-virtual-collection",@.source=="dspace")]').any?
            virtual_collections << keyword
          else
            keywords << keyword
            dspace_keywords << keyword  if filter( keyword.attributes.to_json, '$[?(@.source=="dspace")]').any?
          end
        else 
          keywords << keyword
        end
      end
      output[:virtual_collections] = virtual_collections 
      output[:dspace_keywords] = dspace_keywords 
      output[:keyword] = keywords

      filter(object, '$.records.record') .each do |record|
        output[:pmid]     = record["_id_at_source"] if  record["_source_name"] == "pubmed"
        output[:wosid]    = record["_id_at_source"] if  record["_source_name"] == "wos"
        output[:scopusid] = record["_id_at_source"] if  record["_source_name"] == "scopus"
      end

      #Filter on every record with source_name merged !
      filter(object, '$.records.record[?(@._source_name=="merged")]').each do |record|

        output[:title] = filter(record, '$..native.field[?(@._name=="title")].text')
        output[:alternative_title] = filter(record, '$..native.field[?(@._name=="c-alttitle")].text')
        output[:abstract] = filter(record, '$..native.field[?(@._name=="abstract")].text')
        output[:author] = filter(filter(record, '$..native.field[?(@._name=="authors")].people.person'), [:first_names, :last_name, :initials, :username, :identifiers])
        output[:author_url] = filter(record, '$..native.field[?(@._name=="author-url")].text')
                       
        output[:serie] = filter(record, '$..native.field[?(@._name=="series")].text')
        output[:book_serie] = filter(record, '$..native.field[?(@._name=="c-series-editor")].text')
        output[:edition] = filter(record, '$..native.field[?(@._name=="edition")].text')
        output[:volume] = filter(record, '$..native.field[?(@._name=="volume")].text')
        output[:issue] = filter(record, '$..native.field[?(@._name=="issue")].text')

        output[:pagination] = filter(record, '$..native.field[?(@._name=="pagination")][?(@._display_name=="Pagination")].pagination')
        output[:number_of_pages] = filter(record, '$..native.field[?(@._name=="pagination")][?(@._display_name=="Number of pages")].pagination.begin_page')

        output[:publisher] = filter(record, '$..native.field[?(@._name=="publisher")].text')
        output[:publisher_url] = filter(record, '$..native.field[?(@._name=="publisher-url")].text')
        output[:place_of_publication] = filter(record, '$..native.field[?(@._name=="place-of-publication")].text')
        output[:publication_date] = filter(record, '$..native.field[?(@._name=="publication-date")].date').map {
          |d| DateTime.parse("#{d['year']}-#{d['month'] || '1'}-#{d['day'] || '1'}  ").strftime("%Y-%m-%d")
        } 
        output[:online_publication_date] = filter(record, '$..native.field[?(@._name=="online-publication-date")].date').map {
          |d| DateTime.parse("#{d['year']}-#{d['month'] || '1'}-#{d['day'] || '1'}  ").strftime("%Y-%m-%d")
        } 
        output[:isbn_10] = filter(record, '$..native.field[?(@._name=="isbn-10")].text')
        output[:isbn_13] = filter(record, '$..native.field[?(@._name=="isbn-13")].text')
        output[:doi] = filter(record, '$..native.field[?(@._name=="doi")].text')
        output[:medium] = filter(record, '$..native.field[?(@._name=="medium")].text')
        output[:publication_status] = filter(record, '$..native.field[?(@._name=="publication-status")].text')
        output[:note] = filter(record, '$..native.field[?(@._name=="notes")].text')
        output[:numbers] = filter(record, '$..native.field[?(@._name=="numbers")].text')
        output[:chapter_number] = filter(record, '$..native.field[?(@._display_name=="Chapter number")].text')
        output[:abstract_number] = filter(record, '$..native.field[?(@._display_name=="Abstract number")].text')
        output[:report_number] = filter(record, '$..native.field[?(@._display_name=="Report number")].text')
        output[:paper_number] = filter(record, '$..native.field[?(@._display_name=="Paper number")].text')
        output[:article_number] = filter(record, '$..native.field[?(@._display_name=="Article number")].text')
      
        output[:parent_title] = filter(record, '$..native.field[?(@._name=="parent-title")].text')
        output[:name_of_conference] = filter(record, '$..native.field[?(@._name=="name-of-conference")].text')
        output[:location] = filter(record, '$..native.field[?(@._name=="location")].text')
        output[:start_date] = filter(record, '$..native.field[?(@._name=="start-date")].date').map {
          |d| DateTime.parse("#{d['year']}-#{d['month'] || '1'}-#{d['day'] || '1'}  ").strftime("%Y-%m-%d")
        } 
        output[:finish_date] = filter(record, '$..native.field[?(@._name=="finish-date")].date').map {
          |d| DateTime.parse("#{d['year']}-#{d['month'] || '1' }-#{d['day'] || '1'}  ").strftime("%Y-%m-%d")
        } 
        output[:journal] = filter(record, '$..native.field[?(@._name=="journal")].text')
        output[:issn] = filter(record, '$..native.field[?(@._name=="issn")].text')
        output[:pii] = filter(record, '$..native.field[?(@._name=="pii")].text')
        output[:language] = filter(record, '$..native.field[?(@._name=="language")].text')
        output[:patent_number] = filter(record, '$..native.field[?(@._name=="patent-number")].text')
        output[:associated_authors] = filter(filter(record, '$..native.field[?(@._name=="associated-authors")].people.person'), [:first_names, :last_name, :initials])
        output[:filed_date] = filter(record, '$..native.field[?(@._name=="filed-date")].date').map {
          |d| DateTime.parse("#{d['year']}-#{d['month'] || '1' }-#{d['day'] || '1'}  ").strftime("%Y-%m-%d")
        } 
        output[:patent_status] = filter(record, '$..native.field[?(@._name=="patent-status")].text')
        output[:commissioning_body] = filter(record, '$..native.field[?(@._name=="commissioning-body")].text')
        output[:confidential] = filter(record, '$..native.field[?(@._name=="confidential")].boolean').map(&:to_s)
        output[:number_of_pieces] = filter(record, '$..native.field[?(@._name=="number-of-pieces")].boolean').map(&:to_s)
        output[:version] = filter(record, '$..native.field[?(@._name=="version")].boolean').map(&:to_s)
        output[:eissn] = filter(record, '$..native.field[?(@._name=="eissn")].text')
        output[:external_identifiers] = filter(record, '$..native.field[?(@._name=="external-identifiers")].identifiers.identifier')
        output[:peer_reviewed] = filter(record, '$..native.field[?(@._name=="c-peer-review")].text')
        output[:professional_oriented] = filter(record, '$..native.field[?(@._name=="c-professional")].boolean').map(&:to_s)
        output[:funding_acknowledgements] = filter(record, '$..native.field[?(@._name=="funding-acknowledgements")].funding_acknowledgements.acknowledgement_text')
        output[:vabb_type] = filter(record, '$..native.field[?(@._name=="c-vabb-type")].text')
        output[:vabb_identifier] = filter(record, '$..native.field[?(@._name=="c-vabb-identifier")].text')
        output[:historic_collection] = filter(record, '$..native.field[?(@._name=="c-collections-historic")].items.item')
        output[:public_url] = filter(record, '$..native.field[?(@._name=="public-url")].text')
        output[:files] = filter( filter(record, '$..native.files.file') ,  [:file_url, :filename, :description, :extension, :embargo_release_date, :embargo_description, :is_open_access, :filePublic, :fileIntranet])

        output[:venue_designart] = filter(record, '$..native.field[?(@._name=="c-venue-designart")].items.item')
        output[:additional_identifier] = filter(record, '$..native.field[?(@._name=="c-additional-identifier")].items.item')

        output[:organizational_unit] = filter(record, '$..native.field[?(@._name=="cache-user-ous")].items.item')

        output[:editor]     = filter(filter(record, '$..native.field[?(@._name=="editors")][?(@._display_name=="Editors")].people.person'), [:first_names, :last_name, :initials, :username, :identifiers])
        output[:supervisor] = filter(filter(record, '$..native.field[?(@._name=="editors")][?(@._display_name=="Supervisor")].people.person'), [:first_names, :last_name, :initials, :username, :identifiers])
        output[:co_supervisor] = filter(filter(record, '$..native.field[?(@._name=="c-cosupervisor")].people.person'), [:first_names, :last_name, :initials, :username, :identifiers])

        output[:contributor] = filter(filter(record, '$..native.field[?(@._name=="c-contributor")].people.person'), [:first_names, :last_name, :initials, :username, :identifiers])
        
        output[:actor] = filter(filter(record, '$..native.field[?(@._name=="c-actor")].people.person'), [:first_names, :last_name, :initials, :username, :identifiers])
        output[:architect] = filter(filter(record, '$..native.field[?(@._name=="c-architect")].people.person'), [:first_names, :last_name, :initials, :username, :identifiers])
        output[:choreographer] = filter(filter(record, '$..native.field[?(@._name=="c-choreographer")].people.person'), [:first_names, :last_name, :initials, :username, :identifiers])
        output[:cinematographer] = filter(filter(record, '$..native.field[?(@._name=="c-cinematographer")].people.person'), [:first_names, :last_name, :initials, :username, :identifiers])
        output[:composer] = filter(filter(record, '$..native.field[?(@._name=="c-composer")].people.person'), [:first_names, :last_name, :initials, :username, :identifiers])
        output[:conductor] = filter(filter(record, '$..native.field[?(@._name=="c-conductor")].people.person'), [:first_names, :last_name, :initials, :username, :identifiers])
        output[:curator] = filter(filter(record, '$..native.field[?(@._name=="c-curator")].people.person'), [:first_names, :last_name, :initials, :username, :identifiers])
        output[:director] = filter(filter(record, '$..native.field[?(@._name=="c-director")].people.person'), [:first_names, :last_name, :initials, :username, :identifiers])
        output[:editor_c] = filter(filter(record, '$..native.field[?(@._name=="c-editor")].people.person'), [:first_names, :last_name, :initials, :username, :identifiers])
        
        output[:educator] = filter(filter(record, '$..native.field[?(@._name=="c-educator")].people.person'), [:first_names, :last_name, :initials, :username, :identifiers])
        output[:interaction] = filter(filter(record, '$..native.field[?(@._name=="c-interaction-designer")].people.person'), [:first_names, :last_name, :initials, :username, :identifiers])
        output[:interior_architect] = filter(filter(record, '$..native.field[?(@._name=="c-interior-architect")].people.person'), [:first_names, :last_name, :initials, :username, :identifiers])
        output[:game_designer] = filter(filter(record, '$..native.field[?(@._name=="c-game-designer")].people.person'), [:first_names, :last_name, :initials, :username, :identifiers])
        output[:graphic_designer] = filter(filter(record, '$..native.field[?(@._name=="c-graphic-designer")].people.person'), [:first_names, :last_name, :initials, :username, :identifiers])
        output[:landscape_architect] = filter(filter(record, '$..native.field[?(@._name=="c-landscape-architect")].people.person'), [:first_names, :last_name, :initials, :username, :identifiers])
        output[:music_performer] = filter(filter(record, '$..native.field[?(@._name=="c-music-performer")].people.person'), [:first_names, :last_name, :initials, :username, :identifiers])
        output[:otherrole] = filter(filter(record, '$..native.field[?(@._name=="c-otherrole")].people.person'), [:first_names, :last_name, :initials, :username, :identifiers])
        output[:photographer] = filter(filter(record, '$..native.field[?(@._name=="c-photographer")].people.person'), [:first_names, :last_name, :initials, :username, :identifiers])
        output[:producer] = filter(filter(record, '$..native.field[?(@._name=="c-producer")].people.person'), [:first_names, :last_name, :initials, :username, :identifiers])
        output[:product_designer] = filter(filter(record, '$..native.field[?(@._name=="c-product-designer")].people.person'), [:first_names, :last_name, :initials, :username, :identifiers])
        output[:sound_artist] = filter(filter(record, '$..native.field[?(@._name=="c-sound-artist")].people.person'), [:first_names, :last_name, :initials, :username, :identifiers])
        output[:urban_designer] = filter(filter(record, '$..native.field[?(@._name=="c-urban-designer")].people.person'), [:first_names, :last_name, :initials, :username, :identifiers])
        output[:visual_artist] = filter(filter(record, '$..native.field[?(@._name=="c-visual-artist")].people.person'), [:first_names, :last_name, :initials, :username, :identifiers])
        output[:writer] = filter(filter(record, '$..native.field[?(@._name=="c-writer")].people.person'), [:first_names, :last_name, :initials, :username, :identifiers])

        author_functions = [
          :actor, 
          :architect,
          :choreographer,
          :cinematographer,
          :composer,
          :conductor,
          :curator,
          :director,
          :editor_c,
          :educator,
          :interaction,
          :interior_architect,
          :game_designer,
          :graphic_designer,
          :landscape_architect,
          :music_performer,
          :otherrole,
          :photographer,
          :producer,
          :product_designer,
          :sound_artist,
          :urban_designer,
          :visual_artist,
          :writer
        ]
        author_functions.each do |function|
          if output[function]
            output.raw()[function].each do |pfunction|
              [:author, :editor, :supervisor, :co_supervisor, :contributor].each do |author_type|
                if output[author_type]
                  output[author_type].map do |person|
                    if  person["last_name"] == pfunction["last_name"] &&  
                      person["first_name"] == pfunction["first_name"] &&  
                      person["initials"] == pfunction["initials"] && 
                      person["identifiers"] == pfunction["identifiers"]  
                      person["function"] ? person["function"] << function.to_s : person["function"] = [function.to_s]
                    else
                      person
                    end
                  end
                end
              end
            end
          end
        end
      end

      #Save to file using an ERB template
      #First create all xml-files in temp_dir than tar
      output.to_tmp_file("templates/lirias_pre_pnx_template.erb",tmp_records_dir)
      #Activate this line for a tar for each xml-file
      #output.to_file("templates/lirias_pre_pnx_template.erb", "#{ output[:id][0] }_#{Time.now.to_i}.tar.gz")

      # Create data for resolver
      
      resolver_data_lirias_records = output.raw().slice(:id, :wosid, :scopus, :pmid, :doi, :isbn_13, :isbn10, :issn, :eissn, :additional_identifier, :files, :public_url, :author, :editor, :supervisor, :co_supervisor, :contributor)
      resolver_data[ resolver_data_lirias_records[:id].first ] = resolver_data_lirias_records
=begin      
      if output.raw().slice(:author)[:author]
        output.raw().slice(:author)[:author].each do |author|

          if author.has_key?( "identifiers" )
            identifiers =  author["identifiers"]["identifier"]
            identifiers = [ identifiers ] unless identifiers.is_a?(Array)
            identifiers.each do |id|
              #log ( "id : #{ id } // #{ id.attributes["scheme"] } ")
              author["identifiers"][ id.attributes["scheme"] ] =  id
            end
          end

          if author.has_key?( "username" )
            if resolver_authors.has_key?( author["username"] ) 
              resolver_authors[author["username"]].merge!(  author ) 
            else
              resolver_authors[author["username"]] =  author
            end
          end
        end
      end
=end
      counter += 1
      output.clear()      
    end

    updated_records = counter

    log(" last_affected_when #{ last_affected_when } ")
    log(" records created #{ updated_records } ")

    if updated_records > 0
      tarfilename = "lirias_#{Time.now.to_i}.tar.gz"
      c_dir = Dir.pwd
      #tar_resp = `cd #{records_dir}; tar -czf #{tarfilename}  *.xml; cd ../`
      tar_resp = `cd #{c_dir}/#{tmp_records_dir}; tar -czf #{c_dir}/#{records_dir}/#{tarfilename}  *.xml; cd #{c_dir}`

      if $?.exitstatus != 0
          log("ERROR in creating tar.gz")
      end


    # DEZE LIJNEN ACTIVEREN VOOR DE JSON INPUT VOOR DE RESOLVER
      output.to_jsonfile(resolver_data, "lirias_resolver_data")
      # output.to_jsonfile(resolver_authors, "lirias_resolver_author")

      #mv_resp = `mv ./#{records_dir}/*.xml ./pre_pnx_temp`
      mv_resp = `rm -r ./#{tmp_records_dir}`
      if $?.exitstatus != 0
          log("ERROR removing tmp_records_dir #{tmp_records_dir}")
      end
    end
    #update config with the new data
    log(" update  config[:last_run_updates] with #{ last_affected_when } ")
    config[:last_run_updates] = last_affected_when

    #Filter next URL
    url = filter(data, '$..link[?(@._rel=="next")]._href').first || nil
    log("Converted in #{((Time.now - timing_start) * 1000).to_i} ms")

    if counter > max_updated_records
      url = nil
    end
  end

  log("Get Deleted affected-since #{from_date_deleted} ")  
  
  counter = 0

  #url_delete = "https://lirias2test.libis.kuleuven.be/elements-cache/rest/deleted/publications?per-page=1000&deleted-since=2018-04-24T13%3A14%3A06%2B00%3A00"


  log("url_delete n #{url_delete}")

  while url_delete
    timing_start = Time.now
    #Load data
    data = input.from_uri(url_delete, options)
    tmp_records_dir = "#{records_dir}/records_#{Time.now.to_i}"

    resolver_deleted_data = {}

    log("Data loaded in #{((Time.now - timing_start) * 1000).to_i} ms")

    resolver_data_lirias_records = []

    timing_start = Time.now
    #Filter on Object
    filter(data, '$..entry[*].deleted_object').each do |object|
      output[:id] = filter(object, '@._id')
      output[:deleted] = filter(object, '@._deleted_when')

      #log(" record id #{ output[:id] } deleted")

      output.to_tmp_file("templates/lirias_delete_template.erb",tmp_records_dir)
      
      deleted_when = output[:deleted][0]
      resolver_data_lirias_records << output.raw()

      #results are sorted by deleted_when
      counter += 1
      output.clear()  
    end

    deleted_records = counter
    log(" deleted_when #{ deleted_when } ")
    log(" delete-records created #{ deleted_records } ")
   
    output.to_jsonfile(resolver_data_lirias_records, "lirias_resolver_deleted_data")

    if deleted_records > 0
      tarfilename = "lirias_deleted_#{Time.now.to_i}.tar.gz"
      c_dir = Dir.pwd

      if File.directory?(tmp_records_dir) 
        #tar_resp = `cd #{records_dir}; tar -czf #{tarfilename}  *.xml; cd ../`
        tar_resp = `cd #{c_dir}/#{tmp_records_dir}; tar -czf #{c_dir}/#{records_dir}/#{tarfilename}  *.xml; cd #{c_dir}`
        if $?.exitstatus != 0
          log("ERROR in creating tar.gz")
        end

        rm_resp = `rm -r ./#{tmp_records_dir}`
        if $?.exitstatus != 0
            log("ERROR removing tmp_records_dir #{tmp_records_dir}")
        end
      end
    end

    #update config with the new data
    log("update config[:last_run_delete] with #{ last_affected_when } ")
    config[:last_run_deletes] = deleted_when

    url_delete = filter(data, '$..link[?(@._rel=="next")]._href').first || nil

    if counter > max_deleted_records
      url_delete = nil
    end

  end
  
ensure
  log("Counted #{updated_records} updated records")
  log("Counted #{deleted_records} deleted records")
  # config[:last_run_updates] = Time.now.xmlschema
end
