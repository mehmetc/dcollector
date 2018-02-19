$LOAD_PATH << '.' << './lib'
require 'lib/data_collect'

Dir.glob('./records/*.xml') do |f|
  File.unlink(f)
end

if ARGV.empty?
  puts "USAGE #{__FILE__} rules"
  exit 1
else
  filename = ARGV[0]
  dc = DataCollect.new
  dc.runner(filename)
end
