GC.disable

ENV['RAILS_ENV'] = 'production'
require 'English'

# Benchmark Configuration container
module TestConfiguration
  module_function

  def migrate
    ActiveRecord::Schema.define do
      drop_table(:samples) if table_exists?(:samples)

      create_table :samples do |t|
        t.string :name
        t.timestamps
      end
    end
  end

  def sidekiq
    Sidekiq.options.tap do |options|
      options[:tag] = 'test'
      options[:queues] << 'default'
      options[:concurrency] = 20
      options[:timeout] = 2
    end
  end

  def redis
    {pool_size: 30, timeout: 3}
  end

  def iteration_count=(c)
    @iteration_count=c
  end
  def iteration_count
    @iteration_count ||= 500
  end
end

require 'bundler/setup'
require 'rails/all'
Bundler.require(*Rails.groups)

# Example Rails App
module SampleApp
  class Application < Rails::Application;
  end
end

# Overrides rails configueration locations
module OverrideConfiguration
  def paths
    super.tap {|path| path.add 'config/database', with: 'benchmarks/postgres_database.yml'}
  end
end

Rails::Application::Configuration.prepend(OverrideConfiguration)

Rails.application.configure do
  config.cache_classes = true
  config.eager_load = true
  config.active_job.queue_adapter = :sidekiq
end

ActiveRecord::Base.configurations = Rails.application.config.database_configuration
Rails.application.initialize!

ActiveRecord::Base.class_eval do
  def self.establish_connection(*)
    super.tap do
      MemoryTestFix::SchemaLoader.init_schema
    end
  end
end

TestConfiguration::migrate

class Sample < ActiveRecord::Base;
end

require 'sidekiq/launcher'
require 'sidekiq/cli'
require 'concurrent/atomic/atomic_fixnum'

Sidekiq.configure_server do |config|
  redis_conn = proc do
    Redis.new(
        host: ENV.fetch('TEST_REDIS_HOST', '127.0.0.1'),
        port: ENV.fetch('TEST_REDIS_PORT', 6379)
    )
  end
  config.redis = ConnectionPool.new(size: TestConfiguration.redis[:pool_size],
                                    timeout: TestConfiguration.redis[:timeout],
                                    &redis_conn)
  Sidekiq::Logging.logger = nil
end


# Simple Sidekiq worker performing the real benchmark
class Worker
  class << self
    attr_reader :iterations, :conditional_variable
  end

  @iterations = Concurrent::AtomicFixnum.new(0)
  @conditional_variable = ConditionVariable.new

  include Sidekiq::Worker

  def perform(iter, max_iterations)
    self.class.iterations.increment
    self.class.conditional_variable.broadcast if self.class.iterations.value > max_iterations

    100.times do
      Sample.last.name
    end

    Sample.last(100).to_a
  end
end

if Datadog.respond_to?(:configure)
  Datadog.configure do |d|
    d.use :rails, enabled: true, tags: {'tag' => 'value'}
    d.use :http
    d.use :sidekiq, service_name: 'service'
    d.use :redis
    d.use :dalli
    d.use :resque, workers: [Worker]

    processor = Datadog::Pipeline::SpanProcessor.new do |span|
      true if span.service == 'B'
    end

    Datadog::Pipeline.before_flush(processor)
  end
end

def current_memory
  `ps -o rss #{$PROCESS_ID}`.split("\n")[1].to_f / 1024
end

def time
  Process.clock_gettime(Process::CLOCK_MONOTONIC)
end

def launch(iterations, options)
  1000.times do |i|
    Sample.create!(name: "#{i}-#{iterations}").save
  end

  iterations.times do |i|
    Worker.perform_async(i, iterations)
  end

  launcher = Sidekiq::Launcher.new(options)
  launcher.run
end

def wait_and_measure(iterations)
  start = time

  STDERR.puts "#{time - start}, #{current_memory}"

  mutex = Mutex.new

  while Worker.iterations.value < iterations
    mutex.synchronize do
      Worker.conditional_variable.wait(mutex, 1)
      STDERR.puts "#{time - start}, #{current_memory}"
    end
  end
end

require 'ruby-prof'

def calltree(result)
  path = "#{ENV['B']}"
  Dir.mkdir(path)
  printer = RubyProf::CallTreePrinter.new(result)
  printer.print(path: path)
end

def threads(result)
  printer = RubyProf::GraphHtmlPrinter.new(result)
  File.open("#{ENV['B']}-#{$PROCESS_ID}-threads.html", 'w+') do |file|
    printer.print(file)
    puts file.path
  end
end

def bm
  RubyProf.start
  yield

  result = RubyProf.stop

  calltree(result)
  threads(result)
end

def run(&block)
  if ENV['B']
    bm(&block)
  else
    yield
  end
end

if ENV['B']
  TestConfiguration.iteration_count /= 5
end

launch(TestConfiguration.iteration_count, TestConfiguration.sidekiq)
run {wait_and_measure(TestConfiguration.iteration_count)}