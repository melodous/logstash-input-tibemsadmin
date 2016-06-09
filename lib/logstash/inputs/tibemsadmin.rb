# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "tibems"
require "socket" # for Socket.gethostname

# Generate a repeating message.
#
# This plugin is intented only as an example.

class LogStash::Inputs::TibEMSAdmin < LogStash::Inputs::Base
  config_name "tibemsadmin"

  # If undefined, Logstash will complain, even if codec is unused.
  default :codec, "plain"

  # The message string to use in the event.
  config :message, :validate => :string, :default => "Hello World!"

  # The message string to use in the event.
  config :url, :validate => :string, :default => "tcp://localhost:7222"

  # The message string to use in the event.
  config :user, :validate => :string, :default => "admin"

  # The message string to use in the event.
  config :pass, :validate => :string, :default => ""

  # Set how frequently messages should be sent.
  #
  # The default, `1`, means send a message every second.
  config :interval, :validate => :number, :default => 1

  public
  def register
    @host = Socket.gethostname
    begin
      @tibems = TibEMS::Admin.new(:url => @url, :user => @user, :pass => @pass)
    rescue => e
      logger.error("Unable to create tibems admin connection from given configuration", :tibems_error_message => e)
      throw e
    end
  end # def register

  def run(queue)
    # we can abort the loop if stop? becomes true
    while !stop?
      begin
        info = @tibems.get_info()
      rescue => e
        logger.error("Unable to get info from tibems admin connection", :tibems_error_message => e)
        throw e
      end

      queues_count = info[:queue]
      topics_count = info[:topic]
      consumers_count = info[:consumer]
      producers_count = info[:producer]

      event = LogStash::Event.new("message" => @message, "host" => @host,
                              "queue.type" => "server", "queue.queues" => queues_count, "queue.topics" => topics_count,
                        "queue.consumers" => consumers_count, "queue.producers" => producers_count )
      decorate(event)
      queue << event

      queues = info[:queues]
      queues.each { |q| create_event(queue, q, "queue") }

      topics = info[:topics]
      topics.each { |q| create_event(queue, q, "topic") }

      # because the sleep interval can be big, when shutdown happens
      # we want to be able to abort the sleep
      # Stud.stoppable_sleep will frequently evaluate the given block
      # and abort the sleep(@interval) if the return value is true
      Stud.stoppable_sleep(@interval) { stop? }
    end # loop
  end # def run

  def stop
    # nothing to do in this case so it is not necessary to define stop
    # examples of common "stop" tasks:
    #  * close sockets (unblocking blocking reads/accepts)
    #  * cleanup temporary files
    #  * terminate spawned threads
    @tibems.close()
  end

  private
  def create_event(queue, info, type)
      event = LogStash::Event.new("message" => @message, "host" => @host,
                      "queue.name" => info[:name],
                      "queue.name" => type,
                      "queue.deliveredMessageCount" => info[:deliveredMessageCount],
                      "queue.flowControlMaxBytes" => info[:flowControlMaxBytes],
                      "queue.maxBytes" => info[:maxBytes],
                      "queue.maxMsgs" => info[:maxMsgs],
                      "queue.pendingMessageCount" => info[:pendingMessageCount],
                      "queue.pendingMessageSize" => info[:pendingMessageSize],
                      "queue.pendingPersistentMessageCount" => info[:pendingPersistentMessageCount],
                      "queue.receiverCount" => info[:receiverCount],
                      "queue.outbound.byteRate" => info[:outbound][:byteRate],
                      "queue.outbound.messageRate" => info[:outbound][:messageRate],
                      "queue.outbound.totalBytes" => info[:outbound][:totalBytes],
                      "queue.outbound.totalMessages" => info[:outbound][:toalMessages],
                      "queue.inbound.byteRate" => info[:inbound][:byteRate],
                      "queue.inbound.messageRate" => info[:inbound][:messageRate],
                      "queue.inbound.totalBytes" => info[:inbound][:totalBytes],
                      "queue.inbound.totalMessages" => info[:inbound][:toalMessages])

      decorate(event)
      queue << event
  end
end # class LogStash::Inputs::TibEMSAdmin
