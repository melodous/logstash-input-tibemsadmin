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

      event_info = { "queue" => {
                     "type" => "server",
		     "queues" => info["queueCount"],
		     "topics" => info["topicCount"],
                     "consumers" => info["consumerCount"],
		     "producers" => info["producerCount"]
		}
      }

      event = LogStash::Event.new(event_info)

      decorate(event)
      queue << event

      tibco_queues = info["queues"]
      tibco_queues.each { |q| create_event(queue, q, "queue") }

      tibco_topics = info["topics"]
      tibco_topics.each { |q| create_event(queue, q, "topic") }

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

#queue:{"name"=>"name", "consumerCount"=>0, "flowControlMaxBytes"=>0, "pendingMessageCount"=>0, "pendingMessageSize"=>0, "receiverCount"=>0, "deliveredMessageCount"=>0, "inTransitMessageCount"=>0, "maxRedelivery"=>0, "inbound"=>{"totalMessages"=>0, "messageRate"=>0, "totalBytes"=>0, "byteRate"=>0}, "outbound"=>{"totalMessages"=>0, "messageRate"=>0, "totalBytes"=>0, "byteRate"=>0}},
#topic:{"name"=>"name", "consumerCount"=>0, "flowControlMaxBytes"=>0, "pendingMessageCount"=>0, "pendingMessageSize"=>0, "subscriberCount"=>0, "durableCount"=>0, "activeDurableCount"=>0, "inbound"=>{"totalMessages"=>17, "messageRate"=>0, "totalBytes"=>27783, "byteRate"=>0}, "outbound"=>{"totalMessages"=>17, "messageRate"=>0, "totalBytes"=>27783, "byteRate"=>0}}

      event_info = {
		      "host" => @host,
		      "queue" => {
                          "name" => info["name"],
                          "type" => type,
                          "consumerCount" => info["consumerCount"],
                          "flowControlMaxBytes" => info["flowControlMaxBytes"],
                          "pendingMessageCount" => info["pendingMessageCount"],
                          "pendingMessageSize" => info["pendingMessageSize"],
			  "outbound" => {
                              "totalMessages" => info["outbound"]["totalMessages"],
                              "messageRate" => info["outbound"]["messageRate"],
                              "totalBytes" => info["outbound"]["totalBytes"],
                              "byteRate" => info["outbound"]["byteRate"],
			  },
			  "inbound" => {
                              "totalMessages" => info["inbound"]["totalMessages"],
                              "messageRate" => info["inbound"]["messageRate"],
                              "totalBytes" => info["inbound"]["totalBytes"],
                              "byteRate" => info["inbound"]["byteRate"]
			  }
		      }
      }

      if (type == "topic")
        event_info["queue"]["subscriberCount"] = info["subscriberCount"]
        event_info["queue"]["durableCount"] = info["durableCount"]
        event_info["queue"]["activeDurableCount"] = info["activeDurableCount"]
      else
        event_info["queue"]["receiverCount"] = info["receiverCount"]
        event_info["queue"]["deliveredMessageCount"] = info["deliveredMessageCount"]
        event_info["queue"]["inTransitMessageCount"] = info["inTransitMessageCount"]
        event_info["queue"]["maxRedelivery"] = info["maxRedelivery"]
      end

      event = LogStash::Event.new(event_info)

      decorate(event)
      queue << event
  end
end # class LogStash::Inputs::TibEMSAdmin
