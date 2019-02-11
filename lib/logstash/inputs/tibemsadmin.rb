# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "tibems"
require "stud/interval"
require "socket" # for Socket.gethostname

# Generate a repeating message.
#
# This plugin is intented only as an example.

class LogStash::Inputs::Tibemsadmin < LogStash::Inputs::Base
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
      rescue com.tibco.tibjms.admin.TibjmsAdminSecurityException => e
        logger.error("Unable to get info from tibems admin connection, security exception", :tibems_error_message => e)
        Stud.stoppable_sleep(@interval) { stop? }
      rescue com.tibco.tibjms.admin.TibjmsAdminException => e
        if e.inspect.include? "Session is closed" or e.inspect.include? "Connection has been terminated"
          Stud.stoppable_sleep(@interval) { stop? }
          register
        elsif e.inspect.include? "store info: File not found"
          Stud.stoppable_sleep(@interval) { stop? }
        else
          logger.error("Unable to get info from tibems admin connection", :tibems_error_message => e)
          Stud.stoppable_sleep(@interval) { stop? }
          throw e
        end
      rescue => e
        logger.error("Unable to get info from tibems admin connection", :tibems_error_message => e)
        Stud.stoppable_sleep(@interval) { stop? }
        throw e
      else

				event_info = { "queue" => {
											 "type" => "server",
											 "queues" => info["queueCount"],
											 "topics" => info["topicCount"],
											 "consumers" => info["consumerCount"],
											 "producers" => info["producerCount"],
											 "asyncDBSize" => info["asyncDBSize"],
											 "connections" => info["connectionCount"],
											 "diskReadOperationsRate" => info["diskReadOperationsRate"],
											 "diskReadRate" => info["diskReadRate"],
											 "diskWriteOperationsRate" => info["diskWriteOperationsRate"],
											 "diskWriteRate" => info["diskWriteRate"],
											 "durables" => info["durableCount"],
											 "inboundBytesRate" => info["inboundBytesRate"],
											 "inboundMessages" => info["inboundMessageCount"],
											 "inboundMessageRate" => info["inboundMessageRate"],
											 "maxConnections" => info["maxConnections"],
											 "maxMsgMemory" => info["maxMsgMemory"],
											 "msgMem" => info["msgMem"],
											 "msgMemPooled" => info["msgMemPooled"],
											 "outboundBytesRate" => info["outboundBytesRate"],
											 "outboundMessages" => info["outboundMessageCount"],
											 "outboundMessageRate" => info["outboundMessageRate"],
											 "pendingMessages" => info["pendingMessageCount"],
											 "pendingMessageSize" => info["pendingMessageSize"],
											 "reserveMemory" => info["reserveMemory"],
											 "sessions" => info["sessionCount"],
											 "syncDBSize" => info["syncDBSize"]
										 }
				}

				event = LogStash::Event.new(event_info)

				decorate(event)
				queue << event

				tibco_queues = info["queues"]
				tibco_queues.each { |q| create_event(queue, q, "queue") }

				#tibco_topics = info["topics"]
				#tibco_topics.each { |q| create_event(queue, q, "topic") }

				tibco_stores = info["stores"]
        tibco_stores.each { |q| create_event_store(queue, q) }
	#
	# because the sleep interval can be big, when shutdown happens
	# we want to be able to abort the sleep
	# Stud.stoppable_sleep will frequently evaluate the given block
	# and abort the sleep(@interval) if the return value is true
	Stud.stoppable_sleep(@interval) { stop? }
      end
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
  def create_event_store(queue, info)
    event_info = {
      "host" => @host,
      "store" => {
	"name" => info["name"],
        "type" => "store",
      	"msgBytes" => info["msgBytes"],
      	"getMsgCount" => info["getMsgCount"],
      	"destinationDefrag" => info["destinationDefrag"],
      	"fileMinimum" => info["fileMinimum"],
      	"fileName" => info["fileName"],
      	"fragmentation" => info["fragmentation"],
      	"inUseSpace" => info["inUseSpace"],
      	"notInUseSpace" => info["notInUseSpace"],
      	"size" => info["size"],
      	"writeRate" => info["writeRate"]
      }
    }
    event = LogStash::Event.new(event_info)
    decorate(event)
    queue << event
  end

  private
  def create_event(queue, info, type)
      event_info = {
		      "host" => @host,
		      "queue" => {
                          "name" => info["name"],
                          "type" => type,
                          "consumers" => info["consumerCount"],
                          "flowControlMaxBytes" => info["flowControlMaxBytes"],
                          "maxBytes" => info["maxBytes"],
                          "maxMsgs" => info["maxMsgs"],
                          "pendingMessages" => info["pendingMessageCount"],
                          "pendingMessageSize" => info["pendingMessageSize"],
                          "pendingPersistentMessages" => info["pendingPersistentMessageCount"],
                          "pendingPersistentMessageSize" => info["pendingPersistentMessageSize"],
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

#      if (type == "topic")
#        event_info["queue"]["activeDurables"] = info["activeDurableCount"]
#        event_info["queue"]["durables"] = info["durableCount"]
#        event_info["queue"]["subscribers"] = info["subscriberCount"]
#      else
        event_info["queue"]["deliveredMessages"] = info["deliveredMessageCount"]
        event_info["queue"]["inTransitMessages"] = info["inTransitMessageCount"]
        event_info["queue"]["maxRedelivery"] = info["maxRedelivery"]
        event_info["queue"]["receivers"] = info["receiverCount"]
#      end
#
      event = LogStash::Event.new(event_info)

      decorate(event)
      queue << event
  end
end # class LogStash::Inputs::TibEMSAdmin
