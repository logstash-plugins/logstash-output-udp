# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "socket"

# Send events over UDP
#
# Keep in mind that UDP will lose messages.
class LogStash::Outputs::UDP < LogStash::Outputs::Base
  config_name "udp"
  
  default :codec, "json"

  # The address to send messages to
  config :host, :validate => :string, :required => true

  # The port to send messages on
  config :port, :validate => :number, :required => true

  public
  def register
    @socket = UDPSocket.new
    @codec.on_event do |event, payload|
      begin
        @socket.send(payload, 0, @host, @port)
      rescue Errno::EMSGSIZE => e
        logger.error("Failed to send event, message size of #{payload.size} too long", error_hash(e, payload))
      rescue => e
        logger.error("Failed to send event:", error_hash(e, payload))
      end
    end
  end

  MAX_DEBUG_PAYLOAD = 1000

  def error_hash(error, payload)
    error_hash = {:error => error.inspect,
                  :backtrace => error.backtrace.first(10)
                 }
    if logger.debug?
      error_hash.merge(:event_payload =>
                       payload.length > MAX_DEBUG_PAYLOAD ? "#{payload[0...MAX_DEBUG_PAYLOAD]}...<TRUNCATED>" : payload
                      )
    else
      error_hash
    end
  end

  def receive(event)
    return if event == LogStash::SHUTDOWN
    @codec.encode(event)
  end

end # class LogStash::Outputs::Stdout
