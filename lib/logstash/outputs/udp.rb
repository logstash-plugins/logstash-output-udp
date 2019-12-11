# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "socket"

# Send events over UDP
#
# Keep in mind that UDP is a lossy protocol
class LogStash::Outputs::UDP < LogStash::Outputs::Base
  config_name "udp"
  
  default :codec, "json"

  # The address to send messages to
  config :host, :validate => :string, :required => true

  # The port to send messages on
  config :port, :validate => :number, :required => true

  # The number of times to retry a failed UPD socket write
  config :retry_count, :validate => :number, :default => 0

  # The amount of time to wait in milliseconds before attempting to retry a failed UPD socket write
  config :retry_backoff_ms, :validate => :number, :default => 100

  def register
    @socket = UDPSocket.new

    @codec.on_event do |event, payload|
      socket_send(payload)
    end
  end

  def receive(event)
    @codec.encode(event)
  end

  private

  def socket_send(payload)
    send_count = 0
    begin
      send_count += 1
      @socket.send(payload, 0, @host, @port)
    rescue Errno::EMSGSIZE => e
      logger.error("Failed to send event, message size of #{payload.size} too long", error_hash(e, payload))
    rescue => e
      if @retry_count > 0 && send_count <= @retry_count
        logger.warn("Failed to send event, retrying:", error_hash(e, payload))
        sleep(@retry_backoff_ms / 1000.0)
        retry
      else
        logger.error("Failed to send event:", error_hash(e, payload))
      end
    end
  end

  MAX_DEBUG_PAYLOAD = 1000

  def error_hash(error, payload)
    error_hash = {
      :error => error.inspect,
      :backtrace => error.backtrace.first(10)
    }
    if logger.debug?
      error_hash.merge(
        :event_payload =>
        payload.length > MAX_DEBUG_PAYLOAD ? "#{payload[0...MAX_DEBUG_PAYLOAD]}...<TRUNCATED>" : payload
      )
    else
      error_hash
    end
  end
end
