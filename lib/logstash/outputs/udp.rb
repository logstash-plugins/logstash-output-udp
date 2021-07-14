# encoding: utf-8
require "socket"
require "logstash/outputs/base"
require "logstash/namespace"

# Send events over UDP
#
# Keep in mind that UDP is a lossy protocol
class LogStash::Outputs::UDP < LogStash::Outputs::Base

  config_name "udp"
  
  default :codec, "json"

  # The address to send messages to
  config :host, :validate => :string, :required => true

  # The port to send messages on
  config :port, :validate => :string, :required => true

  # The number of times to retry a failed UPD socket write
  config :retry_count, :validate => :number, :default => 0

  # The amount of time to wait in milliseconds before attempting to retry a failed UPD socket write
  config :retry_backoff_ms, :validate => :number, :default => 100

  def register
    @socket = UDPSocket.new

    @codec.on_event do |event, payload|
      port = event.sprintf(@port)
      begin
        port = Integer(port)
      rescue => e # ArgumentError (invalid value for Integer(): "foo")
        logger.error("Failed to resolve port, dropping event", port: @port, event: event.to_hash, message: e.message)
      else
        socket_send(payload, port)
      end
    end
  end

  def receive(event)
    @codec.encode(event)
  end

  private

  def socket_send(payload, port)
    send_count = 0
    begin
      send_count += 1
      @socket.send(payload, 0, @host, port)
    rescue Errno::EMSGSIZE => e
      logger.error("Failed to send event, message size of #{payload.size} too long", error_hash(e, payload))
    rescue => e
      if @retry_count > 0 && send_count <= @retry_count
        logger.warn("Failed to send event, retrying:", error_hash(e, payload, trace: false))
        sleep(@retry_backoff_ms / 1000.0)
        retry
      else
        logger.error("Failed to send event:", error_hash(e, payload))
      end
    end
  end

  MAX_DEBUG_PAYLOAD = 1000

  def error_hash(error, payload, trace: true)
    error_hash = { :message => error.message, :exception => error.class }
    if logger.debug?
      error_hash[:event_payload] =
          payload.length > MAX_DEBUG_PAYLOAD ? "#{payload[0...MAX_DEBUG_PAYLOAD]}...<TRUNCATED>" : payload
    end
    error_hash[:backtrace] = error.backtrace if trace || logger.debug?
    error_hash
  end
end
