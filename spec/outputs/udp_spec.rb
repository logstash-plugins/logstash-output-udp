# encoding: utf-8
require_relative "../spec_helper"

describe LogStash::Outputs::UDP do

  subject { described_class.new(config) }
  let(:host) { "localhost" }
  let(:port) { rand(1024..65535) }
  let(:config) {{ "host" => host, "port" => port}}

  let(:logger) { subject.logger }
  let(:socket) { subject.instance_variable_get("@socket") }

  it "should register without errors" do
    plugin = LogStash::Plugin.lookup("output", "udp").new(config)
    expect { plugin.register }.to_not raise_error
  end

  describe "#send" do
    let(:properties) { { "message" => "This is a message!"} }
    let(:event)      { LogStash::Event.new(properties) }

    before(:each) do
      subject.register
    end

    it "should receive the generated event" do
      expect(socket).to receive(:send).with(kind_of(String), 0, host, port)
      expect(logger).not_to receive(:error)
      subject.receive(event)
    end
  end

  context "IP-stack configuration" do
    matcher :socket_address_family do |expected_flag_name|
      expected_flag_value = Socket::Constants.const_get(expected_flag_name)
      match do |actual|
        actual == expected_flag_value
      end
      failure_message do |actual_flag_value|
        actual_flag_name = Socket::Constants.constants
                                            .select { |c| c.start_with?("AF_") }
                                            .find {|c| Socket::Constants.const_get(c) == actual_flag_value } || "UNKNOWN"
        "expected that #{actual_flag_name}(#{actual_flag_value}) would be equivalent to #{expected_flag_name}(#{expected_flag_value})"
      end
    end

    {
      "::1" => :AF_INET6,
      "2001:db8:0000:0000:0000:0000:0000:0001" => :AF_INET6,
      "[2001:db8:0000:0000:0000:0000:0000:0001]" => :AF_INET6,
      "2001:db8::0001" => :AF_INET6,
      "[2001:db8::0001]" => :AF_INET6,
      "127.0.0.1" => :AF_INET,
      "198.51.100.1" => :AF_INET,
      "localhost" => (IPAddr.new(IPSocket.getaddress("localhost")).ipv4? ? :AF_INET : :AF_INET6),
      "example.com" => (IPAddr.new(IPSocket.getaddress("example.com")).ipv4? ? :AF_INET : :AF_INET6),
    }.each do |specified_host, expected_address_family|
      context "host => '#{specified_host}'" do
        let(:host) { specified_host }

        it "creates a UDPSocket with the `#{expected_address_family}` family" do
          expect(UDPSocket).to receive(:new).and_wrap_original do |original, socket_family|
            expect(socket_family).to socket_address_family(expected_address_family)
            original.call(socket_family)
          end

          subject.register
        end
      end
    end
  end

  describe "port" do

    let(:config) { super().merge('port' => '%{[target_port]}') }

    before do
      subject.register
    end

    context 'valid' do

      let(:event) { LogStash::Event.new('message' => 'Hey!', 'target_port' => '12345') }

      before do
        expect(socket).to receive(:send).with(kind_of(String), 0, host, 12345)
        expect(logger).not_to receive(:error)
      end

      it "should receive the event" do
        subject.receive(event)
      end

    end

    context 'invalid' do

      let(:event) { LogStash::Event.new('message' => 'Hey!', 'target_port' => 'bogus') }

      before do
        expect(socket).to_not receive(:send)
        expect(logger).to receive(:error).with /Failed to resolve port, dropping event/,
                                               hash_including(:message => "invalid value for Integer(): \"bogus\"")
      end

      it "should receive the event" do
        subject.receive(event)
      end

    end
  end

  describe "retries" do
    let(:event) { LogStash::Event.new("message" => "test") }
    let(:config) {{ "host" => host, "port" => port}}

    before(:each) do
      subject.register
    end

    context "not using :retry_count" do
      it "should not retry upon send exception by default" do
        allow(socket).to receive(:send).once.and_raise(IOError)
        expect(logger).to receive(:error).once
        subject.receive(event)
      end
    end

    context "using :retry_count" do
      let(:backoff) { 10 }
      let(:retry_count) { 5 }
      let(:config) {{ "host" => host, "port" => port, "retry_count" => retry_count, "retry_backoff_ms" => backoff}}

      it "should retry upon send exception" do
        allow(socket).to receive(:send).exactly(retry_count + 1).times.and_raise(IOError)
        expect(logger).to receive(:warn).exactly(retry_count).times
        expect(logger).to receive(:error).once
        expect(subject).to receive(:sleep).with(backoff / 1000.0).exactly(retry_count).times
        subject.receive(event)
      end
    end
  end

  describe "large message" do
    let(:properties) { { "message" => "0" * 65_536 } }
    let(:event)      { LogStash::Event.new(properties) }

    before(:each) do
      subject.register
    end

    it "should handle the error and log when an error is received" do
      expect(logger).to receive(:error)
      subject.receive(event)
    end

    it "should log a truncated payload with debug logging when an error is received and the message is too long" do
      expect(logger).to receive(:debug?).and_return(true)
      expect(logger).to receive(:error) do |_, hash|
        expect(hash).to include(:event_payload)
        expect(hash[:event_payload]).to include("TRUNCATED")
      end
      subject.receive(event)
    end

    it "should not log a payload with debug logging when an error is received" do
      expect(logger).to receive(:debug?).and_return(false)
      expect(logger).to receive(:error) do |_, hash|
        expect(hash).not_to include(:event_payload)
      end
      subject.receive(event)
    end

  end
end
