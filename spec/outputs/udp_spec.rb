# encoding: utf-8
require_relative "../spec_helper"

describe LogStash::Outputs::UDP do

  subject { described_class.new(config) }
  let(:host) { "localhost" }
  let(:port) { rand(1024..65535) }
  let(:config) {{ "host" => host, "port" => port}}

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
      expect(subject.instance_variable_get("@socket")).to receive(:send).with(kind_of(String), 0, host, port)
      expect(subject.instance_variable_get("@logger")).not_to receive(:error)
      subject.receive(event)
    end
  end

  describe "large message" do
    let(:properties) { { "message" => "0" * 65_536 } }
    let(:event)      { LogStash::Event.new(properties) }

    before(:each) do
      subject.register
    end

    it "should handle the error and log when an error is received" do
      expect(subject.instance_variable_get("@logger")).to receive(:error)
      subject.receive(event)
    end
  end
end
