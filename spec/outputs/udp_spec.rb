# encoding: utf-8
require_relative "../spec_helper"

describe LogStash::Outputs::UDP do

  let(:host) { "localhost" }
  let(:port) { rand(1024..65535) }

  it "should register without errors" do
    plugin = LogStash::Plugin.lookup("output", "udp").new({ "host" => host, "port" => port})
    expect { plugin.register }.to_not raise_error
  end

  describe "#send" do

    subject { LogStash::Outputs::UDP.new({"host" => host, "port" => port}) }

    let(:properties) { { "message" => "This is a message!"} }
    let(:event)      { LogStash::Event.new(properties) }

    before(:each) do
      subject.register
    end

    it "should receive the generated event" do
      expect(subject.instance_variable_get("@socket")).to receive(:send).with(kind_of(String), 0, host, port)
      subject.receive(event)
    end
  end
end
