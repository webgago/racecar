module Racecar
  class RebalanceListener
    def initialize(config)
      @config = config
      @consumer_class = config.consumer_class
    end

    attr_reader :config, :consumer_class

    def on_partitions_assigned(_consumer, topic_partition_list)
      tpl = topic_partition_list.to_h
      config.instrumenter.instrument("partitions_assigned", tpl)
      consumer_class.respond_to?(:on_partitions_assigned) &&
        consumer_class.on_partitions_assigned(tpl)
    rescue
    end

    def on_partitions_revoked(_consumer, topic_partition_list)
      tpl = topic_partition_list.to_h
      config.instrumenter.instrument("partitions_revoked", tpl)
      consumer_class.respond_to?(:on_partitions_revoked) &&
        consumer_class.on_partitions_revoked(tpl)
    rescue
    end
  end
end
