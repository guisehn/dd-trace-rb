require 'forwardable'
require 'ddtrace/ext/distributed'

module Datadog
  # Serializable construct representing a trace
  class Trace
    extend Forwardable

    SPANS_METHODS = [
      :any?,
      :empty?,
      :length
    ].freeze

    attr_reader \
      :sampled,
      :spans,
      :tags

    def initialize(
      spans,
      origin: nil,
      sampled: false,
      sampling_priority: nil
    )
      @sampled = (sampled == true)
      @spans = spans || []
      @tags = {}

      self.sampling_priority = sampling_priority
      self.origin = origin
    end

    def_delegators :spans, *SPANS_METHODS

    def sampling_priority
      tags[:sampling_priority]
    end

    def sampling_priority=(value)
      tags[:sampling_priority] = value
    end

    def origin
      tags[:origin]
    end

    def origin=(value)
      tags[:origin] = value
    end

    def root_span
      spans.first
    end

    def id
      root_span && root_span.trace_id
    end

    def partial?
      root_span && root_span.parent_id == 0
    end

    # Annotate trace by embedding trace tags onto the spans
    def annotate!
      return unless root_span

      attach_sampling_priority! if sampled && sampling_priority
      attach_origin! if origin
    end

    def to_msgpack(*args)
      spans.to_msgpack(*args)
    end

    private

    def attach_sampling_priority!
      root_span.set_metric(
        Ext::DistributedTracing::SAMPLING_PRIORITY_KEY,
        sampling_priority
      )
    end

    def attach_origin!
      root_span.set_tag(
        Ext::DistributedTracing::ORIGIN_KEY,
        origin
      )
    end
  end
end
