# encoding: utf-8
require "logstash/outputs/sumologic/common"
require "logstash/outputs/sumologic/statistics"
require "logstash/outputs/sumologic/message_queue"

module LogStash; module Outputs; class SumoLogic;
  class Piler

    include LogStash::Outputs::SumoLogic::Common

    attr_reader :is_pile

    def initialize(queue, stats, config)
      
      @interval = config["interval"] ||= 0
      @pile_max = config["pile_max"] ||= 0
      @queue = queue
      @stats = stats
      @stopping = Concurrent::AtomicBoolean.new(false)
      @is_pile = (@interval > 0 && @pile_max > 0)

      if (@is_pile)
        @piles = Hash.new
        @pile_size = 0
        @semaphore = Mutex.new
      end

    end # def initialize

    def start()
      @stopping.make_false()
      if (@is_pile)
        @piler_t = Thread.new { 
          while @stopping.false?
            Stud.stoppable_sleep(@interval) { @stopping.true? }
            log_dbg("timeout, enqueue pile now")
            enq_and_clear()
          end # while
        }
      end # if
    end # def start

    def stop()
      @stopping.make_true()
      if (@is_pile)
        log_info "shutting down piler..."
        @piler_t.join
        log_info "piler is fully shutted down"
      end
    end # def stop

    def input(entry, key)
      if (@stopping.true?)
        log_warn "piler is shutting down, message ignored", "message" => entry
      elsif (@is_pile)
        @semaphore.synchronize {
          if @pile_size + entry.bytesize > @pile_max
            @piles.each do |sc, pile|
              @queue.enq(QueueItem(@pile.join($/), @sc))
              @pile.clear
            end
            @pile_size = 0
            @stats.record_clear_pile()
          end
          if !@piles.key?(key)
            @piles[key] = Array.new
          end
          @piles[key] << entry
          @pile_size += entry.bytesize
          @stats.record_input(entry)
        }
      else
        @queue.enq(QueueItem(entry,key))
      end # if
    end # def input

    private
    def enq_and_clear()
      if (@piles.size > 0)
        @semaphore.synchronize {
            @piles.each do |sc, pile|
              @queue.enq(queueItem(@pile.join($/), @sc))
              @pile.clear
            end
            @pile_size = 0
            @stats.record_clear_pile()
        }
      end
    end # def enq_and_clear

  end

  class QueueItem 
    def initialize(message, key)
      @key = key
      @message = message
    end

    def getKey()
      @key
    end

    def getMessage()
      @message
    end
  end
end; end; end