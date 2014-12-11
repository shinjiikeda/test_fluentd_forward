
require 'serverengine'
require 'msgpack'

# Server module controls the parent process
module MyServer
  def before_run
    @sock = TCPServer.new(config[:bind], config[:port])
    
    linger_timeout = 0
    opt = [1, linger_timeout].pack('I!I!') # { int l_onoff; int l_linger; }
    @sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, opt)
  end
  
  attr_reader :sock
end

# Worker module controls child processes
module MyWorker

  def run
    logger.info("start worker #{self.worker_id}")
    buf = ""
    recv_buffer_size = config[:recv_buffer_max]
    @msg_unpacker = MessagePack::Unpacker.new
    while @stop != true 
      #r, w = IO.select([server.sock], nil, nil, 5)
      #if r.nil? || r[0] == nil
      #  #logger.debug("timeout")
      #  next
      #end

      begin
        c = server.sock.accept
        #c = r[0].accept
        begin
          logger.info("accept")
          if c.read(recv_buffer_size + 1, buf) != nil
            if buf.bytesize <= recv_buffer_size
              @msg_unpacker.feed_each(buf) do | obj |
                option = process_message(obj)
                if option && option['chunk']
                  res = { 'ack' => option['chunk'] }
                  c.write(res.to_msgpack)
                end
              end
            else
              logger.error("buffer overflow")
            end
          end
        ensure
          c.close
        end
      rescue => e
        logger.error(e.inspect)
      end
    end
  end

  def stop
    @stop = true
  end

  def process_message(msg)
    if msg.nil?
      return nil
    end
    
    begin
      tag = msg[0].to_s
      entries = msg[1]
      
      if entries.class == String
        @msg_unpacker.feed_each(entries) do | obj |
          logger.debug("obj: #{obj.to_s}")
        end
        return
      elsif entries.class == Array
        logger.debug("message: #{msg.to_s}")
        return nil
      
        # Forward
        events = []
        entries.each do |e|
          record = e[1]
          next if record.nil?
          time = e[0].to_i
          time = (now ||= Time.now) if time == 0
          events << [tag, time, record]
        end
        option = msg[2]
        
        logger.debug("events: " + events.to_s)
        logger.debug("option: " + option.to_s)
        
        return option
      else
        logger.error("unkown message")
      end
    rescue => e
      logger.error(e.inspect)
      return nil
    end
  end 
end


se = ServerEngine.create(MyServer, MyWorker, {
  daemonize: true,
  log: 'myserver.log',
  pid_path: 'myserver.pid',
  worker_type: 'process',
  workers: 4,
  worker_graceful_kill_timeout: 30,
  bind: '0.0.0.0',
  port: 9071,
  recv_buffer_max: 10_000_000,
})

se.run

