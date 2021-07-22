from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor
from database import Sample, write_sample
import struct


class Echo(DatagramProtocol):
    def datagramReceived(self, data, addr):
        if len(data) != 56:
            print("Bad Packet: wrong size")
            return
        sample = Sample.from_buffer_copy(data)
        if not sample.is_valid():
            return
        write_sample(sample)
        self.transport.write(struct.pack("d", sample.timestamp), addr)
        print(sample)


reactor.listenUDP(9999, Echo())
reactor.run()
