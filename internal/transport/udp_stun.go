package transport

import (
	"crypto/rand"
	"encoding/binary"
	"hash/crc32"
	"net"
	"strconv"
)

const (
	stunBindingRequest       uint16 = 0x0001
	stunBindingSuccess       uint16 = 0x0101
	stunAttrXORMappedAddress uint16 = 0x0020
	stunAttrFingerprint      uint16 = 0x8028
	stunHeaderLen                   = 20
	stunMagicCookie          uint32 = 0x2112A442
	stunFingerprintXOR       uint32 = 0x5354554e
	stunFamilyIPv4           byte   = 0x01
	stunFamilyIPv6           byte   = 0x02
)

func buildSTUNBindingRequest() ([]byte, [12]byte, error) {
	var txID [12]byte
	if _, err := rand.Read(txID[:]); err != nil {
		return nil, txID, err
	}
	packet := newSTUNMessage(stunBindingRequest, txID, nil)
	return appendSTUNFingerprint(packet), txID, nil
}

func buildSTUNBindingSuccess(txID [12]byte, addr *net.UDPAddr) []byte {
	attrs := encodeSTUNXORMappedAddress(txID, addr)
	return appendSTUNFingerprint(newSTUNMessage(stunBindingSuccess, txID, attrs))
}

func newSTUNMessage(messageType uint16, txID [12]byte, attrs []byte) []byte {
	packet := make([]byte, stunHeaderLen+len(attrs))
	binary.BigEndian.PutUint16(packet[0:2], messageType)
	binary.BigEndian.PutUint16(packet[2:4], uint16(len(attrs)))
	binary.BigEndian.PutUint32(packet[4:8], stunMagicCookie)
	copy(packet[8:20], txID[:])
	copy(packet[20:], attrs)
	return packet
}

func appendSTUNFingerprint(packet []byte) []byte {
	withFingerprint := make([]byte, len(packet)+8)
	copy(withFingerprint, packet)
	binary.BigEndian.PutUint16(withFingerprint[2:4], uint16(len(withFingerprint)-stunHeaderLen))
	binary.BigEndian.PutUint16(withFingerprint[len(packet):len(packet)+2], stunAttrFingerprint)
	binary.BigEndian.PutUint16(withFingerprint[len(packet)+2:len(packet)+4], 4)
	crc := crc32.ChecksumIEEE(withFingerprint[:len(packet)]) ^ stunFingerprintXOR
	binary.BigEndian.PutUint32(withFingerprint[len(packet)+4:], crc)
	return withFingerprint
}

func validSTUNFingerprint(packet []byte) bool {
	if !isSTUNMessage(packet) {
		return false
	}
	messageLen := int(binary.BigEndian.Uint16(packet[2:4]))
	end := stunHeaderLen + messageLen
	for offset := stunHeaderLen; offset+4 <= end; {
		attrType := binary.BigEndian.Uint16(packet[offset : offset+2])
		attrLen := int(binary.BigEndian.Uint16(packet[offset+2 : offset+4]))
		valueStart := offset + 4
		valueEnd := valueStart + attrLen
		paddedEnd := valueStart + ((attrLen + 3) &^ 3)
		if valueEnd > end || paddedEnd > end {
			return false
		}
		if attrType == stunAttrFingerprint {
			if attrLen != 4 || paddedEnd != end {
				return false
			}
			want := crc32.ChecksumIEEE(packet[:offset]) ^ stunFingerprintXOR
			return binary.BigEndian.Uint32(packet[valueStart:valueEnd]) == want
		}
		offset = paddedEnd
	}
	return false
}
func isSTUNMessage(packet []byte) bool {
	if len(packet) < stunHeaderLen || packet[0]&0xc0 != 0 {
		return false
	}
	if binary.BigEndian.Uint32(packet[4:8]) != stunMagicCookie {
		return false
	}
	messageLen := int(binary.BigEndian.Uint16(packet[2:4]))
	return messageLen%4 == 0 && stunHeaderLen+messageLen <= len(packet)
}

func parseSTUNBindingSuccess(packet []byte) ([12]byte, string, bool) {
	var txID [12]byte
	if !isSTUNMessage(packet) || binary.BigEndian.Uint16(packet[0:2]) != stunBindingSuccess {
		return txID, "", false
	}
	copy(txID[:], packet[8:20])
	messageLen := int(binary.BigEndian.Uint16(packet[2:4]))
	end := stunHeaderLen + messageLen
	for offset := stunHeaderLen; offset+4 <= end; {
		attrType := binary.BigEndian.Uint16(packet[offset : offset+2])
		attrLen := int(binary.BigEndian.Uint16(packet[offset+2 : offset+4]))
		valueStart := offset + 4
		valueEnd := valueStart + attrLen
		if valueEnd > end {
			return txID, "", false
		}
		if attrType == stunAttrXORMappedAddress {
			observed, ok := decodeSTUNXORMappedAddress(txID, packet[valueStart:valueEnd])
			return txID, observed, ok
		}
		offset = valueStart + ((attrLen + 3) &^ 3)
	}
	return txID, "", false
}

func encodeSTUNXORMappedAddress(txID [12]byte, addr *net.UDPAddr) []byte {
	if ip4 := addr.IP.To4(); ip4 != nil {
		value := make([]byte, 8)
		value[1] = stunFamilyIPv4
		binary.BigEndian.PutUint16(value[2:4], uint16(addr.Port)^uint16(stunMagicCookie>>16))
		binary.BigEndian.PutUint32(value[4:8], binary.BigEndian.Uint32(ip4)^stunMagicCookie)
		return stunAttribute(stunAttrXORMappedAddress, value)
	}
	ip16 := addr.IP.To16()
	if ip16 == nil {
		return nil
	}
	value := make([]byte, 20)
	value[1] = stunFamilyIPv6
	binary.BigEndian.PutUint16(value[2:4], uint16(addr.Port)^uint16(stunMagicCookie>>16))
	mask := stunIPv6Mask(txID)
	for i := 0; i < net.IPv6len; i++ {
		value[4+i] = ip16[i] ^ mask[i]
	}
	return stunAttribute(stunAttrXORMappedAddress, value)
}

func decodeSTUNXORMappedAddress(txID [12]byte, value []byte) (string, bool) {
	if len(value) < 4 {
		return "", false
	}
	port := int(binary.BigEndian.Uint16(value[2:4]) ^ uint16(stunMagicCookie>>16))
	switch value[1] {
	case stunFamilyIPv4:
		if len(value) < 8 {
			return "", false
		}
		ip := make(net.IP, net.IPv4len)
		binary.BigEndian.PutUint32(ip, binary.BigEndian.Uint32(value[4:8])^stunMagicCookie)
		return net.JoinHostPort(ip.String(), strconv.Itoa(port)), true
	case stunFamilyIPv6:
		if len(value) < 20 {
			return "", false
		}
		ip := make(net.IP, net.IPv6len)
		mask := stunIPv6Mask(txID)
		for i := 0; i < net.IPv6len; i++ {
			ip[i] = value[4+i] ^ mask[i]
		}
		return net.JoinHostPort(ip.String(), strconv.Itoa(port)), true
	default:
		return "", false
	}
}

func stunAttribute(attrType uint16, value []byte) []byte {
	paddedLen := (len(value) + 3) &^ 3
	attr := make([]byte, 4+paddedLen)
	binary.BigEndian.PutUint16(attr[0:2], attrType)
	binary.BigEndian.PutUint16(attr[2:4], uint16(len(value)))
	copy(attr[4:], value)
	return attr
}

func stunIPv6Mask(txID [12]byte) [16]byte {
	var mask [16]byte
	binary.BigEndian.PutUint32(mask[0:4], stunMagicCookie)
	copy(mask[4:], txID[:])
	return mask
}
