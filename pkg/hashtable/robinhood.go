package hashtable

import (
	"crypto/rand"
	"encoding/binary"
	"math"

	//"github.com/dchest/siphash"

	"github.com/dchest/siphash"
	"github.com/dgryski/go-t1ha"
	"github.com/inspirent/go-spooky"
	"github.com/minio/highwayhash"
)

const (
	minArrayLen = 50
	defaultMaxLF = 0.8    //Default load factor to initiate scaling up
	defaultMinLF = 0.25   //Default load factor to initiate scaling down
	hashKeyLen = 32
)

type Hasher interface {
	CreateHash(payload string) uint32
}

type SipHash struct {
	key0 uint64
	key1 uint64
}


func (h SipHash) CreateHash(payload string) uint32 {
	hash := siphash.Hash(h.key0, h.key1, []byte(payload))
	
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, hash)

	b[4] = b[0] ^ b[4]
	b[5] = b[1] ^ b[5]
	b[6] = b[2] ^ b[6]
	b[7] = b[3] ^ b[7]


	return binary.LittleEndian.Uint32(b)
}

func NewSipHash() (*SipHash, error) {
	first, err := createSeedUint64()
	if err != nil {
		return nil, err
	}

	second, err := createSeedUint64()
	if err != nil {
		return nil, err
	}

	return &SipHash{
		key0: first,
		key1: second,
	}, nil
}


type HwHash struct {
	key []byte
}


func (h HwHash) CreateHash(payload string) uint32 {
	hash := highwayhash.Sum64([]byte(payload), h.key)
	
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, hash)

	b[4] = b[0] ^ b[4]
	b[5] = b[1] ^ b[5]
	b[6] = b[2] ^ b[6]
	b[7] = b[3] ^ b[7]


	return binary.LittleEndian.Uint32(b)
}

func NewHwHash() (*HwHash, error) {
	k, err := createKey(hashKeyLen)
	if err != nil {
		return nil, err
	}

	return &HwHash{
		key: k,
	}, nil
}



type T1Hash struct {
	key uint64
}


func (h T1Hash) CreateHash(payload string) uint32 {
	hash := t1ha.Sum64([]byte(payload), h.key)
	
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, hash)

	b[4] = b[0] ^ b[4]
	b[5] = b[1] ^ b[5]
	b[6] = b[2] ^ b[6]
	b[7] = b[3] ^ b[7]


	return binary.LittleEndian.Uint32(b)
}

func NewT1Hash() (*T1Hash, error) {
	k, err := createSeedUint64()
	if err != nil {
		return nil, err
	}

	return &T1Hash{
		key: k,
	}, nil
}

type SpookyHash struct {
	key uint32
}


func (h SpookyHash) CreateHash(payload string) uint32 {
	return spooky.Hash32Seed([]byte(payload), h.key)
}

func NewSpookyHash() (*SpookyHash, error) {
	k, err := createSeedUint32()
	if err != nil {
		return nil, err
	}

	return &SpookyHash{
		key: k,
	}, nil
}


func createKey(length int) ([]byte, error) {
	buff := make([]byte, length)

	if _, err := rand.Read(buff); err != nil {
		return nil, err
	}

	return buff, nil
}


func createSeedUint64() (uint64, error) {
	buff := make([]byte, 8)

	if _, err := rand.Read(buff); err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint64(buff), nil
}

func createSeedUint32() (uint32, error) {
	buff := make([]byte, 4)

	if _, err := rand.Read(buff); err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint32(buff), nil
}


type Bucket struct {
	hashKey uint32
	pollPos uint8
	value   interface{}
}

type HashMap struct {
	items []Bucket
	Length uint64
	Capacity uint64
	Load uint64
	initSize uint64
	maxSize uint64
	maxLF float32
	minLF float32
	hash  Hasher
}

func NewHashMap(h Hasher) (*HashMap, error) {

	hm := &HashMap{
		initSize: minArrayLen,
		maxLF: defaultMaxLF,
		minLF: defaultMinLF,
		maxSize: math.MaxInt64,
		items: make([]Bucket, minArrayLen),
		hash: h,
		Length: minArrayLen,
		Load: 0,
	}

	hm.Capacity = hm.usableSpace(hm.Length)
	return hm, nil
}

func NewAdvancedHashMap(h Hasher, initialSize uint64, maxSize uint64, scaleMaxLF float32, scaleMinLF float32) (*HashMap, error) {
	//sh, err := createKey(hashKeyLen)

	hm := &HashMap{
		initSize: initialSize,
		maxLF: scaleMaxLF,
		minLF: scaleMinLF,
		maxSize: math.MaxInt64,
		items: make([]Bucket, initialSize),
		hash: h,
		Length: initialSize,
		Load: 0,
	}

	hm.Capacity = hm.usableSpace(hm.Length)
	return hm, nil
}



func (h *HashMap) Add(key string, value interface{}) {
	lf := float32(0)
	if h.Length != 0 {
		lf = float32(h.Load) / float32(h.Length)
	} 

	if lf >= h.maxLF {
		h.scaleUp()
	} else if h.Length > h.initSize  && lf <= h.minLF {
		h.scaleDown()
	}

	hash := h.hash.CreateHash(key)
	mustResize := h.add(h.items, hash, &h.Load, value, h.Capacity)
	if mustResize {
		h.scaleUp()
		h.add(h.items, hash, &h.Load, value, h.Capacity)
	}
}

func (h *HashMap) Get(key string) (interface{}, bool) {
	hash := h.hash.CreateHash(key)
	pos := uint32((uint64((hash << 1)) * h.Capacity) >> 32)
	if h.items[pos].hashKey == hash && h.items[pos].pollPos == 0 {
		return h.items[pos].value, true
	} 
			
	pos++
	for h.items[pos].pollPos != 0 && uint64(pos) < h.Length {
		if h.items[pos].hashKey == hash {
			return h.items[pos].value, true
		}
		pos++
	}

	return nil, false
}

func (h *HashMap) add(arr []Bucket, hash uint32, load *uint64, value interface{}, capacity uint64) bool {

	arrLen := uint64(len(arr))
	startPos := h.pollPosition(hash, capacity)
	pos := startPos
	if arr[pos].value == nil {
		//No collision
		arr[pos].hashKey = hash
		arr[pos].value = &value
		arr[pos].pollPos = 0
		//arr[pos].pp = startPos
		
		*load++
	} else {
		//Gotta go all robin hood & shit now
		done := false
		pp := uint8(0)

		for !done {
			if arr[pos].value == nil {
				//No collision
				arr[pos].hashKey = hash
				arr[pos].value = &value
				arr[pos].pollPos = pp
				//arr[pos].pp = startPos
				*load++
				done = true
			} else if arr[pos].pollPos < pp {
				//Current element is richer. Must expropriate.
				tmpHash  := arr[pos].hashKey
				tmpValue := arr[pos].value
				tmpPP    := arr[pos].pollPos
				//tmpStartPP := arr[pos].pp

				arr[pos].hashKey = hash
				arr[pos].pollPos = pp
				arr[pos].value = value
				//arr[pos].pp = startPos

				hash = tmpHash
				value = tmpValue
				pp = tmpPP
				//startPos = tmpStartPP

				pp++
				pos++
			} else {
				pp++
				pos++
			}

			//If we've moved beyond the array bounds but the load factor is still quite low then tack on a few buckets.
			if uint64(pos) >= arrLen {
				return true
			}

		}
	}

	return false
}

func (h HashMap) usableSpace(len uint64) uint64 {
	return len - (len >> (h.numBits(len) >> 1))
}

func (h *HashMap) scaleUp() {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(h.Length))
	var newSize uint64
	if h.Length >> 16 <= 0{
		// Quadratic upscale
		newSize = h.Length * uint64(2)
	} else {
		bits := h.numBits(h.Length)
		newSize = h.Length + ((h.Length * uint64(2)) / uint64((64 - int(bits) - 16) / 4))
	}

	h.scaleTo(newSize)
}

func (h HashMap) numBits(num uint64) uint8 {
	bits := uint8(0)
	i := num
	for i > 0 {	
		bits++
		i = i >> 1
	}

	return bits
}

func (h *HashMap) scaleDown() error {
	return nil
}

func (h *HashMap) scaleTo(size uint64) {
	//Resize
	newArr := make([]Bucket, size)
	newCap := h.usableSpace(uint64(len(newArr)))
	newLoad := uint64(0)
	for i := uint64(0); i < h.Length; i++ {
		if h.items[i].value != nil {
			mustResize := h.add(newArr, h.items[i].hashKey, &newLoad, h.items[i].value, newCap)

			for mustResize {
				size = size + h.initSize
				h.scaleTo(size)
				mustResize = h.add(newArr, h.items[i].hashKey, &newLoad, h.items[i].value, newCap)
			}
		}
	}

	h.items = newArr
	h.Load = uint64(newLoad)
	h.Length = uint64(size)
	h.Capacity = h.usableSpace(h.Length)
}

func (h *HashMap) findPosition(item Bucket, pp uint8, hash uint32, value interface{}) {
	if item.pollPos > pp {
		//Current element is richer. Must expropriate.
		tmp := h.items[pp]
		 
		h.items[pp].hashKey = hash
		h.items[pp].pollPos = pp
		h.items[pp].value = value
		
		pp++
		h.findPosition(tmp, pp, hash, value)
	}
}

//Allegedly faster than remainder(%) operator
func (h HashMap) pollPosition(hash uint32, arrLen uint64) uint32 {
	return uint32((uint64((hash << 1)) * arrLen) >> 32) 
}

//func (h HashMap) pollPositionScale(hash uint64, scale uint64) uint64 {
//	mask := (1 << (64 - h.clz)) - 1
//	return (hash & uint64(mask)) * scale >> 4
//}






