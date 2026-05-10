package encoding

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/hamba/avro/v2"
	"github.com/riferrei/srclient"
	"warehouse/internal/model"
)

type Resolver struct {
	sr *srclient.SchemaRegistryClient
}

func NewResolver(srURL string) *Resolver {
	return &Resolver{sr: srclient.CreateSchemaRegistryClient(srURL)}
}

func (r *Resolver) DecodeWarehousing(raw []byte, out *model.WarehouseEvent) error {
	if len(raw) < 5 || raw[0] != 0 {
		return fmt.Errorf("invalid wire format")
	}
	id := int(binary.BigEndian.Uint32(raw[1:5]))
	sch, err := r.sr.GetSchema(id)
	if err != nil {
		return err
	}
	parsed, err := avro.Parse(sch.Schema())
	if err != nil {
		return err
	}
	return avro.Unmarshal(parsed, raw[5:], out)
}

func (r *Resolver) EncodeWarehousing(schemaStr string, id int, ev *model.WarehouseEvent) ([]byte, error) {
	parsed, err := avro.Parse(schemaStr)
	if err != nil {
		return nil, err
	}
	payload, err := avro.Marshal(parsed, ev)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	buf.WriteByte(0)
	_ = binary.Write(&buf, binary.BigEndian, uint32(id))
	_, _ = buf.Write(payload)
	return buf.Bytes(), nil
}

func ConfluentSchemaID(raw []byte) (int, error) {
	if len(raw) < 5 || raw[0] != 0 {
		return 0, fmt.Errorf("confluent wire")
	}
	return int(binary.BigEndian.Uint32(raw[1:5])), nil
}

func (r *Resolver) RegistrySchemaVersion(raw []byte) (int, error) {
	id, err := ConfluentSchemaID(raw)
	if err != nil {
		return 0, err
	}
	sch, err := r.sr.GetSchema(id)
	if err != nil {
		return 0, err
	}
	return sch.Version(), nil
}
